// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
)

var _ resolver = (*uriResolver)(nil)

const (
	defaultPollInterval = 5 * time.Second
	defaultPollTimeout  = time.Second
)

var (
	errNoURIFile = errors.New("no uri specified to resolve the backends")

	uriresolverMutator = tag.Upsert(tag.MustNewKey("resolver"), "uri")

	uriresolverSuccessTrueMutators  = []tag.Mutator{resolverMutator, successTrueMutator}
	uriresolverSuccessFalseMutators = []tag.Mutator{resolverMutator, successFalseMutator}
)

type uriResolver struct {
	logger      *zap.Logger
	uri         string
	resolver    urinetResolver
	resInterval time.Duration
	resTimeout  time.Duration

	endpoints         []string
	onChangeCallbacks []func([]string)

	stopCh             chan (struct{})
	updateLock         sync.Mutex
	shutdownWg         sync.WaitGroup
	changeCallbackLock sync.RWMutex
}

type urinetResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

func newURIResolver(logger *zap.Logger, bucket string, interval time.Duration, timeout time.Duration) (*uriResolver, error) {
	if len(bucket) == 0 {
		return nil, errNoURIFile
	}
	if interval == 0 {
		interval = defaultPollInterval
	}
	if timeout == 0 {
		timeout = defaultPollTimeout
	}

	return &uriResolver{
		logger:      logger,
		uri:         bucket,
		resolver:    &net.Resolver{},
		resInterval: interval,
		resTimeout:  timeout,
		stopCh:      make(chan struct{}),
	}, nil
}

func (r *uriResolver) start(ctx context.Context) error {
	if _, err := r.resolve(ctx); err != nil {
		r.logger.Warn("failed to resolve", zap.Error(err))
	}

	go r.periodicallyResolve()

	r.logger.Debug("URI resolver started", zap.String("uri", r.uri))
	return nil
}

func (r *uriResolver) shutdown(_ context.Context) error {
	r.changeCallbackLock.Lock()
	r.onChangeCallbacks = nil
	r.changeCallbackLock.Unlock()

	close(r.stopCh)
	r.shutdownWg.Wait()
	return nil
}

func (r *uriResolver) periodicallyResolve() {
	ticker := time.NewTicker(r.resInterval)

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), r.resTimeout)
			if _, err := r.resolve(ctx); err != nil {
				r.logger.Warn("failed to resolve", zap.Error(err))
			} else {
				r.logger.Debug("resolved successfully")
			}
			cancel()
		case <-r.stopCh:
			return
		}
	}
}

func (r *uriResolver) resolve(ctx context.Context) ([]string, error) {

	r.shutdownWg.Add(1)
	defer r.shutdownWg.Done()
	backends := make([]string, 0)
	// if starts with gs://, then read from google storage, else assume it is http/https
	if strings.HasPrefix(r.uri, "gs:") {
		// use env variable for authentication
		//export GOOGLE_APPLICATION_CREDENTIALS=<path_to_service_account_json_file>
		gsuri, err := url.Parse(r.uri)
		// Create a storage client.
		client, err := storage.NewClient(context.Background())
		if err != nil {
			return nil, err
		}
		bucket := client.Bucket(gsuri.Host)
		object := bucket.Object(strings.TrimLeft(gsuri.Path, "/"))
		r.logger.Info("Reading from bucket", zap.String("bucket", gsuri.Host), zap.String("object", gsuri.Path))
		// Read the object.
		reader, err := object.NewReader(context.Background())
		if err != nil {
			return nil, err
		}
		// Read the object's contents.
		contents, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		lines := strings.Split(string(contents), "\n")
		reader.Close()
		// Trim and remove empty lines

		for _, line := range lines {
			trimmedLine := strings.TrimSpace(line)
			if trimmedLine != "" {
				backends = append(backends, trimmedLine)
			}
		}

	} else {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client := &http.Client{Transport: tr}
		resp, err := client.Get(r.uri)
		if err != nil {
			r.logger.Error("failed to get from uri"+r.uri, zap.Error(err))
			return nil, err
		}
		contents, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		lines := strings.Split(string(contents), "\n")
		// Trim and remove empty lines
		for _, line := range lines {
			trimmedLine := strings.TrimSpace(line)
			if trimmedLine != "" {
				backends = append(backends, trimmedLine)
			}
		}
	}

	// keep it always in the same order
	sort.Strings(backends)

	if equalStringSlice(r.endpoints, backends) {
		r.logger.Info("Same list of backends, not updating")
		return r.endpoints, nil
	}

	// the list has changed!
	r.logger.Info("List of backends has changed, updating")
	r.updateLock.Lock()
	r.endpoints = backends
	r.updateLock.Unlock()
	_ = stats.RecordWithTags(ctx, resolverSuccessTrueMutators, mNumBackends.M(int64(len(backends))))

	// propagate the change
	r.changeCallbackLock.RLock()
	for _, callback := range r.onChangeCallbacks {
		callback(r.endpoints)
	}
	r.changeCallbackLock.RUnlock()

	return r.endpoints, nil
}

func (r *uriResolver) onChange(f func([]string)) {
	r.changeCallbackLock.Lock()
	defer r.changeCallbackLock.Unlock()
	r.onChangeCallbacks = append(r.onChangeCallbacks, f)
}
