// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestInitialURIResolution(t *testing.T) {
	// prepare
	res, err := newURIResolver(zap.NewNop(), "service-1", 5*time.Second, 1*time.Second)
	require.NoError(t, err)

	res.resolver = &mockURIResolver{
		onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{
				{IP: net.IPv4(127, 0, 0, 1)},
				{IP: net.IPv4(127, 0, 0, 2)},
				{IP: net.IPv6loopback},
			}, nil
		},
	}

	// test
	var resolved []string
	res.onChange(func(endpoints []string) {
		resolved = endpoints
	})
	require.NoError(t, res.start(context.Background()))
	defer func() {
		require.NoError(t, res.shutdown(context.Background()))
	}()

	// verify
	assert.Len(t, resolved, 3)
	for i, value := range []string{"127.0.0.1", "127.0.0.2", "[::1]"} {
		assert.Equal(t, value, resolved[i])
	}
}

func TestInitialURIResolutionWithPort(t *testing.T) {
	// prepare
	res, err := newURIResolver(zap.NewNop(), "service-1", 5*time.Second, 1*time.Second)
	require.NoError(t, err)

	res.resolver = &mockURIResolver{
		onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{
				{IP: net.IPv4(127, 0, 0, 1)},
				{IP: net.IPv4(127, 0, 0, 2)},
				{IP: net.IPv6loopback},
			}, nil
		},
	}

	// test
	var resolved []string
	res.onChange(func(endpoints []string) {
		resolved = endpoints
	})
	require.NoError(t, res.start(context.Background()))
	defer func() {
		require.NoError(t, res.shutdown(context.Background()))
	}()

	// verify
	assert.Len(t, resolved, 3)
	for i, value := range []string{"127.0.0.1:55690", "127.0.0.2:55690", "[::1]:55690"} {
		assert.Equal(t, value, resolved[i])
	}
}

func TestURIErrNoHostname(t *testing.T) {
	// test
	res, err := newURIResolver(zap.NewNop(), "", 5*time.Second, 1*time.Second)

	// verify
	assert.Nil(t, res)
	assert.Equal(t, errNoHostname, err)
}

func TestURICantResolve(t *testing.T) {
	// prepare
	res, err := newURIResolver(zap.NewNop(), "service-1", 5*time.Second, 1*time.Second)
	require.NoError(t, err)

	expectedErr := errors.New("some expected error")
	res.resolver = &mockURIResolver{
		onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			return nil, expectedErr
		},
	}

	// test
	require.NoError(t, res.start(context.Background()))

	// verify
	assert.NoError(t, err)
}

func TestURIOnChange(t *testing.T) {
	// prepare
	res, err := newURIResolver(zap.NewNop(), "service-1", 5*time.Second, 1*time.Second)
	require.NoError(t, err)

	resolve := []net.IPAddr{
		{IP: net.IPv4(127, 0, 0, 1)},
	}
	res.resolver = &mockURIResolver{
		onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			return resolve, nil
		},
	}

	// test
	counter := &atomic.Int64{}
	res.onChange(func(endpoints []string) {
		counter.Add(1)
	})
	require.NoError(t, res.start(context.Background()))
	defer func() {
		require.NoError(t, res.shutdown(context.Background()))
	}()
	require.Equal(t, int64(1), counter.Load())

	// now, we run it with the same IPs being resolved, which shouldn't trigger a onChange call
	_, err = res.resolve(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), counter.Load())

	// change what the resolver will resolve and trigger a resolution
	resolve = []net.IPAddr{
		{IP: net.IPv4(127, 0, 0, 2)},
		{IP: net.IPv4(127, 0, 0, 3)},
	}
	_, err = res.resolve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(2), counter.Load())
}

func TestURIEqualStringSlice(t *testing.T) {
	for _, tt := range []struct {
		source    []string
		candidate []string
		expected  bool
	}{
		{
			[]string{"endpoint-1"},
			[]string{"endpoint-1"},
			true,
		},
		{
			[]string{"endpoint-1", "endpoint-2"},
			[]string{"endpoint-1"},
			false,
		},
		{
			[]string{"endpoint-1"},
			[]string{"endpoint-2"},
			false,
		},
	} {
		res := equalStringSlice(tt.source, tt.candidate)
		assert.Equal(t, tt.expected, res)
	}
}

func TestURIPeriodicallyResolve(t *testing.T) {
	// prepare
	res, err := newURIResolver(zap.NewNop(), "service-1", 10*time.Millisecond, 1*time.Second)
	require.NoError(t, err)

	counter := &atomic.Int64{}
	resolve := [][]net.IPAddr{
		{
			{IP: net.IPv4(127, 0, 0, 1)},
		}, {
			{IP: net.IPv4(127, 0, 0, 1)},
			{IP: net.IPv4(127, 0, 0, 2)},
		}, {
			{IP: net.IPv4(127, 0, 0, 1)},
			{IP: net.IPv4(127, 0, 0, 2)},
			{IP: net.IPv4(127, 0, 0, 3)},
		},
	}
	res.resolver = &mockURIResolver{
		onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			defer func() {
				counter.Add(1)
			}()
			// for second call, return the second result
			if counter.Load() == 2 {
				return resolve[1], nil
			}
			// for subsequent calls, return the last result, because we need more two periodic results
			// to confirm that it works as expected.
			if counter.Load() >= 3 {
				return resolve[2], nil
			}

			// for the first call, return the first result
			return resolve[0], nil
		},
	}

	wg := sync.WaitGroup{}
	res.onChange(func(backends []string) {
		wg.Done()
	})

	// test
	wg.Add(3)
	require.NoError(t, res.start(context.Background()))
	defer func() {
		require.NoError(t, res.shutdown(context.Background()))
	}()

	// wait for three resolutions: from the start, and two periodic resolutions
	wg.Wait()

	// verify
	assert.GreaterOrEqual(t, counter.Load(), int64(3))
	assert.Len(t, res.endpoints, 3)
}

func TestPeriodicallyURIResolveFailure(t *testing.T) {
	// prepare
	res, err := newURIResolver(zap.NewNop(), "service-1", 10*time.Millisecond, 1*time.Second)
	require.NoError(t, err)

	expectedErr := errors.New("some expected error")
	wg := sync.WaitGroup{}
	counter := &atomic.Int64{}
	resolve := []net.IPAddr{{IP: net.IPv4(127, 0, 0, 1)}}
	res.resolver = &mockURIResolver{
		onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			counter.Add(1)

			// count down at most two times
			if counter.Load() <= 2 {
				wg.Done()
			}

			// for subsequent calls, return the error
			if counter.Load() >= 2 {
				return nil, expectedErr
			}

			// for the first call, return the first result
			return resolve, nil
		},
	}

	// test
	wg.Add(2)
	require.NoError(t, res.start(context.Background()))
	defer func() {
		require.NoError(t, res.shutdown(context.Background()))
	}()

	// wait for two resolutions: from the start, and one periodic
	wg.Wait()

	// verify
	assert.GreaterOrEqual(t, counter.Load(), int64(2))
	assert.Len(t, res.endpoints, 1) // no change to the list of endpoints
}

func TestShutdownURIClearsCallbacks(t *testing.T) {
	// prepare
	res, err := newURIResolver(zap.NewNop(), "service-1", 5*time.Second, 1*time.Second)
	require.NoError(t, err)

	res.resolver = &mockURIResolver{}
	res.onChange(func(s []string) {})
	require.NoError(t, res.start(context.Background()))

	// sanity check
	require.Len(t, res.onChangeCallbacks, 1)

	// test
	err = res.shutdown(context.Background())

	// verify
	assert.NoError(t, err)
	assert.Len(t, res.onChangeCallbacks, 0)

	// check that we can add a new onChange before a new start
	res.onChange(func(s []string) {})
	assert.Len(t, res.onChangeCallbacks, 1)
}

var _ netResolver = (*mockURIResolver)(nil)

type mockURIResolver struct {
	net.Resolver
	onLookupIPAddr func(context.Context, string) ([]net.IPAddr, error)
}

func (m *mockURIResolver) LookupIPAddr(ctx context.Context, hostname string) ([]net.IPAddr, error) {
	if m.onLookupIPAddr != nil {
		return m.onLookupIPAddr(ctx, hostname)
	}
	return nil, nil
}
