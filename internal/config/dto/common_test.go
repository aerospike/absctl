// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dto

import (
	"testing"

	"github.com/aerospike/tools-common-go/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCluster_ToAerospikeConfig(t *testing.T) {
	t.Run("nil cluster returns error", func(t *testing.T) {
		var c *Cluster
		cfg, err := c.ToAerospikeConfig()

		assert.Nil(t, cfg)
		assert.EqualError(t, err, "cluster cannot be nil")
	})

	t.Run("minimal valid config", func(t *testing.T) {
		c := &Cluster{}

		cfg, err := c.ToAerospikeConfig()

		require.NoError(t, err)
		assert.NotNil(t, cfg)
	})

	t.Run("error in auth propagates", func(t *testing.T) {
		c := &Cluster{
			Auth: new("INVALID_AUTH_MODE"),
		}

		cfg, err := c.ToAerospikeConfig()

		assert.Nil(t, cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to set auth mode")
	})

	t.Run("error in TLS propagates", func(t *testing.T) {
		c := &Cluster{
			TLS: &ClusterTLS{
				Enable:    new(true),
				Protocols: new("InvalidProtocol"),
			},
		}

		cfg, err := c.ToAerospikeConfig()

		assert.Nil(t, cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to set tls protocols")
	})

	t.Run("service alternate is applied", func(t *testing.T) {
		c := &Cluster{
			ServiceAlternate: new(true),
		}

		cfg, err := c.ToAerospikeConfig()

		require.NoError(t, err)
		assert.NotNil(t, cfg)
	})
}

func TestCluster_applySeeds(t *testing.T) {
	t.Run("nil seeds - no error", func(t *testing.T) {
		c := &Cluster{Seeds: nil}
		var f flags.AerospikeFlags

		err := c.applySeeds(&f)

		assert.NoError(t, err)
	})

	t.Run("empty seeds slice - no error", func(t *testing.T) {
		c := &Cluster{Seeds: []ClusterSeed{}}
		var f flags.AerospikeFlags

		err := c.applySeeds(&f)

		assert.NoError(t, err)
	})

	t.Run("single seed with host only", func(t *testing.T) {
		c := &Cluster{
			Seeds: []ClusterSeed{
				{Host: new("192.168.1.1")},
			},
		}
		var f flags.AerospikeFlags

		err := c.applySeeds(&f)

		require.NoError(t, err)

		assert.NotEmpty(t, f.Seeds)
	})

	t.Run("single seed with host and port", func(t *testing.T) {
		c := &Cluster{
			Seeds: []ClusterSeed{
				{
					Host: new("192.168.1.1"),
					Port: new(3000),
				},
			},
		}
		var f flags.AerospikeFlags

		err := c.applySeeds(&f)

		require.NoError(t, err)
		assert.NotEmpty(t, f.Seeds)
	})

	t.Run("single seed with host, port and tls name", func(t *testing.T) {
		c := &Cluster{
			Seeds: []ClusterSeed{
				{
					Host:    new("192.168.1.1"),
					Port:    new(3000),
					TLSName: new("tls-name"),
				},
			},
		}
		var f flags.AerospikeFlags

		err := c.applySeeds(&f)

		require.NoError(t, err)
		assert.NotEmpty(t, f.Seeds)
	})

	t.Run("multiple seeds", func(t *testing.T) {
		c := &Cluster{
			Seeds: []ClusterSeed{
				{
					Host: new("192.168.1.1"),
					Port: new(3000),
				},
				{
					Host: new("192.168.1.2"),
					Port: new(3001),
				},
				{
					Host:    new("192.168.1.3"),
					Port:    new(3002),
					TLSName: new("tls-name"),
				},
			},
		}
		var f flags.AerospikeFlags

		err := c.applySeeds(&f)

		require.NoError(t, err)
		assert.NotEmpty(t, f.Seeds)
	})

	t.Run("seed with empty tls name is ignored", func(t *testing.T) {
		c := &Cluster{
			Seeds: []ClusterSeed{
				{
					Host:    new("192.168.1.1"),
					Port:    new(3000),
					TLSName: new(""),
				},
			},
		}
		var f flags.AerospikeFlags

		err := c.applySeeds(&f)

		require.NoError(t, err)
		assert.NotEmpty(t, f.Seeds)
	})

	t.Run("seed with port 0 is ignored", func(t *testing.T) {
		c := &Cluster{
			Seeds: []ClusterSeed{
				{
					Host: new("192.168.1.1"),
					Port: new(0),
				},
			},
		}
		var f flags.AerospikeFlags

		err := c.applySeeds(&f)

		require.NoError(t, err)
		assert.NotEmpty(t, f.Seeds)
	})

	t.Run("seed with nil values uses defaults", func(t *testing.T) {
		c := &Cluster{
			Seeds: []ClusterSeed{
				{
					Host:    nil,
					Port:    nil,
					TLSName: nil,
				},
			},
		}
		var f flags.AerospikeFlags

		err := c.applySeeds(&f)

		assert.NoError(t, err)
	})
}

func TestCluster_applyAuthAndUser(t *testing.T) {
	t.Run("no auth fields - no error", func(t *testing.T) {
		c := &Cluster{}
		var f flags.AerospikeFlags

		err := c.applyAuthAndUser(&f)

		assert.NoError(t, err)
	})

	t.Run("user only", func(t *testing.T) {
		c := &Cluster{
			User: new("admin"),
		}
		var f flags.AerospikeFlags

		err := c.applyAuthAndUser(&f)

		require.NoError(t, err)
		assert.Equal(t, "admin", f.User)
	})

	t.Run("user and password", func(t *testing.T) {
		c := &Cluster{
			User:     new("admin"),
			Password: new("password123"),
		}
		var f flags.AerospikeFlags

		err := c.applyAuthAndUser(&f)

		require.NoError(t, err)
		assert.Equal(t, "admin", f.User)
		assert.NotEmpty(t, f.Password)
	})

	t.Run("user, password and auth mode", func(t *testing.T) {
		c := &Cluster{
			User:     new("admin"),
			Password: new("password123"),
			Auth:     new("INTERNAL"),
		}
		var f flags.AerospikeFlags

		err := c.applyAuthAndUser(&f)

		require.NoError(t, err)
		assert.Equal(t, "admin", f.User)
		assert.NotEmpty(t, f.Password.String())
		assert.NotEmpty(t, f.AuthMode.String())
	})

	t.Run("empty user is ignored", func(t *testing.T) {
		c := &Cluster{
			User: new(""),
		}
		var f flags.AerospikeFlags

		err := c.applyAuthAndUser(&f)

		require.NoError(t, err)
		assert.Empty(t, f.User)
	})

	t.Run("empty password is ignored", func(t *testing.T) {
		c := &Cluster{
			Password: new(""),
		}
		var f flags.AerospikeFlags

		err := c.applyAuthAndUser(&f)

		assert.NoError(t, err)
	})

	t.Run("empty auth mode is ignored", func(t *testing.T) {
		c := &Cluster{
			Auth: new(""),
		}
		var f flags.AerospikeFlags

		err := c.applyAuthAndUser(&f)

		assert.NoError(t, err)
	})

	t.Run("invalid auth mode returns error", func(t *testing.T) {
		c := &Cluster{
			Auth: new("INVALID_MODE"),
		}
		var f flags.AerospikeFlags

		err := c.applyAuthAndUser(&f)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to set auth mode")
	})

	t.Run("nil values are handled", func(t *testing.T) {
		c := &Cluster{
			User:     nil,
			Password: nil,
			Auth:     nil,
		}
		var f flags.AerospikeFlags

		err := c.applyAuthAndUser(&f)

		assert.NoError(t, err)
	})
}

func TestCluster_ToModelClientPolicy(t *testing.T) {
	t.Run("nil cluster returns nil", func(t *testing.T) {
		var c *Cluster

		policy := c.ToModelClientPolicy()

		assert.Nil(t, policy)
	})

	t.Run("empty cluster returns policy with zero values", func(t *testing.T) {
		c := &Cluster{}

		policy := c.ToModelClientPolicy()

		require.NotNil(t, policy)
		assert.Equal(t, int64(0), policy.Timeout)
		assert.Equal(t, int64(0), policy.IdleTimeout)
		assert.Equal(t, int64(0), policy.LoginTimeout)
	})

	t.Run("cluster with timeouts", func(t *testing.T) {
		c := &Cluster{
			ClientTimeout:      int64Ptr(5000),
			ClientIdleTimeout:  int64Ptr(10000),
			ClientLoginTimeout: int64Ptr(3000),
		}

		policy := c.ToModelClientPolicy()

		require.NotNil(t, policy)
		assert.Equal(t, int64(5000), policy.Timeout)
		assert.Equal(t, int64(10000), policy.IdleTimeout)
		assert.Equal(t, int64(3000), policy.LoginTimeout)
	})

	t.Run("nil timeout pointers return zero values", func(t *testing.T) {
		c := &Cluster{
			ClientTimeout:      nil,
			ClientIdleTimeout:  nil,
			ClientLoginTimeout: nil,
		}

		policy := c.ToModelClientPolicy()

		require.NotNil(t, policy)
		assert.Equal(t, int64(0), policy.Timeout)
		assert.Equal(t, int64(0), policy.IdleTimeout)
		assert.Equal(t, int64(0), policy.LoginTimeout)
	})
}
