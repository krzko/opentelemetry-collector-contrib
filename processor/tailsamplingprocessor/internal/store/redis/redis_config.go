// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"fmt"
	"os"
	"time"
)

// Config holds Redis-specific configuration for the TraceBufferStore
type Config struct {
	// Client configuration
	Client ClientConfig `mapstructure:"client"`

	// Keyspace is the Redis key prefix (e.g., "ts:")
	Keyspace string `mapstructure:"keyspace"`

	// DefaultTTL is the default TTL for hot storage keys
	DefaultTTL time.Duration `mapstructure:"default_ttl"`

	// DecisionWait is how long to wait before evaluating policies
	DecisionWait time.Duration `mapstructure:"decision_wait"`

	// DecisionTTL is how long to cache decisions
	DecisionTTL time.Duration `mapstructure:"decision_ttl"`

	// LeaseTTL is the default TTL for trace evaluation leases
	LeaseTTL time.Duration `mapstructure:"lease_ttl"`

	// EnableAOF enables Redis Append-Only File persistence
	EnableAOF bool `mapstructure:"enable_aof"`

	// Retry configuration
	Retry RetryConfig `mapstructure:"retry"`
}

// ClientConfig holds Redis client configuration
type ClientConfig struct {
	// Endpoints is the list of Redis server addresses
	Endpoints []string `mapstructure:"endpoints"`

	// Cluster indicates whether to use Redis Cluster mode
	Cluster bool `mapstructure:"cluster"`

	// Username for Redis authentication (Redis 6.0+)
	Username string `mapstructure:"username"`

	// Password for Redis authentication
	Password string `mapstructure:"password"`

	// Database number to use (ignored in cluster mode)
	Database int `mapstructure:"database"`

	// TLS configuration
	TLS TLSConfig `mapstructure:"tls"`

	// Connection pool settings
	Pool PoolConfig `mapstructure:"pool"`

	// Timeouts
	Timeouts TimeoutConfig `mapstructure:"timeouts"`

	// CircuitBreaker configuration for resilience
	CircuitBreaker *CircuitBreakerConfig `mapstructure:"circuit_breaker"`
}

// TLSConfig holds TLS configuration for Redis connections
type TLSConfig struct {
	// Enabled indicates whether TLS should be used
	Enabled bool `mapstructure:"enabled"`

	// InsecureSkipVerify controls whether to skip certificate verification
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`

	// CertFile is the path to the client certificate file
	CertFile string `mapstructure:"cert_file"`

	// KeyFile is the path to the client private key file
	KeyFile string `mapstructure:"key_file"`

	// CAFile is the path to the certificate authority file
	CAFile string `mapstructure:"ca_file"`
}

// PoolConfig holds connection pool configuration
type PoolConfig struct {
	// MaxIdleConns is the maximum number of idle connections
	MaxIdleConns int `mapstructure:"max_idle_conns"`

	// MaxActiveConns is the maximum number of active connections
	MaxActiveConns int `mapstructure:"max_active_conns"`

	// ConnMaxLifetime is the maximum lifetime of a connection
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`

	// ConnMaxIdleTime is the maximum idle time for a connection
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`
}

// TimeoutConfig holds timeout configuration
type TimeoutConfig struct {
	// Connect timeout for establishing connections
	Connect time.Duration `mapstructure:"connect"`

	// Read timeout for Redis operations
	Read time.Duration `mapstructure:"read"`

	// Write timeout for Redis operations
	Write time.Duration `mapstructure:"write"`
}

// RetryConfig holds retry configuration for Redis operations
type RetryConfig struct {
	// Enabled indicates whether retries are enabled
	Enabled bool `mapstructure:"enabled"`

	// MaxAttempts is the maximum number of retry attempts
	MaxAttempts int `mapstructure:"max_attempts"`

	// InitialBackoff is the initial backoff duration
	InitialBackoff time.Duration `mapstructure:"initial_backoff"`

	// MaxBackoff is the maximum backoff duration
	MaxBackoff time.Duration `mapstructure:"max_backoff"`

	// BackoffMultiplier is the backoff multiplier for exponential backoff
	BackoffMultiplier float64 `mapstructure:"backoff_multiplier"`
}

// NewDefaultConfig returns a Redis configuration with sensible defaults
func NewDefaultConfig() Config {
	return Config{
		Client: ClientConfig{
			Endpoints: []string{"localhost:6379"},
			Cluster:   false,
			Username:  "",
			Password:  "",
			Database:  0,
			TLS: TLSConfig{
				Enabled:            false,
				InsecureSkipVerify: false,
			},
			Pool: PoolConfig{
				MaxIdleConns:    10,
				MaxActiveConns:  100,
				ConnMaxLifetime: time.Hour,
				ConnMaxIdleTime: 30 * time.Minute,
			},
			Timeouts: TimeoutConfig{
				Connect: 5 * time.Second,
				Read:    3 * time.Second,
				Write:   3 * time.Second,
			},
			CircuitBreaker: &CircuitBreakerConfig{
				FailureThreshold:    5,
				SuccessThreshold:    3,
				OpenDuration:        30 * time.Second,
				HalfOpenMaxAttempts: 3,
				ObservationWindow:   1 * time.Minute,
			},
		},
		Keyspace:     "ts:",
		DefaultTTL:   10 * time.Minute,
		DecisionWait: 30 * time.Second,
		DecisionTTL:  time.Hour,
		LeaseTTL:     10 * time.Second,
		EnableAOF:    false,
		Retry: RetryConfig{
			Enabled:           true,
			MaxAttempts:       3,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        5 * time.Second,
			BackoffMultiplier: 2.0,
		},
	}
}

// Validate validates the Redis configuration
func (c *Config) Validate() error {
	if len(c.Client.Endpoints) == 0 {
		return fmt.Errorf("at least one Redis endpoint must be specified")
	}

	if c.Keyspace == "" {
		return fmt.Errorf("keyspace cannot be empty")
	}

	if c.DefaultTTL <= 0 {
		return fmt.Errorf("default_ttl must be positive")
	}

	if c.DecisionWait <= 0 {
		return fmt.Errorf("decision_wait must be positive")
	}

	if c.DecisionTTL <= 0 {
		return fmt.Errorf("decision_ttl must be positive")
	}

	if c.LeaseTTL <= 0 {
		return fmt.Errorf("lease_ttl must be positive")
	}

	// Validate client configuration
	if err := c.Client.Validate(); err != nil {
		return fmt.Errorf("client config validation failed: %w", err)
	}

	// Validate retry configuration
	if err := c.Retry.Validate(); err != nil {
		return fmt.Errorf("retry config validation failed: %w", err)
	}

	return nil
}

// Validate validates the client configuration
func (c *ClientConfig) Validate() error {
	if c.Database < 0 || c.Database > 15 {
		return fmt.Errorf("database must be between 0 and 15")
	}

	if c.Pool.MaxIdleConns < 0 {
		return fmt.Errorf("max_idle_conns cannot be negative")
	}

	if c.Pool.MaxActiveConns <= 0 {
		return fmt.Errorf("max_active_conns must be positive")
	}

	if c.Pool.MaxIdleConns > c.Pool.MaxActiveConns {
		return fmt.Errorf("max_idle_conns cannot exceed max_active_conns")
	}

	if c.Timeouts.Connect <= 0 {
		return fmt.Errorf("connect timeout must be positive")
	}

	if c.Timeouts.Read <= 0 {
		return fmt.Errorf("read timeout must be positive")
	}

	if c.Timeouts.Write <= 0 {
		return fmt.Errorf("write timeout must be positive")
	}

	// Validate TLS configuration
	if c.TLS.Enabled {
		if c.TLS.CertFile != "" && !fileExists(c.TLS.CertFile) {
			return fmt.Errorf("TLS cert file does not exist: %s", c.TLS.CertFile)
		}

		if c.TLS.KeyFile != "" && !fileExists(c.TLS.KeyFile) {
			return fmt.Errorf("TLS key file does not exist: %s", c.TLS.KeyFile)
		}

		if c.TLS.CAFile != "" && !fileExists(c.TLS.CAFile) {
			return fmt.Errorf("TLS CA file does not exist: %s", c.TLS.CAFile)
		}
	}

	// Validate circuit breaker configuration if provided
	if c.CircuitBreaker != nil {
		if err := c.CircuitBreaker.Validate(); err != nil {
			return fmt.Errorf("circuit breaker config validation failed: %w", err)
		}
	}

	return nil
}

// Validate validates the retry configuration
func (r *RetryConfig) Validate() error {
	if r.MaxAttempts < 1 {
		return fmt.Errorf("max_attempts must be at least 1")
	}

	if r.InitialBackoff <= 0 {
		return fmt.Errorf("initial_backoff must be positive")
	}

	if r.MaxBackoff <= 0 {
		return fmt.Errorf("max_backoff must be positive")
	}

	if r.InitialBackoff > r.MaxBackoff {
		return fmt.Errorf("initial_backoff cannot exceed max_backoff")
	}

	if r.BackoffMultiplier <= 1.0 {
		return fmt.Errorf("backoff_multiplier must be greater than 1.0")
	}

	return nil
}

// fileExists checks if a file exists
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
