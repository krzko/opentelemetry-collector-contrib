// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// RedisClient abstracts Redis operations to support both single-node and cluster modes
type RedisClient interface {
	// Basic operations
	Ping(ctx context.Context) error
	Close() error
	
	// Script operations
	ScriptLoad(ctx context.Context, script string) (string, error)
	EvalSHA(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error)
	
	// Hash operations
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HSet(ctx context.Context, key string, values ...interface{}) error
	HGet(ctx context.Context, key string, field string) (string, error)
	
	// List operations
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	LLen(ctx context.Context, key string) (int64, error)
	
	// String operations
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	
	// Set operations for active traces
	ZAdd(ctx context.Context, key string, members ...redis.Z) error
	ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error)
	ZRem(ctx context.Context, key string, members ...interface{}) error
	
	// TTL operations
	Expire(ctx context.Context, key string, expiration time.Duration) error
	PExpire(ctx context.Context, key string, expiration time.Duration) error
	
	// Transaction support
	Pipeline() redis.Pipeliner
	TxPipeline() redis.Pipeliner
}

// redisClientImpl implements RedisClient for single-node Redis
type redisClientImpl struct {
	client         *redis.Client
	logger         *zap.Logger
	circuitBreaker *CircuitBreaker
	
	// Reconnection management
	config          ClientConfig
	reconnectMu     sync.Mutex
	isReconnecting  atomic.Bool
	lastReconnect   time.Time
	reconnectDelay  time.Duration
	maxReconnectDelay time.Duration
	
	// Shutdown
	closed  atomic.Bool
	closeCh chan struct{}
}

// redisClusterClientImpl implements RedisClient for Redis Cluster
type redisClusterClientImpl struct {
	client         *redis.ClusterClient
	logger         *zap.Logger
	circuitBreaker *CircuitBreaker
	
	// Reconnection management
	config          ClientConfig
	reconnectMu     sync.Mutex
	isReconnecting  atomic.Bool
	lastReconnect   time.Time
	reconnectDelay  time.Duration
	maxReconnectDelay time.Duration
	
	// Shutdown
	closed  atomic.Bool
	closeCh chan struct{}
}

// NewRedisClient creates a new Redis client based on the configuration
func NewRedisClient(config ClientConfig) (RedisClient, error) {
	if config.Cluster {
		return newClusterClient(config)
	}
	return newSingleClient(config)
}

// newSingleClient creates a single-node Redis client
func newSingleClient(config ClientConfig) (RedisClient, error) {
	opts := &redis.Options{
		Addr:         config.Endpoints[0], // Use first endpoint for single-node
		Username:     config.Username,
		Password:     config.Password,
		DB:           config.Database,
		MaxIdleConns: config.Pool.MaxIdleConns,
		// MaxOpenConns field not available in go-redis/v9, using PoolSize instead
		PoolSize: config.Pool.MaxActiveConns,
		ConnMaxLifetime: config.Pool.ConnMaxLifetime,
		ConnMaxIdleTime: config.Pool.ConnMaxIdleTime,
		DialTimeout:     config.Timeouts.Connect,
		ReadTimeout:     config.Timeouts.Read,
		WriteTimeout:    config.Timeouts.Write,
	}
	
	// Configure TLS if enabled
	if config.TLS.Enabled {
		tlsConfig, err := createTLSConfig(config.TLS)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.TLSConfig = tlsConfig
	}
	
	client := redis.NewClient(opts)
	
	// Create circuit breaker if configured
	var circuitBreaker *CircuitBreaker
	if config.CircuitBreaker != nil {
		config.CircuitBreaker.SetDefaults()
		cb, err := NewCircuitBreaker(*config.CircuitBreaker)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to create circuit breaker: %w", err)
		}
		circuitBreaker = cb
	}
	
	rc := &redisClientImpl{
		client:            client,
		logger:            zap.L().With(zap.String("component", "redis-client")),
		circuitBreaker:    circuitBreaker,
		config:            config,
		reconnectDelay:    1 * time.Second,
		maxReconnectDelay: 30 * time.Second,
		closeCh:           make(chan struct{}),
	}
	
	// Set circuit breaker callback if configured
	if circuitBreaker != nil {
		circuitBreaker.SetOnStateChange(rc.onCircuitStateChange)
		go rc.healthCheckLoop()
	}
	
	return rc, nil
}

// newClusterClient creates a Redis Cluster client
func newClusterClient(config ClientConfig) (RedisClient, error) {
	opts := &redis.ClusterOptions{
		Addrs:        config.Endpoints,
		Username:     config.Username,
		Password:     config.Password,
		MaxIdleConns: config.Pool.MaxIdleConns,
		DialTimeout:  config.Timeouts.Connect,
		ReadTimeout:  config.Timeouts.Read,
		WriteTimeout: config.Timeouts.Write,
	}
	
	// Configure TLS if enabled
	if config.TLS.Enabled {
		tlsConfig, err := createTLSConfig(config.TLS)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.TLSConfig = tlsConfig
	}
	
	client := redis.NewClusterClient(opts)
	
	// Create circuit breaker if configured
	var circuitBreaker *CircuitBreaker
	if config.CircuitBreaker != nil {
		config.CircuitBreaker.SetDefaults()
		cb, err := NewCircuitBreaker(*config.CircuitBreaker)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to create circuit breaker: %w", err)
		}
		circuitBreaker = cb
	}
	
	rc := &redisClusterClientImpl{
		client:            client,
		logger:            zap.L().With(zap.String("component", "redis-cluster-client")),
		circuitBreaker:    circuitBreaker,
		config:            config,
		reconnectDelay:    1 * time.Second,
		maxReconnectDelay: 30 * time.Second,
		closeCh:           make(chan struct{}),
	}
	
	// Set circuit breaker callback if configured
	if circuitBreaker != nil {
		circuitBreaker.SetOnStateChange(rc.onCircuitStateChange)
		go rc.healthCheckLoop()
	}
	
	return rc, nil
}

// createTLSConfig creates a TLS configuration from the provided settings
func createTLSConfig(config TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.InsecureSkipVerify,
	}
	
	// Load client certificate if provided
	if config.CertFile != "" && config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	
	// Load CA certificate if provided
	if config.CAFile != "" {
		caCert, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		
		// Note: In production, you would want to properly add the CA cert to the cert pool
		// This is a simplified implementation
		_ = caCert // TODO: Add to cert pool
	}
	
	return tlsConfig, nil
}

// healthCheckLoop periodically checks Redis health (single-node)
func (r *redisClientImpl) healthCheckLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-r.closeCh:
			return
		case <-ticker.C:
			if r.closed.Load() {
				return
			}
			
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := r.Ping(ctx)
			cancel()
			
			if err != nil && err != ErrCircuitOpen {
				r.logger.Warn("Redis health check failed", zap.Error(err))
				if !r.isReconnecting.Load() {
					go r.reconnect()
				}
			}
		}
	}
}

// reconnect attempts to reconnect to Redis (single-node)
func (r *redisClientImpl) reconnect() {
	r.reconnectMu.Lock()
	if r.isReconnecting.Load() {
		r.reconnectMu.Unlock()
		return
	}
	r.isReconnecting.Store(true)
	r.reconnectMu.Unlock()
	
	defer r.isReconnecting.Store(false)
	
	r.logger.Info("Starting Redis reconnection attempt")
	
	for !r.closed.Load() {
		select {
		case <-time.After(r.reconnectDelay):
		case <-r.closeCh:
			return
		}
		
		// Create new client
		newOpts := &redis.Options{
			Addr:            r.config.Endpoints[0],
			Username:        r.config.Username,
			Password:        r.config.Password,
			DB:              r.config.Database,
			MaxIdleConns:    r.config.Pool.MaxIdleConns,
			PoolSize:        r.config.Pool.MaxActiveConns,
			ConnMaxLifetime: r.config.Pool.ConnMaxLifetime,
			ConnMaxIdleTime: r.config.Pool.ConnMaxIdleTime,
			DialTimeout:     r.config.Timeouts.Connect,
			ReadTimeout:     r.config.Timeouts.Read,
			WriteTimeout:    r.config.Timeouts.Write,
		}
		
		if r.config.TLS.Enabled {
			tlsConfig, err := createTLSConfig(r.config.TLS)
			if err != nil {
				r.logger.Error("Failed to create TLS config", zap.Error(err))
				continue
			}
			newOpts.TLSConfig = tlsConfig
		}
		
		newClient := redis.NewClient(newOpts)
		
		// Test connection
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := newClient.Ping(ctx).Err()
		cancel()
		
		if err != nil {
			newClient.Close()
			r.logger.Error("New Redis connection failed", zap.Error(err))
			r.reconnectDelay *= 2
			if r.reconnectDelay > r.maxReconnectDelay {
				r.reconnectDelay = r.maxReconnectDelay
			}
			continue
		}
		
		// Success - replace client
		oldClient := r.client
		r.client = newClient
		oldClient.Close()
		
		r.lastReconnect = time.Now()
		r.reconnectDelay = 1 * time.Second
		
		if r.circuitBreaker != nil {
			r.circuitBreaker.Reset()
		}
		
		r.logger.Info("Successfully reconnected to Redis")
		return
	}
}

// onCircuitStateChange handles circuit breaker state changes
func (r *redisClientImpl) onCircuitStateChange(from, to CircuitState) {
	r.logger.Info("Circuit breaker state changed",
		zap.String("from", from.String()),
		zap.String("to", to.String()))
	
	if to == StateClosed {
		// Circuit recovered, ensure connection is healthy
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := r.client.Ping(ctx).Err()
		cancel()
		
		if err != nil {
			r.logger.Warn("Redis still unhealthy after circuit close", zap.Error(err))
			if !r.isReconnecting.Load() {
				go r.reconnect()
			}
		}
	}
}

// wrapCall wraps a Redis operation with circuit breaker if configured
func (r *redisClientImpl) wrapCall(ctx context.Context, fn func() error) error {
	if r.closed.Load() {
		return fmt.Errorf("client is closed")
	}
	
	if r.circuitBreaker != nil {
		return r.circuitBreaker.Call(ctx, fn)
	}
	return fn()
}

// redisClientImpl implementations

func (r *redisClientImpl) Ping(ctx context.Context) error {
	return r.wrapCall(ctx, func() error {
		return r.client.Ping(ctx).Err()
	})
}

func (r *redisClientImpl) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	
	close(r.closeCh)
	return r.client.Close()
}

func (r *redisClientImpl) ScriptLoad(ctx context.Context, script string) (string, error) {
	var result string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.ScriptLoad(ctx, script).Result()
		return err
	})
	return result, err
}

func (r *redisClientImpl) EvalSHA(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
	var result interface{}
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.EvalSha(ctx, sha, keys, args...).Result()
		return err
	})
	return result, err
}

func (r *redisClientImpl) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	var result map[string]string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.HGetAll(ctx, key).Result()
		return err
	})
	return result, err
}

func (r *redisClientImpl) HSet(ctx context.Context, key string, values ...interface{}) error {
	return r.wrapCall(ctx, func() error {
		return r.client.HSet(ctx, key, values...).Err()
	})
}

func (r *redisClientImpl) HGet(ctx context.Context, key string, field string) (string, error) {
	var result string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.HGet(ctx, key, field).Result()
		return err
	})
	return result, err
}

func (r *redisClientImpl) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	var result []string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.LRange(ctx, key, start, stop).Result()
		return err
	})
	return result, err
}

func (r *redisClientImpl) LLen(ctx context.Context, key string) (int64, error) {
	var result int64
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.LLen(ctx, key).Result()
		return err
	})
	return result, err
}

func (r *redisClientImpl) Get(ctx context.Context, key string) (string, error) {
	var result string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.Get(ctx, key).Result()
		return err
	})
	return result, err
}

func (r *redisClientImpl) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return r.wrapCall(ctx, func() error {
		return r.client.Set(ctx, key, value, expiration).Err()
	})
}

func (r *redisClientImpl) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	return r.wrapCall(ctx, func() error {
		return r.client.ZAdd(ctx, key, members...).Err()
	})
}

func (r *redisClientImpl) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	var result []string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.ZRangeByScore(ctx, key, opt).Result()
		return err
	})
	return result, err
}

func (r *redisClientImpl) ZRem(ctx context.Context, key string, members ...interface{}) error {
	return r.wrapCall(ctx, func() error {
		return r.client.ZRem(ctx, key, members...).Err()
	})
}

func (r *redisClientImpl) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return r.wrapCall(ctx, func() error {
		return r.client.Expire(ctx, key, expiration).Err()
	})
}

func (r *redisClientImpl) PExpire(ctx context.Context, key string, expiration time.Duration) error {
	return r.wrapCall(ctx, func() error {
		return r.client.PExpire(ctx, key, expiration).Err()
	})
}

func (r *redisClientImpl) Pipeline() redis.Pipeliner {
	return r.client.Pipeline()
}

func (r *redisClientImpl) TxPipeline() redis.Pipeliner {
	return r.client.TxPipeline()
}

// healthCheckLoop periodically checks Redis health (cluster)
func (r *redisClusterClientImpl) healthCheckLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-r.closeCh:
			return
		case <-ticker.C:
			if r.closed.Load() {
				return
			}
			
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := r.Ping(ctx)
			cancel()
			
			if err != nil && err != ErrCircuitOpen {
				r.logger.Warn("Redis cluster health check failed", zap.Error(err))
				if !r.isReconnecting.Load() {
					go r.reconnect()
				}
			}
		}
	}
}

// reconnect attempts to reconnect to Redis cluster
func (r *redisClusterClientImpl) reconnect() {
	r.reconnectMu.Lock()
	if r.isReconnecting.Load() {
		r.reconnectMu.Unlock()
		return
	}
	r.isReconnecting.Store(true)
	r.reconnectMu.Unlock()
	
	defer r.isReconnecting.Store(false)
	
	r.logger.Info("Starting Redis cluster reconnection attempt")
	
	for !r.closed.Load() {
		select {
		case <-time.After(r.reconnectDelay):
		case <-r.closeCh:
			return
		}
		
		// Create new cluster client
		newOpts := &redis.ClusterOptions{
			Addrs:        r.config.Endpoints,
			Username:     r.config.Username,
			Password:     r.config.Password,
			MaxIdleConns: r.config.Pool.MaxIdleConns,
			DialTimeout:  r.config.Timeouts.Connect,
			ReadTimeout:  r.config.Timeouts.Read,
			WriteTimeout: r.config.Timeouts.Write,
		}
		
		if r.config.TLS.Enabled {
			tlsConfig, err := createTLSConfig(r.config.TLS)
			if err != nil {
				r.logger.Error("Failed to create TLS config", zap.Error(err))
				continue
			}
			newOpts.TLSConfig = tlsConfig
		}
		
		newClient := redis.NewClusterClient(newOpts)
		
		// Test connection
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := newClient.Ping(ctx).Err()
		cancel()
		
		if err != nil {
			newClient.Close()
			r.logger.Error("New Redis cluster connection failed", zap.Error(err))
			r.reconnectDelay *= 2
			if r.reconnectDelay > r.maxReconnectDelay {
				r.reconnectDelay = r.maxReconnectDelay
			}
			continue
		}
		
		// Success - replace client
		oldClient := r.client
		r.client = newClient
		oldClient.Close()
		
		r.lastReconnect = time.Now()
		r.reconnectDelay = 1 * time.Second
		
		if r.circuitBreaker != nil {
			r.circuitBreaker.Reset()
		}
		
		r.logger.Info("Successfully reconnected to Redis cluster")
		return
	}
}

// onCircuitStateChange handles circuit breaker state changes
func (r *redisClusterClientImpl) onCircuitStateChange(from, to CircuitState) {
	r.logger.Info("Circuit breaker state changed",
		zap.String("from", from.String()),
		zap.String("to", to.String()))
	
	if to == StateClosed {
		// Circuit recovered, ensure connection is healthy
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := r.client.Ping(ctx).Err()
		cancel()
		
		if err != nil {
			r.logger.Warn("Redis cluster still unhealthy after circuit close", zap.Error(err))
			if !r.isReconnecting.Load() {
				go r.reconnect()
			}
		}
	}
}

// wrapCall wraps a Redis operation with circuit breaker if configured
func (r *redisClusterClientImpl) wrapCall(ctx context.Context, fn func() error) error {
	if r.closed.Load() {
		return fmt.Errorf("client is closed")
	}
	
	if r.circuitBreaker != nil {
		return r.circuitBreaker.Call(ctx, fn)
	}
	return fn()
}

// redisClusterClientImpl implementations

func (r *redisClusterClientImpl) Ping(ctx context.Context) error {
	return r.wrapCall(ctx, func() error {
		return r.client.Ping(ctx).Err()
	})
}

func (r *redisClusterClientImpl) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	
	close(r.closeCh)
	return r.client.Close()
}

func (r *redisClusterClientImpl) ScriptLoad(ctx context.Context, script string) (string, error) {
	var result string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.ScriptLoad(ctx, script).Result()
		return err
	})
	return result, err
}

func (r *redisClusterClientImpl) EvalSHA(ctx context.Context, sha string, keys []string, args []interface{}) (interface{}, error) {
	var result interface{}
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.EvalSha(ctx, sha, keys, args...).Result()
		return err
	})
	return result, err
}

func (r *redisClusterClientImpl) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	var result map[string]string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.HGetAll(ctx, key).Result()
		return err
	})
	return result, err
}

func (r *redisClusterClientImpl) HSet(ctx context.Context, key string, values ...interface{}) error {
	return r.wrapCall(ctx, func() error {
		return r.client.HSet(ctx, key, values...).Err()
	})
}

func (r *redisClusterClientImpl) HGet(ctx context.Context, key string, field string) (string, error) {
	var result string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.HGet(ctx, key, field).Result()
		return err
	})
	return result, err
}

func (r *redisClusterClientImpl) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	var result []string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.LRange(ctx, key, start, stop).Result()
		return err
	})
	return result, err
}

func (r *redisClusterClientImpl) LLen(ctx context.Context, key string) (int64, error) {
	var result int64
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.LLen(ctx, key).Result()
		return err
	})
	return result, err
}

func (r *redisClusterClientImpl) Get(ctx context.Context, key string) (string, error) {
	var result string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.Get(ctx, key).Result()
		return err
	})
	return result, err
}

func (r *redisClusterClientImpl) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return r.wrapCall(ctx, func() error {
		return r.client.Set(ctx, key, value, expiration).Err()
	})
}

func (r *redisClusterClientImpl) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	return r.wrapCall(ctx, func() error {
		return r.client.ZAdd(ctx, key, members...).Err()
	})
}

func (r *redisClusterClientImpl) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) ([]string, error) {
	var result []string
	err := r.wrapCall(ctx, func() error {
		var err error
		result, err = r.client.ZRangeByScore(ctx, key, opt).Result()
		return err
	})
	return result, err
}

func (r *redisClusterClientImpl) ZRem(ctx context.Context, key string, members ...interface{}) error {
	return r.wrapCall(ctx, func() error {
		return r.client.ZRem(ctx, key, members...).Err()
	})
}

func (r *redisClusterClientImpl) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return r.wrapCall(ctx, func() error {
		return r.client.Expire(ctx, key, expiration).Err()
	})
}

func (r *redisClusterClientImpl) PExpire(ctx context.Context, key string, expiration time.Duration) error {
	return r.wrapCall(ctx, func() error {
		return r.client.PExpire(ctx, key, expiration).Err()
	})
}

func (r *redisClusterClientImpl) Pipeline() redis.Pipeliner {
	return r.client.Pipeline()
}

func (r *redisClusterClientImpl) TxPipeline() redis.Pipeliner {
	return r.client.TxPipeline()
}

// generateOwnerID creates a unique identifier for this process
func generateOwnerID() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	
	pid := os.Getpid()
	
	// Include a timestamp component to ensure uniqueness across restarts
	timestamp := time.Now().Unix()
	
	return fmt.Sprintf("%s-%d-%d", strings.ReplaceAll(hostname, ":", "_"), pid, timestamp), nil
}