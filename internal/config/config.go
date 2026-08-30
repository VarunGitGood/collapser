package config

import (
	"fmt"
	"net"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	// Server
	GRPCPort    int `envconfig:"GRPC_PORT" default:"50052"`
	MetricsPort int `envconfig:"METRICS_PORT" default:"2112"`

	// Backend
	BackendAddress string        `envconfig:"BACKEND_ADDRESS" required:"true"`
	BackendTimeout time.Duration `envconfig:"BACKEND_TIMEOUT" default:"10s"`
	BackendUseTLS  bool          `envconfig:"BACKEND_USE_TLS" default:"false"`

	// Collapser
	ResultCacheDuration time.Duration `envconfig:"COLLAPSER_CACHE_DURATION" default:"100ms"`
	CleanupInterval     time.Duration `envconfig:"COLLAPSER_CLEANUP_INTERVAL" default:"1s"`
	// CacheErrors controls whether backend errors are stored in the result cache.
	// Disabled by default: a single transient error should not block all callers
	// for the full ResultCacheDuration.
	CacheErrors bool `envconfig:"COLLAPSER_CACHE_ERRORS" default:"false"`
	// MaxCacheEntries caps the result cache. Zero means unlimited.
	MaxCacheEntries int `envconfig:"COLLAPSER_MAX_CACHE_ENTRIES" default:"10000"`
	// KeyHeaders lists incoming metadata headers folded into the collapse key, so
	// requests differing only in those headers are never collapsed together.
	// Set this to any header the backend varies its response by (e.g. authorization,
	// x-tenant-id) or collapsing can serve one caller's response to another.
	KeyHeaders []string `envconfig:"COLLAPSER_KEY_HEADERS"`

	// Logging
	LogLevel  string `envconfig:"LOG_LEVEL" default:"info"`
	LogFormat string `envconfig:"LOG_FORMAT" default:"json"`
}

func Load() (*Config, error) {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.GRPCPort < 1 || c.GRPCPort > 65535 {
		return fmt.Errorf("invalid GRPC_PORT: %d", c.GRPCPort)
	}
	if c.MetricsPort < 1 || c.MetricsPort > 65535 {
		return fmt.Errorf("invalid METRICS_PORT: %d", c.MetricsPort)
	}
	if c.BackendAddress == "" {
		return fmt.Errorf("BACKEND_ADDRESS cannot be empty")
	}
	if _, _, err := net.SplitHostPort(c.BackendAddress); err != nil {
		return fmt.Errorf("BACKEND_ADDRESS must be host:port, got %q: %w", c.BackendAddress, err)
	}
	if c.BackendTimeout <= 0 {
		return fmt.Errorf("BACKEND_TIMEOUT must be positive")
	}
	if c.ResultCacheDuration < 0 {
		return fmt.Errorf("COLLAPSER_CACHE_DURATION must be non-negative")
	}
	if c.CleanupInterval <= 0 {
		return fmt.Errorf("COLLAPSER_CLEANUP_INTERVAL must be positive")
	}
	if c.MaxCacheEntries < 0 {
		return fmt.Errorf("COLLAPSER_MAX_CACHE_ENTRIES must be non-negative")
	}
	return nil
}
