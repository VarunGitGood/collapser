package config

import (
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		GRPCPort:            50052,
		MetricsPort:         2112,
		BackendAddress:      "backend:50051",
		BackendTimeout:      10 * time.Second,
		ResultCacheDuration: 100 * time.Millisecond,
		CleanupInterval:     time.Second,
		MaxCacheEntries:     10000,
	}
}

func TestValidate_AcceptsValidConfig(t *testing.T) {
	c := validConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidate_RejectsBadValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"port too low", func(c *Config) { c.GRPCPort = 0 }},
		{"port too high", func(c *Config) { c.GRPCPort = 70000 }},
		{"metrics port invalid", func(c *Config) { c.MetricsPort = -1 }},
		{"empty backend", func(c *Config) { c.BackendAddress = "" }},
		{"backend missing port", func(c *Config) { c.BackendAddress = "backend" }},
		{"backend is a url", func(c *Config) { c.BackendAddress = "http://backend:50051" }},
		{"non-positive timeout", func(c *Config) { c.BackendTimeout = 0 }},
		{"negative cache duration", func(c *Config) { c.ResultCacheDuration = -time.Second }},
		{"non-positive cleanup", func(c *Config) { c.CleanupInterval = 0 }},
		{"negative cache cap", func(c *Config) { c.MaxCacheEntries = -1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Errorf("Validate() accepted an invalid config")
			}
		})
	}
}

// A zero cache TTL disables caching rather than being an error, and an unlimited
// cache is expressed as zero. Both must pass validation.
func TestValidate_AcceptsDisablingValues(t *testing.T) {
	c := validConfig()
	c.ResultCacheDuration = 0
	c.MaxCacheEntries = 0
	if err := c.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil for caching-disabled config", err)
	}
}

func TestLoad_RequiresBackendAddress(t *testing.T) {
	t.Setenv("BACKEND_ADDRESS", "")
	if _, err := Load(); err == nil {
		t.Error("Load() succeeded without BACKEND_ADDRESS")
	}
}

func TestLoad_ParsesEnvironment(t *testing.T) {
	t.Setenv("BACKEND_ADDRESS", "backend.default.svc.cluster.local:50051")
	t.Setenv("COLLAPSER_CACHE_DURATION", "250ms")
	t.Setenv("COLLAPSER_KEY_HEADERS", "authorization,x-tenant-id")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if cfg.ResultCacheDuration != 250*time.Millisecond {
		t.Errorf("ResultCacheDuration = %v, want 250ms", cfg.ResultCacheDuration)
	}
	if len(cfg.KeyHeaders) != 2 || cfg.KeyHeaders[0] != "authorization" {
		t.Errorf("KeyHeaders = %v, want [authorization x-tenant-id]", cfg.KeyHeaders)
	}
	if cfg.GRPCPort != 50052 {
		t.Errorf("GRPCPort = %d, want the 50052 default", cfg.GRPCPort)
	}
}
