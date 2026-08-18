// Package config holds the server's startup configuration.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// Config is everything the server needs to run. It is read from one JSON
// file (path in GRAPHENE_SERVER_CONFIG); every field has a sane dev
// default except the token list.
type Config struct {
	// ListenGRPC serves agents and the Temporal proxy — the single gRPC
	// door of the installation.
	ListenGRPC string `json:"listenGrpc"`
	// ListenHTTP serves the server API and the registry proxy.
	ListenHTTP string `json:"listenHttp"`
	// ExternalGRPC is the address workers and agents dial; handed out in
	// container env. Defaults to ListenGRPC.
	ExternalGRPC string `json:"externalGrpc"`
	// ExternalHTTP is the HTTP base URL workers use (blobs, secrets,
	// capabilities). Defaults to "http://"+ListenHTTP.
	ExternalHTTP string `json:"externalHttp"`

	// TemporalHostPort is the real Temporal frontend the proxy forwards
	// to. Nobody but the server ever sees it.
	TemporalHostPort  string `json:"temporalHostPort"`
	TemporalNamespace string `json:"temporalNamespace"`

	// Tokens is the static credential list of the installation: agents,
	// runs, admins. (A real token service replaces this; the shape on the
	// wire — bearer tokens with scopes — stays.)
	Tokens []Token `json:"tokens"`

	// Secrets maps names to values; only names ever travel. (Encrypted
	// storage replaces this file; the reference semantics stay.)
	Secrets map[string]string `json:"secrets"`

	// BlobDir is the artifact byte store of the dev contour (S3 later).
	BlobDir string `json:"blobDir"`

	// RegistryUpstream is the docker registry the /v2 proxy forwards to.
	RegistryUpstream string `json:"registryUpstream"`

	// AgentHeartbeat tunes the agent session cadence.
	AgentHeartbeat time.Duration `json:"-"`
	// AgentHeartbeatSeconds is the JSON form of AgentHeartbeat.
	AgentHeartbeatSeconds int `json:"agentHeartbeatSeconds"`
	// SweepSeconds is the stand-TTL sweeper period (default 30).
	SweepSeconds int `json:"sweepSeconds"`
	// ReapSeconds is the managed-run reaper period (default 10).
	ReapSeconds int `json:"reapSeconds"`
}

// Token is one credential with its scope.
type Token struct {
	Token string `json:"token"`
	// Role is "agent", "run", or "admin".
	Role string `json:"role"`
	// AgentId scopes an agent token to the one record it may embody.
	AgentId string `json:"agentId,omitempty"`
}

// EnvConfigPath names the config file location variable.
const EnvConfigPath = "GRAPHENE_SERVER_CONFIG"

// Load reads the config file named by GRAPHENE_SERVER_CONFIG.
func Load() (Config, error) {
	path := os.Getenv(EnvConfigPath)
	if path == "" {
		return Config{}, errors.New(EnvConfigPath + " is required")
	}
	raw, err := os.ReadFile(path) //nolint:gosec // the operator names the config file
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.defaults()
	return cfg, cfg.validate()
}

func (c *Config) defaults() {
	if c.ListenGRPC == "" {
		c.ListenGRPC = ":7233"
	}
	if c.ListenHTTP == "" {
		c.ListenHTTP = ":7280"
	}
	if c.ExternalGRPC == "" {
		c.ExternalGRPC = c.ListenGRPC
	}
	if c.ExternalHTTP == "" {
		c.ExternalHTTP = "http://" + c.ListenHTTP
	}
	if c.TemporalHostPort == "" {
		c.TemporalHostPort = "127.0.0.1:7234"
	}
	if c.TemporalNamespace == "" {
		c.TemporalNamespace = "default"
	}
	if c.AgentHeartbeatSeconds == 0 {
		c.AgentHeartbeatSeconds = 15
	}
	c.AgentHeartbeat = time.Duration(c.AgentHeartbeatSeconds) * time.Second
	if c.SweepSeconds == 0 {
		c.SweepSeconds = 30
	}
	if c.ReapSeconds == 0 {
		c.ReapSeconds = 10
	}
	if c.BlobDir == "" {
		c.BlobDir = "/var/lib/graphene-server/blobs"
	}
}

func (c *Config) validate() error {
	if len(c.Tokens) == 0 {
		return errors.New("no tokens configured: nobody could connect")
	}
	for i, t := range c.Tokens {
		if t.Token == "" {
			return fmt.Errorf("tokens[%d]: empty token", i)
		}
		switch t.Role {
		case "agent":
			if t.AgentId == "" {
				return fmt.Errorf("tokens[%d]: agent token needs agentId", i)
			}
		case "run", "admin":
		default:
			return fmt.Errorf("tokens[%d]: unknown role %q", i, t.Role)
		}
	}
	return nil
}
