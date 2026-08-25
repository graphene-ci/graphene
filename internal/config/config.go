// Package config holds the server's startup configuration: a YAML file
// (optional) overlaid by environment variables — the whole config is
// env-expressible for container deployments. Powered by xconf's
// structconf: `mapstructure` names, `default` values, env names derived
// from the dotted path under the GRAPHENE prefix
// (server.grpc -> GRAPHENE_SERVER_GRPC).
package config

import (
	"fmt"
	"github.com/graphene-ci/graphene/internal/runtimes"
	"os"
	"strings"
	"time"

	"github.com/gopherex/xconf/pkg/structconf"
	"gopkg.in/yaml.v3"
)

// EnvConfigPath names the config file location variable; unset means
// env-only (with /etc/graphene/server.yaml picked up when present).
const EnvConfigPath = "GRAPHENE_SERVER_CONFIG"

// File is the on-disk/env shape of the configuration.
type File struct {
	Server struct {
		// Listen is THE port: gRPC (agents, workers, the Temporal proxy,
		// the worker and management planes), ConnectRPC for browsers,
		// probes, and the registry proxy — one listener, split by
		// content. A TLS proxy in front (caddy: reverse_proxy
		// h2c://host:port) terminates TLS for everything at once.
		Listen string `mapstructure:"listen" default:":7233"`
		// External is the address agents, workers, and managed
		// containers dial ("host:port"); defaults from Listen.
		External string `mapstructure:"external"`
	} `mapstructure:"server"`

	Temporal struct {
		HostPort  string `mapstructure:"host_port" default:"127.0.0.1:7234" validate:"required"`
		Namespace string `mapstructure:"namespace" default:"default"`
	} `mapstructure:"temporal"`

	Registry struct {
		// Upstream is the docker registry behind the /v2 proxy.
		Upstream string `mapstructure:"upstream"`
	} `mapstructure:"registry"`

	Blobs struct {
		// Backend: file | s3.
		Backend string `mapstructure:"backend" default:"file" validate:"oneof=file s3"`
		Dir     string `mapstructure:"dir" default:"/var/lib/graphene-server/blobs"`
		S3      struct {
			Endpoint  string `mapstructure:"endpoint"`
			Bucket    string `mapstructure:"bucket" default:"graphene-blobs"`
			AccessKey string `mapstructure:"access_key"`
			SecretKey string `mapstructure:"secret_key"`
			UseSSL    bool   `mapstructure:"use_ssl"`
		} `mapstructure:"s3"`
	} `mapstructure:"blobs"`

	Auth struct {
		// AdminTokens / RunTokens are comma-separated "token[@namespace]"
		// lists; an admin token defaults to every namespace ("*"), a run
		// token to "default".
		AdminTokens string `mapstructure:"admin_tokens"`
		RunTokens   string `mapstructure:"run_tokens"`
		// AgentTokens is comma-separated "agentId:token[@namespace]".
		AgentTokens string `mapstructure:"agent_tokens"`
	} `mapstructure:"auth"`

	Secrets struct {
		// File is a YAML file of name -> value; Values inlines them
		// (file wins on collision). Only names ever leave the server.
		File   string            `mapstructure:"file"`
		Values map[string]string `mapstructure:"values"`
		// Store is the sealed value store on the server's volume;
		// with Key set, secrets survive restarts (AES-GCM). Empty Key
		// keeps the in-memory store (dev only, values die with the
		// process).
		Store string `mapstructure:"store" default:"/var/lib/graphene/secrets.enc"`
		// Key is the installation's master key: 64 hex chars.
		Key string `mapstructure:"key"`
	} `mapstructure:"secrets"`

	Vars struct {
		// Values seeds name -> value. A variable is the visible sibling
		// of a secret: environment configuration params reference as
		// "${var:name}" — substituted by the door on run start.
		Values map[string]string `mapstructure:"values"`
		// Store persists variables on the server's volume, sealed with
		// the secrets key when one is set.
		Store string `mapstructure:"store" default:"/var/lib/graphene/vars.enc"`
	} `mapstructure:"vars"`

	// Identity is where PEOPLE come from: an OIDC provider verifies
	// them, this installation never stores a password. Empty means
	// service accounts only.
	Identity struct {
		// Issuer is the provider's issuer URL.
		Issuer string `mapstructure:"issuer"`
		// Audience is this installation's client id at the provider.
		Audience string `mapstructure:"audience"`
		// UsernameClaim names the claim used as the subject
		// ("sub" by default; "email" is common).
		UsernameClaim string `mapstructure:"username_claim"`
		// GroupsClaim names the claim carrying group membership.
		GroupsClaim string `mapstructure:"groups_claim"`
		// SigningKey signs the short-lived tokens of runs and agents.
		// Empty falls back to the secrets key, and without either the
		// minted contour is unavailable.
		SigningKey string `mapstructure:"signing_key"`
	} `mapstructure:"identity"`

	// Runtimes extends or overrides the toolchain catalogue: adding a
	// language to an installation is configuration, not a code change.
	Runtimes []runtimes.Runtime `mapstructure:"runtimes"`

	Otel struct {
		// The OTLP/HTTP ingest URLs the door forwards each signal to —
		// the backends speak OTLP directly, no collector needed. Empty
		// accepts and drops that signal.
		Traces  string `mapstructure:"traces"`
		Logs    string `mapstructure:"logs"`
		Metrics string `mapstructure:"metrics"`
		// Query names the read side ObserveAPI proxies — STANDARD
		// surfaces: a PromQL base URL for metrics, a Jaeger API base
		// URL for traces; logs are the one signal without a de-facto
		// standard (LogsQL base URL, isolated behind a driver).
		Query struct {
			Metrics string `mapstructure:"metrics"`
			Logs    string `mapstructure:"logs"`
			Traces  string `mapstructure:"traces"`
		} `mapstructure:"query"`
	} `mapstructure:"otel"`

	Log struct {
		// Level: debug | info | warn | error.
		Level string `mapstructure:"level" default:"info" validate:"oneof=debug info warn error"`
		// Format: json (production) | console (humans).
		Format string `mapstructure:"format" default:"json" validate:"oneof=json console"`
	} `mapstructure:"log"`

	Intervals struct {
		AgentHeartbeatSeconds int `mapstructure:"agent_heartbeat_seconds" default:"15"`
		SweepSeconds          int `mapstructure:"sweep_seconds" default:"30"`
		ReapSeconds           int `mapstructure:"reap_seconds" default:"10"`
	} `mapstructure:"intervals"`
}

// Config is the resolved runtime configuration the server composes from.
type Config struct {
	LogLevel  string
	LogFormat string
	// Version is the build version, stamped by main.
	Version string

	Listen   string
	External string

	TemporalHostPort  string
	TemporalNamespace string

	Tokens  []Token
	Secrets map[string]string
	// SecretsStore/SecretsKey open the sealed value store; empty key
	// keeps secrets in memory.
	SecretsStore string
	SecretsKey   string
	// Runtimes is the installation's toolchain catalogue.
	Runtimes  []runtimes.Runtime
	Vars      map[string]string
	VarsStore string
	// Identity wires the OIDC provider (people) and the signing key of
	// minted tokens (runs and agents).
	OIDCIssuer        string
	OIDCAudience      string
	OIDCUsernameClaim string
	OIDCGroupsClaim   string
	SigningKey        string

	BlobBackend      string
	BlobDir          string
	BlobS3           S3
	RegistryUpstream string
	OtelTraces       string
	OtelLogs         string
	OtelMetrics      string
	QueryMetrics     string
	QueryLogs        string
	QueryTraces      string

	AgentHeartbeat        time.Duration
	AgentHeartbeatSeconds int
	SweepSeconds          int
	ReapSeconds           int
}

// S3 is the blob store's S3 wiring.
type S3 struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	UseSSL    bool
}

// Token is one credential with its scope.
type Token struct {
	Token string
	// Role is "agent", "run", or "admin".
	Role string
	// Namespace scopes the token; "*" is every namespace (admins).
	Namespace string
	// AgentId scopes an agent token to the one record it may embody.
	AgentId string
}

// Load reads the file named by GRAPHENE_SERVER_CONFIG (or the default
// path when present) and overlays the environment.
func Load() (Config, error) {
	opts := []structconf.Option{structconf.WithEnvPrefix("GRAPHENE")}
	switch path := os.Getenv(EnvConfigPath); {
	case path != "":
		opts = append(opts, structconf.WithYAMLFile(path))
	default:
		opts = append(opts, structconf.WithYAMLFileOptional("/etc/graphene/server.yaml"))
	}
	f, err := structconf.Load[File](opts...)
	if err != nil {
		return Config{}, err
	}
	return Resolve(*f)
}

// Resolve turns the file shape into the runtime configuration.
func Resolve(f File) (Config, error) {
	cfg := Config{
		LogLevel:          f.Log.Level,
		LogFormat:         f.Log.Format,
		Listen:            f.Server.Listen,
		External:          f.Server.External,
		TemporalHostPort:  f.Temporal.HostPort,
		TemporalNamespace: f.Temporal.Namespace,
		BlobBackend:       f.Blobs.Backend,
		BlobDir:           f.Blobs.Dir,
		BlobS3: S3{
			Endpoint:  f.Blobs.S3.Endpoint,
			Bucket:    f.Blobs.S3.Bucket,
			AccessKey: f.Blobs.S3.AccessKey,
			SecretKey: f.Blobs.S3.SecretKey,
			UseSSL:    f.Blobs.S3.UseSSL,
		},
		RegistryUpstream:      f.Registry.Upstream,
		OtelTraces:            f.Otel.Traces,
		OtelLogs:              f.Otel.Logs,
		OtelMetrics:           f.Otel.Metrics,
		QueryMetrics:          f.Otel.Query.Metrics,
		QueryLogs:             f.Otel.Query.Logs,
		QueryTraces:           f.Otel.Query.Traces,
		AgentHeartbeatSeconds: f.Intervals.AgentHeartbeatSeconds,
		SweepSeconds:          f.Intervals.SweepSeconds,
		ReapSeconds:           f.Intervals.ReapSeconds,
		Secrets:               map[string]string{},
		SecretsStore:          f.Secrets.Store,
		SecretsKey:            f.Secrets.Key,
		Runtimes:              f.Runtimes,
		OIDCIssuer:            f.Identity.Issuer,
		OIDCAudience:          f.Identity.Audience,
		OIDCUsernameClaim:     f.Identity.UsernameClaim,
		OIDCGroupsClaim:       f.Identity.GroupsClaim,
		SigningKey:            f.Identity.SigningKey,
		Vars:                  f.Vars.Values,
		VarsStore:             f.Vars.Store,
	}
	if cfg.External == "" {
		cfg.External = cfg.Listen
		if strings.HasPrefix(cfg.Listen, ":") {
			cfg.External = "127.0.0.1" + cfg.Listen
		}
	}
	cfg.AgentHeartbeat = time.Duration(cfg.AgentHeartbeatSeconds) * time.Second
	// One key protects both stores of secrets: reusing it for minting
	// keeps a dev installation to a single configured secret.
	if cfg.SigningKey == "" {
		cfg.SigningKey = cfg.SecretsKey
	}

	for _, entry := range splitCSV(f.Auth.AdminTokens) {
		tok, ns := splitToken(entry, "*")
		cfg.Tokens = append(cfg.Tokens, Token{Token: tok, Role: "admin", Namespace: ns})
	}
	for _, entry := range splitCSV(f.Auth.RunTokens) {
		tok, ns := splitToken(entry, "default")
		cfg.Tokens = append(cfg.Tokens, Token{Token: tok, Role: "run", Namespace: ns})
	}
	for _, pair := range splitCSV(f.Auth.AgentTokens) {
		agentId, rest, ok := strings.Cut(pair, ":")
		if !ok || agentId == "" || rest == "" {
			return cfg, fmt.Errorf("auth.agent_tokens: %q is not agentId:token[@namespace]", pair)
		}
		tok, ns := splitToken(rest, "default")
		cfg.Tokens = append(cfg.Tokens, Token{Token: tok, Role: "agent", Namespace: ns, AgentId: agentId})
	}
	if len(cfg.Tokens) == 0 {
		return cfg, fmt.Errorf("no tokens configured: nobody could connect")
	}

	for k, v := range f.Secrets.Values {
		cfg.Secrets[k] = v
	}
	if f.Secrets.File != "" {
		raw, err := os.ReadFile(f.Secrets.File)
		if err != nil {
			return cfg, fmt.Errorf("secrets file: %w", err)
		}
		fromFile := map[string]string{}
		if err := yaml.Unmarshal(raw, &fromFile); err != nil {
			return cfg, fmt.Errorf("secrets file: %w", err)
		}
		for k, v := range fromFile {
			cfg.Secrets[k] = v
		}
	}
	return cfg, nil
}

// splitToken parses "token[@namespace]".
func splitToken(entry, defaultNS string) (token, namespace string) {
	if tok, ns, ok := strings.Cut(entry, "@"); ok && ns != "" {
		return tok, ns
	}
	return entry, defaultNS
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
