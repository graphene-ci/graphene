// Package config holds the server's startup configuration: a YAML file
// (optional) overlaid by environment variables — the whole config is
// env-expressible for container deployments. Powered by xconf's
// structconf: `mapstructure` names, `default` values, env names derived
// from the dotted path under the GRAPHENE prefix
// (server.grpc -> GRAPHENE_SERVER_GRPC).
package config

import (
	"fmt"
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
		// Grpc is the single gRPC door: agent sessions + Temporal proxy.
		Grpc string `mapstructure:"grpc" default:":7233"`
		// Http is the HTTP door: runs, blobs, secrets, registry proxy.
		HTTP string `mapstructure:"http" default:":7280"`
		// ExternalGrpc is what workers and agents dial; defaults to Grpc.
		ExternalGRPC string `mapstructure:"external_grpc"`
		// ExternalHttp is the HTTP base workers use; defaults from Http.
		ExternalHTTP string `mapstructure:"external_http"`
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
		Dir string `mapstructure:"dir" default:"/var/lib/graphene-server/blobs"`
	} `mapstructure:"blobs"`

	Auth struct {
		// AdminTokens / RunTokens are comma-separated token lists.
		AdminTokens string `mapstructure:"admin_tokens"`
		RunTokens   string `mapstructure:"run_tokens"`
		// AgentTokens is comma-separated "agentId:token" pairs.
		AgentTokens string `mapstructure:"agent_tokens"`
	} `mapstructure:"auth"`

	Secrets struct {
		// File is a YAML file of name -> value; Values inlines them
		// (file wins on collision). Only names ever leave the server.
		File   string            `mapstructure:"file"`
		Values map[string]string `mapstructure:"values"`
	} `mapstructure:"secrets"`

	Intervals struct {
		AgentHeartbeatSeconds int `mapstructure:"agent_heartbeat_seconds" default:"15"`
		SweepSeconds          int `mapstructure:"sweep_seconds" default:"30"`
		ReapSeconds           int `mapstructure:"reap_seconds" default:"10"`
	} `mapstructure:"intervals"`
}

// Config is the resolved runtime configuration the server composes from.
type Config struct {
	ListenGRPC   string
	ListenHTTP   string
	ExternalGRPC string
	ExternalHTTP string

	TemporalHostPort  string
	TemporalNamespace string

	Tokens  []Token
	Secrets map[string]string

	BlobDir          string
	RegistryUpstream string

	AgentHeartbeat        time.Duration
	AgentHeartbeatSeconds int
	SweepSeconds          int
	ReapSeconds           int
}

// Token is one credential with its scope.
type Token struct {
	Token string
	// Role is "agent", "run", or "admin".
	Role string
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
		ListenGRPC:            f.Server.Grpc,
		ListenHTTP:            f.Server.HTTP,
		ExternalGRPC:          f.Server.ExternalGRPC,
		ExternalHTTP:          f.Server.ExternalHTTP,
		TemporalHostPort:      f.Temporal.HostPort,
		TemporalNamespace:     f.Temporal.Namespace,
		BlobDir:               f.Blobs.Dir,
		RegistryUpstream:      f.Registry.Upstream,
		AgentHeartbeatSeconds: f.Intervals.AgentHeartbeatSeconds,
		SweepSeconds:          f.Intervals.SweepSeconds,
		ReapSeconds:           f.Intervals.ReapSeconds,
		Secrets:               map[string]string{},
	}
	if cfg.ExternalGRPC == "" {
		cfg.ExternalGRPC = cfg.ListenGRPC
	}
	if cfg.ExternalHTTP == "" {
		cfg.ExternalHTTP = "http://" + strings.TrimPrefix(cfg.ListenHTTP, ":")
		if strings.HasPrefix(cfg.ListenHTTP, ":") {
			cfg.ExternalHTTP = "http://127.0.0.1" + cfg.ListenHTTP
		}
	}
	cfg.AgentHeartbeat = time.Duration(cfg.AgentHeartbeatSeconds) * time.Second

	for _, tok := range splitCSV(f.Auth.AdminTokens) {
		cfg.Tokens = append(cfg.Tokens, Token{Token: tok, Role: "admin"})
	}
	for _, tok := range splitCSV(f.Auth.RunTokens) {
		cfg.Tokens = append(cfg.Tokens, Token{Token: tok, Role: "run"})
	}
	for _, pair := range splitCSV(f.Auth.AgentTokens) {
		agentId, tok, ok := strings.Cut(pair, ":")
		if !ok || agentId == "" || tok == "" {
			return cfg, fmt.Errorf("auth.agent_tokens: %q is not agentId:token", pair)
		}
		cfg.Tokens = append(cfg.Tokens, Token{Token: tok, Role: "agent", AgentId: agentId})
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

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
