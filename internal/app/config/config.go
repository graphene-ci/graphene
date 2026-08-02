package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/gopherex/xconf/pkg/structconf"

	"github.com/graphene-ci/graphene/internal/app/secret"
)

// Config is the whole kernel configuration. Capabilities follow presence:
// no roles, no modes.
type Config struct {
	DataDir  string   `default:"/var/lib/graphen" mapstructure:"data_dir" validate:"required"`
	Identity Identity `mapstructure:"identity"`
	Log      Log      `mapstructure:"log"`

	// Store makes this kernel hold the truth.
	Store *Store `mapstructure:"store"`
	// Blobs makes it hold content bytes.
	Blobs *Blobs `mapstructure:"blobs"`
	// Listen makes it serve the API.
	Listen *Listen `mapstructure:"listen"`
	// TLS configures the served TCP endpoint.
	TLS *TLS `mapstructure:"tls"`
	// Auth carries the bootstrap credential; everything else lives as
	// Role/Identity resources.
	Auth *Auth `mapstructure:"auth"`
	// Link connects this kernel to another one.
	Link *Link `mapstructure:"link"`
	// Lease configures the liveness heartbeat sent over the link.
	Lease *Lease `mapstructure:"lease"`
}

// Identity is who this kernel is: the tenant it belongs to and its own
// name — the path segments of its Kernel and KernelLease resources.
type Identity struct {
	Tenant string `default:"default" mapstructure:"tenant" validate:"required"`
	Name   string `default:"local"   mapstructure:"name"   validate:"required"`
}

// Log configures the structured logger.
type Log struct {
	Level  string `default:"info" mapstructure:"level"  validate:"oneof=debug info warn error"`
	Format string `default:"text" mapstructure:"format" validate:"oneof=text json"`
}

// Store selects the truth backend.
type Store struct {
	Backend string `default:"bbolt" mapstructure:"backend" validate:"oneof=bbolt"`
	// Path defaults to <data_dir>/store.db.
	Path string `mapstructure:"path"`
}

// Blobs selects the content backend.
type Blobs struct {
	Backend string `default:"fs" mapstructure:"backend" validate:"oneof=fs"`
	// Path defaults to <data_dir>/blobs.
	Path string `mapstructure:"path"`
}

// Listen configures the served endpoints. An empty address disables that
// endpoint; the unix socket defaults to <data_dir>/graphen.sock.
type Listen struct {
	TCP string `mapstructure:"tcp"`
	UDS string `mapstructure:"uds"`
	// DisableUDS turns off the socket that would otherwise be defaulted.
	DisableUDS bool `mapstructure:"disable_uds"`
}

// TLS configures the certificate of the TCP endpoint.
//
// mode=auto mints a self-signed CA and server certificate on first start
// under <dir>; clients pin the CA (graphen kernel ca). mode=files uses a
// certificate the operator provides.
type TLS struct {
	Mode string `default:"auto" mapstructure:"mode" validate:"oneof=auto files"`
	// Dir defaults to <data_dir>/tls.
	Dir      string   `mapstructure:"dir"`
	DNSNames []string `mapstructure:"dns_names"`
	CertFile string   `mapstructure:"cert_file"`
	KeyFile  string   `mapstructure:"key_file"`
}

// Auth holds the bootstrap credential: the token that creates the first
// Role and Identity resources. Everything after that is administered
// through the API.
type Auth struct {
	Bootstrap BootstrapAuth `mapstructure:"bootstrap"`
}

// BootstrapAuth is the bootstrap identity and its token.
type BootstrapAuth struct {
	Name  string       `default:"bootstrap"  mapstructure:"name" validate:"required"`
	Token secret.Value `mapstructure:"token"`
}

// Link connects this kernel to another one.
//
//	dialout — dial the address directly;
//	stdio   — this process's own stdin/stdout (an ssh-spawned kernel);
//	via     — dial through a relay chain.
type Link struct {
	Mode    string       `default:"dialout"      mapstructure:"mode" validate:"oneof=dialout stdio via"`
	Address string       `mapstructure:"address"`
	Token   secret.Value `mapstructure:"token"`
	CAFile  string       `mapstructure:"ca_file"`
	// ServerName overrides the name verified in the peer's certificate;
	// needed when the address is an IP the certificate does not carry.
	ServerName string `mapstructure:"server_name"`
	Via        *Relay `mapstructure:"via"`
}

// Relay is the first hop of a relay chain.
type Relay struct {
	Address string       `mapstructure:"address" validate:"required"`
	Token   secret.Value `mapstructure:"token"`
}

// Lease configures the liveness heartbeat: the kernel renews its lease
// every RenewInterval, and the far side declares it gone after TTL.
type Lease struct {
	TTL           time.Duration `default:"30s" mapstructure:"ttl"            validate:"required"`
	RenewInterval time.Duration `default:"10s" mapstructure:"renew_interval" validate:"required"`
}

// Load reads the configuration: tag defaults, then the file (optional when
// path is empty), then environment variables prefixed with GRAPHEN_.
func Load(path string) (*Config, error) {
	opts := []structconf.Option{structconf.WithEnvPrefix("GRAPHEN")}
	if path != "" {
		opts = append(opts, structconf.WithFile(path))
	}

	cfg, err := structconf.Load[Config](opts...)
	if err != nil {
		return nil, fmt.Errorf("config: load: %w", err)
	}

	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// applyDefaults fills the paths derived from DataDir.
func (c *Config) applyDefaults() {
	if c.Store != nil && c.Store.Path == "" {
		c.Store.Path = filepath.Join(c.DataDir, "store.db")
	}

	if c.Blobs != nil && c.Blobs.Path == "" {
		c.Blobs.Path = filepath.Join(c.DataDir, "blobs")
	}

	if c.TLS != nil && c.TLS.Dir == "" {
		c.TLS.Dir = filepath.Join(c.DataDir, "tls")
	}

	if c.Listen != nil && c.Listen.UDS == "" && !c.Listen.DisableUDS {
		c.Listen.UDS = filepath.Join(c.DataDir, "graphen.sock")
	}
}

// Errors reported by Validate for combinations no tag can express.
var (
	ErrNothingConfigured = errors.New("config: kernel does nothing: configure store+listen, link, or both")
	ErrListenWithoutData = errors.New("config: listen requires store (nothing to serve otherwise)")
	ErrTCPWithoutTLS     = errors.New("config: listen.tcp requires tls")
	ErrTLSFilesMissing   = errors.New("config: tls mode=files requires cert_file and key_file")
	ErrLinkAddress       = errors.New("config: link mode dialout requires address")
	ErrLinkVia           = errors.New("config: link mode via requires the via section")
	ErrLinkToken         = errors.New("config: link requires a token")
	ErrLeaseWithoutLink  = errors.New("config: lease requires link")
	ErrAuthWithoutListen = errors.New("config: auth requires listen (nobody would present the token)")
)

// Validate checks the cross-section combinations: which capabilities
// require which others.
func (c *Config) Validate() error {
	if c.Store == nil && c.Link == nil {
		return ErrNothingConfigured
	}

	if err := c.validateServing(); err != nil {
		return err
	}

	return c.validateLink()
}

func (c *Config) validateServing() error {
	if c.Listen != nil && c.Store == nil {
		return ErrListenWithoutData
	}

	if c.Listen != nil && c.Listen.TCP != "" && c.TLS == nil {
		return ErrTCPWithoutTLS
	}

	if c.TLS != nil && c.TLS.Mode == "files" && (c.TLS.CertFile == "" || c.TLS.KeyFile == "") {
		return ErrTLSFilesMissing
	}

	if c.Auth != nil && c.Listen == nil {
		return ErrAuthWithoutListen
	}

	return nil
}

func (c *Config) validateLink() error {
	if c.Lease != nil && c.Link == nil {
		return ErrLeaseWithoutLink
	}

	if c.Link == nil {
		return nil
	}

	switch c.Link.Mode {
	case "dialout":
		if c.Link.Address == "" {
			return ErrLinkAddress
		}
	case "via":
		if c.Link.Via == nil {
			return ErrLinkVia
		}
	}

	if c.Link.Token.IsZero() {
		return ErrLinkToken
	}

	return nil
}
