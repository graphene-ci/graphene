package config_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/graphene-ci/graphene/internal/app/config"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "graphene.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	return path
}

func TestServingKernel(t *testing.T) {
	t.Parallel()

	path := writeConfig(t, `
data_dir: /srv/graphene
identity: { tenant: acme, name: srv1 }
store: {}
blobs: {}
listen: { tcp: "0.0.0.0:9000" }
tls: { mode: auto, dns_names: [control] }
auth: { bootstrap: { token: { inline: secret } } }
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// Paths derive from data_dir when unset.
	if cfg.Store.Path != "/srv/graphene/store.db" {
		t.Errorf("store path: %s", cfg.Store.Path)
	}

	if cfg.Blobs.Path != "/srv/graphene/blobs" {
		t.Errorf("blobs path: %s", cfg.Blobs.Path)
	}

	if cfg.TLS.Dir != "/srv/graphene/tls" {
		t.Errorf("tls dir: %s", cfg.TLS.Dir)
	}

	if cfg.Listen.UDS != "/srv/graphene/graphene.sock" {
		t.Errorf("uds: %s", cfg.Listen.UDS)
	}

	// Absent sections stay nil — the kernel simply lacks those capabilities.
	if cfg.Link != nil || cfg.Lease != nil {
		t.Errorf("absent sections materialized: link=%v lease=%v", cfg.Link, cfg.Lease)
	}

	// Defaults inside present sections apply.
	if cfg.Store.Backend != "bbolt" || cfg.Log.Level != "info" {
		t.Errorf("defaults not applied: %+v %+v", cfg.Store, cfg.Log)
	}

	token, err := cfg.Auth.Bootstrap.Token.Resolve()
	if err != nil || token != "secret" {
		t.Errorf("token: %q err=%v", token, err)
	}
}

func TestLinkedKernel(t *testing.T) {
	t.Parallel()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("join-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	path := writeConfig(t, `
identity: { tenant: acme, name: k1 }
link:
  mode: dialout
  address: srv1:9000
  token: { file: `+tokenFile+` }
lease: { ttl: 45s }
`)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Store != nil || cfg.Listen != nil {
		t.Errorf("linked kernel got serving sections: store=%v listen=%v", cfg.Store, cfg.Listen)
	}

	token, err := cfg.Link.Token.Resolve()
	if err != nil || token != "join-token" {
		t.Errorf("token file: %q err=%v", token, err)
	}

	// Explicit value wins, sibling default still applies.
	if cfg.Lease.TTL.String() != "45s" || cfg.Lease.RenewInterval.String() != "10s" {
		t.Errorf("lease: %+v", cfg.Lease)
	}
}

func TestValidationRejectsIncoherentCombinations(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		body string
		want error
	}{
		"nothing configured": {
			body: "identity: { tenant: acme, name: x }\n",
			want: config.ErrNothingConfigured,
		},
		"listen without store": {
			body: "listen: { tcp: \"0.0.0.0:1\" }\nlink: { address: a:1, token: { inline: t } }\n",
			want: config.ErrListenWithoutData,
		},
		"tcp without tls": {
			body: "store: {}\nlisten: { tcp: \"0.0.0.0:1\" }\n",
			want: config.ErrTCPWithoutTLS,
		},
		"tls files without paths": {
			body: "store: {}\nlisten: {}\ntls: { mode: files }\n",
			want: config.ErrTLSFilesMissing,
		},
		"link without token": {
			body: "link: { address: a:1 }\n",
			want: config.ErrLinkToken,
		},
		"dialout without address": {
			body: "link: { mode: dialout, token: { inline: t } }\n",
			want: config.ErrLinkAddress,
		},
		"via without relay": {
			body: "link: { mode: via, token: { inline: t } }\n",
			want: config.ErrLinkVia,
		},
		"lease without link": {
			body: "store: {}\nlease: { ttl: 1s }\n",
			want: config.ErrLeaseWithoutLink,
		},
		"auth without listen": {
			body: "store: {}\nauth: { bootstrap: { token: { inline: t } } }\n",
			want: config.ErrAuthWithoutListen,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := config.Load(writeConfig(t, tc.body))
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestEnvOverridesAndMaterializesSections(t *testing.T) {
	t.Setenv("GRAPHENE_IDENTITY_NAME", "from-env")
	t.Setenv("GRAPHENE_LINK_ADDRESS", "srv2:9000")
	t.Setenv("GRAPHENE_LINK_TOKEN_INLINE", "env-token")

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Identity.Name != "from-env" {
		t.Errorf("identity: %+v", cfg.Identity)
	}

	// An env variable under a section path is enough to enable it — this is
	// how an ssh-spawned kernel gets configured without a file.
	if cfg.Link == nil || cfg.Link.Address != "srv2:9000" || cfg.Link.Mode != "dialout" {
		t.Fatalf("link from env: %+v", cfg.Link)
	}

	if cfg.Store != nil {
		t.Errorf("store materialized from nothing: %+v", cfg.Store)
	}
}
