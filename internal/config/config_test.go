package config

import "testing"

func TestDoorTransport(t *testing.T) {
	for _, tc := range []struct {
		name        string
		publicTLS   bool
		internal    string
		internalTLS bool
		address     string
		insecure    bool
	}{
		{"plaintext", false, "", false, "public:7233", true},
		{"public TLS", true, "", false, "public:7233", false},
		{"internal plaintext", true, "internal:7233", false, "internal:7233", true},
		{"internal TLS", false, "internal:7233", true, "internal:7233", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var f File
			f.Server.External = "public:7233"
			f.Server.ExternalTLS = tc.publicTLS
			f.Server.ExternalInternal = tc.internal
			f.Server.ExternalInternalTLS = tc.internalTLS
			f.Auth.AdminTokens = "test-token"
			c, err := Resolve(f)
			if err != nil {
				t.Fatal(err)
			}
			addr, insecure := c.ManagedDoor()
			if addr != tc.address || insecure != tc.insecure || c.ExternalTLS != tc.publicTLS {
				t.Fatalf("door=%s insecure=%t config=%+v", addr, insecure, c.ExternalTLS)
			}
		})
	}
}

func TestDoorTLSFromEnvironment(t *testing.T) {
	t.Setenv(EnvConfigPath, "")
	t.Setenv("GRAPHENE_AUTH_ADMIN_TOKENS", "test-token")
	t.Setenv("GRAPHENE_SERVER_EXTERNAL_TLS", "true")
	t.Setenv("GRAPHENE_SERVER_EXTERNAL_INTERNAL_TLS", "true")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !c.ExternalTLS || !c.ExternalInternalTLS {
		t.Fatal("TLS env settings lost")
	}
}
