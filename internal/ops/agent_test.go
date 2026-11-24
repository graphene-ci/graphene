package ops

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gopherex/xlog"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/crypto/ssh"

	"github.com/graphene-ci/graphene/internal/agents"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

type keyStore map[id.SecretId]string

func (k keyStore) Get(name id.SecretId) (string, error) {
	v, ok := k[name]
	if !ok {
		return "", errors.New("no secret " + string(name))
	}
	return v, nil
}

// sshMachine is an in-process ssh server: it accepts one client key,
// records the command and the stdin of each exec, and answers with a fixed
// output and exit status.
type sshMachine struct {
	addr    string
	hostKey string
	mu      sync.Mutex
	execs   []string
	stdins  []string
	output  string
	status  uint32
}

func newSSHMachine(t *testing.T, client ssh.PublicKey, output string, status uint32) *sshMachine {
	t.Helper()
	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostSigner, err := ssh.NewSignerFromKey(hostPriv)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &ssh.ServerConfig{PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if string(key.Marshal()) != string(client.Marshal()) {
			return nil, errors.New("unknown key")
		}
		return &ssh.Permissions{}, nil
	}}
	cfg.AddHostKey(hostSigner)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := &sshMachine{addr: ln.Addr().String(), hostKey: string(ssh.MarshalAuthorizedKey(hostSigner.PublicKey())), output: output, status: status}
	done := make(chan struct{})
	t.Cleanup(func() { _ = ln.Close(); <-done })
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		defer wg.Wait()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() { m.serve(conn, cfg) })
		}
	}()
	return m
}

func (m *sshMachine) serve(raw net.Conn, cfg *ssh.ServerConfig) {
	_, chans, reqs, err := ssh.NewServerConn(raw, cfg)
	if err != nil {
		_ = raw.Close()
		return
	}
	go ssh.DiscardRequests(reqs)
	for nc := range chans {
		ch, requests, err := nc.Accept()
		if err != nil {
			continue
		}
		for req := range requests {
			if req.Type != "exec" {
				_ = req.Reply(false, nil)
				continue
			}
			var payload struct{ Command string }
			_ = ssh.Unmarshal(req.Payload, &payload)
			_ = req.Reply(true, nil)
			stdin, _ := io.ReadAll(ch)
			m.mu.Lock()
			m.execs = append(m.execs, payload.Command)
			m.stdins = append(m.stdins, string(stdin))
			m.mu.Unlock()
			_, _ = ch.Write([]byte(m.output))
			_, _ = ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{m.status}))
			_ = ch.Close()
		}
	}
}

func clientKey(t *testing.T) (string, ssh.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(block)), sshPub
}

// An offline agent is uninstalled over the record's ssh: the pinned host
// key, the script on stdin (never argv), and the runner that removes its
// temp file. The machine's answer decides.
func TestDecommissionOverSSH(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		output string
		status uint32
		fails  bool
	}{
		{name: "uninstalled", output: "GRAPHENE_DECOMMISSIONED\n"},
		{name: "another agent owns the machine", output: "GRAPHENE_OTHER_AGENT agent=other\n"},
		{name: "script failed", output: "permission denied\n", status: 1, fails: true},
		{name: "script cut short", output: "partial\n", fails: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			pemKey, pub := clientKey(t)
			machine := newSSHMachine(t, pub, c.output, c.status)
			o := NewAgentOps("test", agents.New(time.Second, xlog.New(xlog.NopCore{})), keyStore{"key": pemKey}, nil)
			install := &pipeline.SSHInstall{Address: machine.addr, User: "root", KeyRef: ref.SecretRef{Name: "key"}, HostKey: machine.hostKey}
			err := o.Decommission(context.Background(), "manual-1", install)
			if (err != nil) != c.fails {
				t.Fatalf("err = %v, want fails=%v", err, c.fails)
			}
			machine.mu.Lock()
			defer machine.mu.Unlock()
			if len(machine.execs) != 1 || !strings.Contains(machine.execs[0], `trap "rm -f \"$s\"" EXIT`) {
				t.Fatalf("execs %v", machine.execs)
			}
			if machine.stdins[0] != UninstallScript("manual-1") {
				t.Fatal("the uninstall script must travel on stdin")
			}
		})
	}
}

// A wrong host key is refused before anything runs.
func TestDecommissionPinsTheHostKey(t *testing.T) {
	t.Parallel()
	pemKey, pub := clientKey(t)
	machine := newSSHMachine(t, pub, "GRAPHENE_DECOMMISSIONED\n", 0)
	other := newSSHMachine(t, pub, "", 0)
	o := NewAgentOps("test", agents.New(time.Second, xlog.New(xlog.NopCore{})), keyStore{"key": pemKey}, nil)
	install := &pipeline.SSHInstall{Address: machine.addr, User: "root", KeyRef: ref.SecretRef{Name: "key"}, HostKey: other.hostKey}
	err := o.Decommission(context.Background(), "manual-1", install)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != pipeline.CodeHostKeyMismatch || !app.NonRetryable() {
		t.Fatalf("err = %v, want a final %s", err, pipeline.CodeHostKeyMismatch)
	}
	if len(machine.execs) != 0 {
		t.Fatal("nothing may run on a machine whose key does not match")
	}
}

// Trust on first use reads the key without logging in or running
// anything; ssh without a pinned key refuses instead of trusting.
func TestHostKeyFirstUse(t *testing.T) {
	t.Parallel()
	pemKey, pub := clientKey(t)
	machine := newSSHMachine(t, pub, "GRAPHENE_DECOMMISSIONED\n", 0)
	o := NewAgentOps("test", agents.New(time.Second, xlog.New(xlog.NopCore{})), keyStore{"key": pemKey}, nil)
	key, err := o.HostKey(context.Background(), machine.addr)
	if err != nil || key != strings.TrimSpace(machine.hostKey) {
		t.Fatalf("host key %q err %v, want %q", key, err, machine.hostKey)
	}
	install := &pipeline.SSHInstall{Address: machine.addr, User: "root", KeyRef: ref.SecretRef{Name: "key"}}
	err = o.Decommission(context.Background(), "manual-1", install)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != pipeline.CodeHostKeyMismatch {
		t.Fatalf("err = %v, want a refusal without a pinned key", err)
	}
	install.HostKey = key
	if err := o.Decommission(context.Background(), "manual-1", install); err != nil {
		t.Fatalf("the pinned key must verify: %v", err)
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if len(machine.execs) != 1 {
		t.Fatalf("execs %v: reading the key must run nothing", machine.execs)
	}
}

// Offline with no ssh: nothing to reach the machine with.
func TestDecommissionWithoutAWay(t *testing.T) {
	t.Parallel()
	o := NewAgentOps("test", agents.New(time.Second, xlog.New(xlog.NopCore{})), keyStore{}, nil)
	if err := o.Decommission(context.Background(), "manual-1", nil); err == nil {
		t.Fatal("an offline agent without ssh cannot be decommissioned")
	}
}

func TestUninstallScriptNamesItsAgent(t *testing.T) {
	t.Parallel()
	s := UninstallScript("db-1")
	if !strings.Contains(s, `"$existing" != "db-1"`) || strings.Contains(s, "%AGENT_ID%") {
		t.Fatalf("script does not guard its own agent id:\n%s", s)
	}
}

func BenchmarkUninstallScript(b *testing.B) {
	for b.Loop() {
		UninstallScript("db-1")
	}
}
