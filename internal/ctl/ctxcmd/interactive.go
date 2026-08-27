package ctxcmd

// The interactive half of login: on a terminal, only the MISSING
// pieces are asked — the token always through hidden input, plaintext
// only after an explicit yes. Off a terminal nothing ever prompts.

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// promptLine asks one line on stderr; an empty answer takes the
// default.
func promptLine(label, def string) (string, error) {
	if def != "" {
		fmt.Fprintf(os.Stderr, "%s [%s]: ", label, def)
	} else {
		fmt.Fprintf(os.Stderr, "%s: ", label)
	}
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return def, nil
	}
	return line, nil
}

// promptSecret reads without echo — the token never lands in the
// terminal or the shell history.
func promptSecret(label string) (string, error) {
	fmt.Fprintf(os.Stderr, "%s: ", label)
	raw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(raw)), nil
}

// promptYes asks a y/N question; only an explicit y answers true.
func promptYes(label string) bool {
	fmt.Fprintf(os.Stderr, "%s [y/N]: ", label)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true
	}
	return false
}

// handshake dials the candidate context and asks WhoAmI — the ONE
// identity door every kind of principal answers through.
func handshake(cmd *cobra.Command, cc cliconfig.Context) (*managementv1.WhoAmIResponse, error) {
	d, err := cmdutil.DialContext(cc)
	if err != nil {
		return nil, err
	}
	who, err := d.Rbac.WhoAmI(cmd.Context(), connect.NewRequest(&managementv1.WhoAmIRequest{}))
	if err != nil {
		return nil, err
	}
	return who.Msg, nil
}

// looksLikePlaintextDoor recognizes the handshake failures a TLS
// client gets from an h2c (dev) door.
func looksLikePlaintextDoor(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "HTTP response to HTTPS client") ||
		strings.Contains(msg, "tls:") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "EOF")
}
