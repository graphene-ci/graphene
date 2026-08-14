package operator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

// ErrNoHostKey means nobody said what the machine's key should be.
var ErrNoHostKey = errors.New("не указан ключ хоста: возьмите его через ssh-keyscan")

// hostKey parses the expected key of the far side.
func hostKey(line string) (ssh.PublicKey, error) {
	if strings.TrimSpace(line) == "" {
		return nil, ErrNoHostKey
	}

	// Строка known_hosts начинается с адреса, authorized_keys — сразу с
	// типа. Принимаем обе: человек берёт её ssh-keyscan-ом и не обязан
	// помнить, какого она вида.
	fields := strings.Fields(line)
	if len(fields) > 2 && !strings.HasPrefix(fields[0], "ssh-") && !strings.HasPrefix(fields[0], "ecdsa-") {
		line = strings.Join(fields[1:], " ")
	}

	key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line)) //nolint:dogsled // так устроена подпись в x/crypto/ssh
	if err != nil {
		return nil, fmt.Errorf("ключ хоста не разобрался: %w", err)
	}

	return key, nil
}

// ErrInstallTimeout means the machine answered but the script never
// finished.
var ErrInstallTimeout = errors.New("установка не уложилась в срок")

// sshTimeouts bound a trip to a machine that may simply be off.
const (
	sshDial = 20 * time.Second
	sshRun  = 10 * time.Minute
)

// SSH installs the agent over ssh.
//
// The machine's host key is checked, and there is no way to ask it not to.
// Trust on first use is what a person at a terminal does; this is a control
// plane opening a root shell on the far side and feeding it a script with
// an installation token in it. Whoever answered at that address would get
// both.
func SSH(ctx context.Context, req InstallRequest) error {
	signer, err := ssh.ParsePrivateKey(req.Key)
	if err != nil {
		return fmt.Errorf("ключ не разобрался: %w", err)
	}

	host, err := hostKey(req.HostKey)
	if err != nil {
		return err
	}

	config := &ssh.ClientConfig{
		User:            req.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.FixedHostKey(host),
		// Просим именно тот алгоритм, ключ которого нам дали. Иначе
		// сервер предложит первый по своему списку, тот не совпадёт с
		// нашим единственным, и отказ будет выглядеть как подмена, хотя
		// это всего лишь разговор о разных ключах одной машины.
		HostKeyAlgorithms: []string{host.Type()},
		Timeout:           sshDial,
	}

	address := req.Address
	if !strings.Contains(address, ":") {
		address = net.JoinHostPort(address, "22")
	}

	dialer := net.Dialer{Timeout: sshDial}

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("до %s не достучаться: %w", address, err)
	}

	client, err := handshake(conn, address, config)
	if err != nil {
		return err
	}
	defer client.Close()

	return run(ctx, client, req.Script)
}

func handshake(conn net.Conn, address string, config *ssh.ClientConfig) (*ssh.Client, error) {
	meeting, channels, requests, err := ssh.NewClientConn(conn, address, config)
	if err != nil {
		_ = conn.Close()

		return nil, fmt.Errorf("вход на %s не удался: %w", address, err)
	}

	return ssh.NewClient(meeting, channels, requests), nil
}

// run feeds the script to a shell on the far side.
//
// Через stdin, а не файлом: тогда на машине не остаётся ничего, что надо
// было бы потом убирать, и скрипт не лежит там, где его прочитает кто
// угодно — а в нём токен установки.
func run(ctx context.Context, client *ssh.Client, script string) error {
	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("сессия не открылась: %w", err)
	}
	defer session.Close()

	var out bytes.Buffer

	session.Stdout = &out
	session.Stderr = &out
	session.Stdin = strings.NewReader(script)

	done := make(chan error, 1)

	go func() { done <- session.Run("sudo -n sh -s") }()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("установка не прошла: %w\n%s", err, tail(out.String()))
		}

		return nil
	case <-time.After(sshRun):
		_ = session.Signal(ssh.SIGKILL)

		return fmt.Errorf("%w (%s):\n%s", ErrInstallTimeout, sshRun, tail(out.String()))
	case <-ctx.Done():
		_ = session.Signal(ssh.SIGKILL)

		return fmt.Errorf("установка прервана: %w", ctx.Err())
	}
}

// tail keeps the end of the output: when something fails, what says why is
// at the end.
func tail(text string) string {
	const limit = 2000
	if len(text) <= limit {
		return text
	}

	return text[len(text)-limit:]
}
