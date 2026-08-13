//go:build e2e

package e2e_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/api/v1"
	"github.com/graphene-ci/graphene/internal/cli"
	"github.com/graphene-ci/graphene/pkg/agent"
)

// Куда машина ходит за агентом и за работой.
//
// Всё через 127.0.0.1, и контейнер запускается с сетью хоста: порты
// окружения намеренно прибиты к петле, а машина в этой проверке живёт на
// той же машине. С настоящей ВМ (M3) сюда встанет адрес, который до неё
// достаёт, и это единственное, что поменяется.
const (
	control  = "http://127.0.0.1:18080"
	temporal = "127.0.0.1:7233"
)

func pushExec(ctx context.Context, t *testing.T, kube client.Client) {
	t.Helper()

	_, err := cli.Push(ctx, cli.PushRequest{
		Kube:      kube,
		Builder:   cli.Ko{Path: tool("ko"), Repo: registry, Out: os.Stderr},
		Namespace: namespace,
		Pipeline:  "exec",
		Dir:       "../../examples/exec",
	})
	if err != nil {
		t.Fatalf("push не прошёл: %v", err)
	}
}

// machine starts a container and puts the agent on it with the very script
// a cloud would run. The container is a plain alpine — nothing of ours is
// baked into it, which is the point: if the script needs something, it has
// to bring it.
func machine(ctx context.Context, t *testing.T, installation string) {
	t.Helper()

	script := agent.Install{
		Control:   control,
		Address:   temporal,
		Namespace: "graphene",
		Records:   namespace,
		Machine:   installation,
		Token:     "проверка",
	}.Script()

	dir := t.TempDir()

	path := filepath.Join(dir, "install.sh")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("скрипт не записался: %v", err)
	}

	name := "graphene-e2e-" + installation

	out, err := exec.CommandContext(ctx, "docker", "run", "--detach", "--rm",
		"--name", name, "--network", "host",
		"--volume", path+":/install.sh:ro",
		"alpine:3.21",
		"sh", "-c", "apk add --no-cache curl >/dev/null 2>&1 && sh /install.sh",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("машина не поднялась: %v\n%s", err, out)
	}

	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "--force", name).Run()
	})
}

// Шаг доезжает до машины и возвращает код возврата и вывод.
//
// Машину поднимаем ПОСЛЕ прогона: очередь и есть ожидание, и шаг,
// поставленный в очередь несуществующего агента, обязан дождаться его в
// ней. Если бы это было не так, порядок здесь имел бы значение.
func TestStepReachesTheMachine(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), patience)
	defer cancel()

	kube := connect(t)
	pushExec(ctx, t, kube)

	run, err := cli.Start(ctx, cli.StartRequest{
		Kube:      kube,
		Namespace: namespace,
		Pipeline:  "exec",
		Params:    []byte(`{"say":"машина ответила"}`),
	})
	if err != nil {
		t.Fatalf("прогон не создался: %v", err)
	}

	// Дать оператору поднять воркфлоу и поставить шаг в очередь, которой
	// пока никто не читает.
	time.Sleep(5 * time.Second)

	machine(ctx, t, run.Name+"-node-0")

	if phase := await(ctx, t, kube, run.Name); phase != v1.RunSucceeded {
		t.Fatalf("прогон завершился как %s", phase)
	}

	// Машина завела себя сама, как kubelet заводит Node.
	var found v1.Machine

	key := client.ObjectKey{Namespace: namespace, Name: run.Name + "-node-0"}
	if err := kube.Get(ctx, key, &found); err != nil {
		t.Fatalf("машина себя не записала: %v", err)
	}

	if found.Status.Facts["os"] != "linux" {
		t.Fatalf("факты не доехали: %v", found.Status.Facts)
	}

	if !strings.HasSuffix(found.Status.Queue, run.Name+"-node-0") {
		t.Fatalf("очередь установки не та: %q", found.Status.Queue)
	}
}
