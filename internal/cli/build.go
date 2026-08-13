package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// baseImage is what a pipeline's image is built on top of, pinned by
// digest.
//
// It is set here rather than left to ko because ko's own default is
// cgr.dev/chainguard/static:latest — a `latest`, and one that now answers
// 403 without credentials, so a build would fail on a clean machine for a
// reason that has nothing to do with the pipeline. `.ko.yaml` says the same
// thing for the images this repository builds of itself; the two are kept
// equal on purpose, because a pipeline is built from wherever the user's
// code lives and cannot rely on our file being next to it.
const baseImage = "gcr.io/distroless/static-debian12@sha256:" +
	"1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a"

// Ko builds a pipeline's image with ko.
//
// A pipeline is a Go binary and nothing else, so there is no Dockerfile and
// nothing to keep in step with one. ko builds straight from the package and
// prints the reference by digest, which is exactly what a revision records.
type Ko struct {
	// Path to the ko binary. Empty means whatever is on PATH.
	Path string
	// Repo is where the image goes. It has to be a registry the cluster
	// can also read: a revision records a digest, and a digest only
	// exists for an image that was pushed somewhere.
	Repo string
	// Out receives the builder's progress. Building takes a while and
	// silence reads as a hang.
	Out *os.File
}

// Build compiles the directory into an image and reports it by digest.
func (k Ko) Build(ctx context.Context, dir string) (string, error) {
	return k.build(ctx, dir)
}

func (k Ko) build(ctx context.Context, dir string) (string, error) {
	binary := k.Path
	if binary == "" {
		binary = "ko"
	}

	// Без --tags: пустой список тегов заставляет ko напечатать
	// «Published» и не отправить ничего — молчаливый холостой ход,
	// который стоил часа. Тег latest, который ko поставит по умолчанию,
	// нас не касается: ссылаемся мы дайджестом.
	cmd := exec.CommandContext(ctx, binary, "build", "--bare", dir)

	cmd.Env = append(os.Environ(),
		"KO_DOCKER_REPO="+k.Repo,
		"KO_DEFAULTBASEIMAGE="+baseImage,
	)
	cmd.Stderr = k.Out

	var out bytes.Buffer

	cmd.Stdout = &out

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("ko не собрал %s: %w", dir, err)
	}

	image := strings.TrimSpace(out.String())
	if !strings.Contains(image, "@sha256:") {
		return "", fmt.Errorf("%w: ko вернул %q", ErrNotDigest, image)
	}

	return image, nil
}
