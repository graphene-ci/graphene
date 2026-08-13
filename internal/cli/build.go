package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Ko builds a pipeline's image with ko.
//
// A pipeline is a Go binary and nothing else, so there is no Dockerfile and
// nothing to keep in step with one. ko builds straight from the package and
// prints the reference by digest, which is exactly what a revision records.
type Ko struct {
	// Path to the ko binary. Empty means whatever is on PATH.
	Path string
	// Repo is where the image goes. For local development this is
	// "ko.local", which means the developer's own docker.
	Repo string
	// ImportTo is the k3d cluster the built image is imported into.
	//
	// This is what makes local development work without running a
	// registry: the image never leaves the machine. Empty means the image
	// is somewhere the cluster can already reach, which is what a real
	// installation looks like.
	ImportTo string
	// K3d is the path to the k3d binary, used only when ImportTo is set.
	K3d string
	// Out receives the builder's progress. Building takes a while and
	// silence reads as a hang.
	Out *os.File
}

// Build compiles the directory into an image and reports it by digest.
func (k Ko) Build(ctx context.Context, dir string) (string, error) {
	image, err := k.build(ctx, dir)
	if err != nil {
		return "", err
	}

	if k.ImportTo == "" {
		return image, nil
	}

	if err := k.importImage(ctx, image); err != nil {
		return "", err
	}

	return image, nil
}

func (k Ko) build(ctx context.Context, dir string) (string, error) {
	binary := k.Path
	if binary == "" {
		binary = "ko"
	}

	cmd := exec.CommandContext(ctx, binary, "build", "--bare", "--tags", "", dir)

	cmd.Env = append(os.Environ(), "KO_DOCKER_REPO="+k.Repo)
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

func (k Ko) importImage(ctx context.Context, image string) error {
	binary := k.K3d
	if binary == "" {
		binary = "k3d"
	}

	cmd := exec.CommandContext(ctx, binary, "image", "import", image, "--cluster", k.ImportTo)
	cmd.Stdout = k.Out
	cmd.Stderr = k.Out

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("образ не занесён в кластер %s: %w", k.ImportTo, err)
	}

	return nil
}
