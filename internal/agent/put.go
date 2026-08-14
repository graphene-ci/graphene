package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/graphene-ci/graphene/sdk/agent"
)

// Put uploads a file from this machine to where it was told.
//
// The bytes never touch the control plane. It handed out a door that opens
// once, and the machine walks through it — sending gigabytes through the
// thing whose job is handing out tasks would make the task-handing stop.
//
// The digest is computed WHILE uploading, from the bytes that actually go.
// Hashing the file separately would describe the file we meant to send, and
// those are not always the same thing.
func Put(ctx context.Context, req agent.PutInput) (agent.PutOutput, error) {
	file, err := os.Open(req.Path)
	if err != nil {
		return agent.PutOutput{}, fmt.Errorf("файл %s не открылся: %w", req.Path, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return agent.PutOutput{}, fmt.Errorf("файл %s не читается: %w", req.Path, err)
	}

	sum := sha256.New()
	body := io.TeeReader(file, sum)

	request, err := http.NewRequestWithContext(ctx, http.MethodPut, req.URL, body)
	if err != nil {
		return agent.PutOutput{}, fmt.Errorf("запрос не собрался: %w", err)
	}

	request.ContentLength = info.Size()

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return agent.PutOutput{}, fmt.Errorf("артефакт не уехал: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusBadRequest {
		return agent.PutOutput{}, fmt.Errorf("хранилище отказало: %s", response.Status) //nolint:err113 // ответ чужой стороны
	}

	// Дочитываем ответ до конца: иначе соединение не переиспользуется, а
	// агент кладёт артефакты пачками.
	_, _ = io.Copy(io.Discard, response.Body)

	return agent.PutOutput{
		Digest: "sha256:" + hex.EncodeToString(sum.Sum(nil)),
		Size:   info.Size(),
	}, nil
}
