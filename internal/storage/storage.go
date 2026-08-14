// Package storage is where artifacts live. It is deliberately thin: we do
// not own the bytes, we own the way to reach them and the rule for how
// long they stay.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// presignLife is how long a place to upload stays open.
//
// Short on purpose. It has to outlive one upload of one file over whatever
// link the machine has, and nothing more: a link that lives for hours is a
// key somebody can find in a log.
const presignLife = 2 * time.Hour

// ErrNotConfigured means nobody told us where artifacts go.
var ErrNotConfigured = errors.New("хранилище артефактов не настроено")

// Storage is where artifacts go.
//
// Endpoint is how WE reach it; Public is how a MACHINE reaches it. They
// differ whenever the control plane is inside something and the machines
// are outside it, which is the normal case and not the exception — a
// service name means nothing to a virtual machine in a cloud.
type Storage struct {
	Endpoint string
	Public   string
	Bucket   string
	Access   string
	Secret   string
	Secure   bool
}

// FromEnv reads where artifacts go.
func FromEnv(get func(string) string) Storage {
	public := get("GRAPHENE_STORAGE_PUBLIC")
	if public == "" {
		public = get("GRAPHENE_STORAGE_ENDPOINT")
	}

	return Storage{
		Endpoint: get("GRAPHENE_STORAGE_ENDPOINT"),
		Public:   public,
		Bucket:   get("GRAPHENE_STORAGE_BUCKET"),
		Access:   get("GRAPHENE_STORAGE_ACCESS_KEY"),
		Secret:   get("GRAPHENE_STORAGE_SECRET_KEY"),
		Secure:   get("GRAPHENE_STORAGE_SECURE") == "true",
	}
}

// Configured answers whether there is anywhere to put anything.
func (s Storage) Configured() bool {
	return s.Endpoint != "" && s.Public != "" && s.Bucket != ""
}

// Presign issues one short-lived place to put one object, and says how a
// machine should reach it.
func (s Storage) Presign(ctx context.Context, key string) (string, error) {
	if !s.Configured() {
		return "", ErrNotConfigured
	}

	// Два клиента, и это не дубль. Хост входит в подпись, поэтому
	// подписывать надо ТЕМ адресом, по которому пойдёт машина, — а
	// разговаривать с хранилищем самим по тому, по которому дотянемся мы.
	// Один клиент на оба дела означал бы, что либо мы не достучались,
	// либо машина принесла подпись не на тот хост.
	talk, err := s.client(s.Endpoint)
	if err != nil {
		return "", err
	}

	sign, err := s.client(s.Public)
	if err != nil {
		return "", err
	}

	// Ведро заводим здесь же, если его нет. Отдельный шаг установки был
	// бы ещё одним местом, где что-то могло не случиться, а спросить у
	// хранилища дёшево и делается раз.
	if err := ensureBucket(ctx, talk, s.Bucket); err != nil {
		return "", err
	}

	link, err := sign.PresignedPutObject(ctx, s.Bucket, key, presignLife)
	if err != nil {
		return "", fmt.Errorf("ссылка не выдалась: %w", err)
	}

	return link.String(), nil
}

// Remove takes the bytes away. Removing what is not there is not an error:
// that is the normal answer when the sweep runs twice.
func (s Storage) Remove(ctx context.Context, key string) error {
	if !s.Configured() {
		return ErrNotConfigured
	}

	talk, err := s.client(s.Endpoint)
	if err != nil {
		return err
	}

	if err := talk.RemoveObject(ctx, s.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("байты %s не убрались: %w", key, err)
	}

	return nil
}

// client dials the storage at the given address.
func (s Storage) client(address string) (*minio.Client, error) {
	made, err := minio.New(address, &minio.Options{
		Creds:  credentials.NewStaticV4(s.Access, s.Secret, ""),
		Secure: s.Secure,
	})
	if err != nil {
		return nil, fmt.Errorf("клиент хранилища %s не собрался: %w", address, err)
	}

	return made, nil
}

// ensureBucket makes the bucket if it is not there.
func ensureBucket(ctx context.Context, client *minio.Client, bucket string) error {
	there, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("хранилище не отвечает: %w", err)
	}

	if there {
		return nil
	}

	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		return fmt.Errorf("ведро %s не завелось: %w", bucket, err)
	}

	return nil
}
