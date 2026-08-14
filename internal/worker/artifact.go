package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// presignLife is how long a place to upload stays open.
//
// Short on purpose. It has to outlive one upload of one file over whatever
// link the machine has, and nothing more: a link that lives for hours is a
// key somebody can find in a log.
const presignLife = 2 * time.Hour

// ErrNoStorage means nobody told us where artifacts go.
var ErrNoStorage = errors.New("хранилище артефактов не настроено")

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

// Presign issues one short-lived place to put one artifact.
func (a *Applier) Presign(ctx context.Context, req agent.PresignInput) (agent.PresignOutput, error) {
	if a.storage.Public == "" || a.storage.Endpoint == "" || a.storage.Bucket == "" {
		return agent.PresignOutput{}, ErrNoStorage
	}

	// Два клиента, и это не дубль. Хост входит в подпись, поэтому
	// подписывать надо ТЕМ адресом, по которому пойдёт машина, — а
	// разговаривать с хранилищем самим по тому, по которому дотянемся мы.
	// Один клиент на оба дела означал бы, что либо мы не достучались,
	// либо машина принесла подпись не на тот хост.
	talk, err := a.storage.client(a.storage.Endpoint)
	if err != nil {
		return agent.PresignOutput{}, err
	}

	sign, err := a.storage.client(a.storage.Public)
	if err != nil {
		return agent.PresignOutput{}, err
	}

	// Ведро заводим здесь же, если его нет. Отдельный шаг установки был
	// бы ещё одним местом, где что-то могло не случиться, а спросить у
	// хранилища дёшево и делается раз.
	if err := ensureBucket(ctx, talk, a.storage.Bucket); err != nil {
		return agent.PresignOutput{}, err
	}

	// Ключ выводится из прогона и имени: спросить дважды — получить одно
	// место, а не два. То же правило, что у имён записей.
	key := req.Owner.Name + "/" + req.Name

	link, err := sign.PresignedPutObject(ctx, a.storage.Bucket, key, presignLife)
	if err != nil {
		return agent.PresignOutput{}, fmt.Errorf("ссылка не выдалась: %w", err)
	}

	return agent.PresignOutput{URL: link.String(), Key: key}, nil
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

// RecordArtifact writes down what was uploaded.
func (a *Applier) RecordArtifact(ctx context.Context, req agent.RecordArtifactInput) error {
	resource := schema.GroupVersionResource{Group: v1.Group, Version: v1.Version, Resource: "artifacts"}
	client := a.client.Resource(resource).Namespace(req.Owner.Namespace)

	name := agent.ObjectName(req.Owner, req.Name)

	fresh := &unstructured.Unstructured{Object: map[string]any{
		fieldVersion: v1.GroupVersion.String(),
		fieldKind:    "Artifact",
		fieldMetadata: map[string]any{
			fieldName: name,
			"ownerReferences": []any{map[string]any{
				fieldVersion: v1.GroupVersion.String(),
				fieldKind:    kindRun,
				fieldName:    req.Owner.Name,
				fieldUID:     req.Owner.UID,
			}},
			fieldLabels: map[string]any{LabelRun: req.Owner.Name, LabelManaged: yes},
		},
		"spec": map[string]any{
			"runRef":  map[string]any{fieldName: req.Owner.Name},
			fieldName: req.Name,
			"key":     req.Key,
		},
	}}

	created, err := client.Create(ctx, fresh, metav1.CreateOptions{})
	if err != nil {
		existing, getErr := client.Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("артефакт %s не записался: %w", name, err)
		}

		created = existing
	}

	status := map[string]any{"digest": req.Digest, "size": req.Size}
	if err := unstructured.SetNestedMap(created.Object, status, "status"); err != nil {
		return fmt.Errorf("статус артефакта не собрался: %w", err)
	}

	if _, err := client.UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("дайджест артефакта не записался: %w", err)
	}

	return nil
}

// StorageFrom reads where artifacts go from the environment.
func StorageFrom(get func(string) string) Storage {
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
