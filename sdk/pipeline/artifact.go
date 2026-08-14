package pipeline

import (
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/sdk/agent"
)

// Artifact keeps a file the machine produced.
//
// Three steps, and each is where it has to be: we issue a door that opens
// once, the MACHINE walks through it with the bytes, and we write down what
// went. The control plane never carries the bytes and the machine never
// holds a key to storage.
//
// What comes back is the digest, computed while uploading. It is how
// anybody later can say the bytes are the ones this run made.
func Artifact(run Run, target Target, name, path string) string {
	var place agent.PresignOutput

	presign := agent.PresignInput{Owner: run.s.owner, Name: name}
	if err := workflow.ExecuteActivity(run.s.ctx, agent.ActivityPresign, presign).Get(run.s.ctx, &place); err != nil {
		run.raise("артефакт "+name, err)
	}

	// Загрузка идёт на очереди МАШИНЫ: файл лежит там.
	onMachine := workflow.WithTaskQueue(run.s.ctx, target.Queue())

	var put agent.PutOutput

	upload := agent.PutInput{Path: path, URL: place.URL}
	if err := workflow.ExecuteActivity(onMachine, agent.ActivityPut, upload).Get(onMachine, &put); err != nil {
		run.raise("артефакт "+name, err)
	}

	record := agent.RecordArtifactInput{
		Owner:  run.s.owner,
		Name:   name,
		Key:    place.Key,
		Digest: put.Digest,
		Size:   put.Size,
	}

	if err := workflow.ExecuteActivity(run.s.ctx, agent.ActivityRecordArtifact, record).Get(run.s.ctx, nil); err != nil {
		run.raise("артефакт "+name, err)
	}

	return put.Digest
}
