package pipeline

import (
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/workflow"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/graphene-ci/graphene/sdk/agent"
)

// Ref is what Apply hands back: a record that has been asked for, not
// necessarily one that exists yet. Await turns it into a thing with an
// address.
type Ref[T runtime.Object] struct {
	memo string
	obj  T
	ref  agent.ObjectRef
}

// Memo is what this record is called inside the run — the name the person
// who wrote the pipeline chose.
func (r Ref[T]) Memo() string { return r.memo }

// Object is the record as it was asked for.
func (r Ref[T]) Object() T { return r.obj }

// Name is what the record is called in the cluster.
func (r Ref[T]) Name() string { return r.ref.Name }

// Apply asks for a record to exist, owned by the run. It does not wait: a
// machine takes three minutes to boot and there is usually work to hand out
// meanwhile.
//
// The memo is the record's name inside the run. Applying twice with the
// same memo means the same record — that is what makes this safe when the
// activity runs more than once, which it will.
func Apply[T runtime.Object](run Run, memo string, obj T) Ref[T] {
	apiVersion, kind, err := run.gvkOf(obj)
	if err != nil {
		run.raise("apply "+memo, err)
	}

	manifest, err := marshalWithKind(obj, apiVersion, kind)
	if err != nil {
		run.raise("apply "+memo, err)
	}

	in := agent.ApplyInput{Name: memo, Manifest: manifest, Owner: run.s.owner}

	var out agent.ApplyOutput
	if err := workflow.ExecuteActivity(run.s.ctx, agent.ActivityApply, in).Get(run.s.ctx, &out); err != nil {
		run.raise("apply "+memo, err)
	}

	run.s.created = append(run.s.created, out.Ref)

	return Ref[T]{memo: memo, obj: obj, ref: out.Ref}
}

// Await waits until the record is ready and returns it with whatever the
// provider filled in — an address, an id, a generated name.
//
// The workflow sleeps in its history while it waits: it holds no worker
// slot, and a restart of everything underneath does not lose the wait.
func Await[T runtime.Object](run Run, ref Ref[T]) T {
	for {
		if sig, ok := run.s.arrived[ref.memo]; ok {
			delete(run.s.arrived, ref.memo)

			if err := applyStatus(ref.obj, sig.Status); err != nil {
				run.raise("await "+ref.memo, err)
			}

			return ref.obj
		}

		var sig agent.ReadySignal

		run.s.ready.Receive(run.s.ctx, &sig)

		// Readiness that is not ours, or not yet readiness, is kept:
		// signals queue per workflow, not per record.
		if sig.Ready {
			run.s.arrived[sig.Name] = sig
		}
	}
}

// marshalWithKind renders the object as the manifest the worker applies,
// with apiVersion and kind in it. Generated types leave those empty, and a
// record without them is not a record.
func marshalWithKind(obj runtime.Object, apiVersion, kind string) ([]byte, error) {
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("объект не сериализуется: %w", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("объект сериализовался не в запись: %w", err)
	}

	fields["apiVersion"] = quoted(apiVersion)
	fields["kind"] = quoted(kind)

	manifest, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("манифест не собрался: %w", err)
	}

	return manifest, nil
}

// quoted renders a string as JSON. It cannot fail for a group, a version
// or a kind: those are names, and json.Marshal only refuses cycles,
// channels and functions.
func quoted(value string) json.RawMessage {
	raw, _ := json.Marshal(value) //nolint:errchkjson // строка всегда сериализуется

	return raw
}

// applyStatus puts the record's status, as the cluster has it now, back
// into the object the pipeline holds.
func applyStatus(obj runtime.Object, status []byte) error {
	if len(status) == 0 {
		return nil
	}

	wrapped, err := json.Marshal(map[string]json.RawMessage{"status": status})
	if err != nil {
		return fmt.Errorf("статус не собрался: %w", err)
	}

	// Unmarshalling a partial document leaves every other field alone, so
	// what the pipeline already put in the spec survives.
	if err := json.Unmarshal(wrapped, obj); err != nil {
		return fmt.Errorf("статус не лёг в объект: %w", err)
	}

	return nil
}
