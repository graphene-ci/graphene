package ctl

// The record verbs, kubectl's grammar: the verb first, the kind
// second. A target is "<kind> <id>" or "kind/id"; a run is a record
// too (kind "run").

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// targetRef reads one record target from the positionals: either
// "kind/id" in one word or "<kind> <id>" in two. Returns the ref and
// the leftover positionals.
func targetRef(pos []string) (string, []string, error) {
	if len(pos) == 0 {
		return "", nil, fmt.Errorf("want a target: \"<kind> <id>\" or \"kind/id\"")
	}
	if strings.Contains(pos[0], "/") {
		return pos[0], pos[1:], nil
	}
	if len(pos) < 2 {
		return "", nil, fmt.Errorf("want a target: \"%s <id>\" or \"%s/<id>\"", pos[0], pos[0])
	}
	return pos[0] + "/" + pos[1], pos[2:], nil
}

// cmdGet lists a kind or shows one record: get all|<kind> [id].
func cmdGet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	co := commonFlags(fs)
	phase := fs.String("p", "", "lifecycle filter: a record phase (creating, ready, ...) or a run status (Running, Completed, ...)")
	owner := fs.String("owner", "", "owner ref filter (lists)")
	watch := fs.Bool("w", false, "watch: print the snapshot, then only changes")
	var labels labelFlag
	fs.Var(&labels, "l", "label selector k=v (repeatable)")
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	switch {
	case len(pos) == 0:
		return fmt.Errorf("usage: get all|<kind> [id]")
	case len(pos) == 1 && !strings.Contains(pos[0], "/"):
		kind := pos[0]
		if kind == "run" {
			return runListWith(ctx, co, *phase, labels.m, *watch)
		}
		if kind == "all" {
			kind = ""
		}
		return resListWith(ctx, co, kind, *phase, *owner, labels.m, *watch)
	default:
		ref, rest, err := targetRef(pos)
		if err != nil {
			return err
		}
		if len(rest) != 0 {
			return fmt.Errorf("get: unexpected arguments after the target: %v", rest)
		}
		if strings.HasPrefix(ref, "run/") {
			return runGetOne(ctx, co, strings.TrimPrefix(ref, "run/"))
		}
		return resGetOne(ctx, co, ref)
	}
}

func resListWith(ctx context.Context, co *common, kind, phase, owner string, labels map[string]string, watch bool) error {
	d, err := co.dial()
	if err != nil {
		return err
	}
	list := func() (*managementv1.ListResponse, error) {
		resp, err := d.Resources.List(ctx, connect.NewRequest(&managementv1.ListRequest{
			Selector: &managementv1.Selector{Kind: kind, Phase: phase, Owner: owner, Labels: labels},
		}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}
	// The table shapes: default, -o wide (more columns), -o name (refs
	// only, xargs-ready).
	header := []string{"REF", "PHASE", "OWNER", "LABELS"}
	cols := func(r *managementv1.Resource) []string {
		return []string{r.GetRef(), r.GetPhase(), r.GetOwner(), labelsCell(r.GetLabels())}
	}
	switch *co.output {
	case "wide":
		header = []string{"REF", "PHASE", "OWNER", "PENDING", "DELETING", "LABELS"}
		cols = func(r *managementv1.Resource) []string {
			return []string{r.GetRef(), r.GetPhase(), r.GetOwner(),
				fmt.Sprint(r.GetPendingCommands()), fmt.Sprint(r.GetMarkedForDeletion()), labelsCell(r.GetLabels())}
		}
	case "name":
		header = []string{"REF"}
		cols = func(r *managementv1.Resource) []string { return []string{r.GetRef()} }
	}
	if watch {
		return watchList(ctx, co, header, func() (map[string]watchRow, error) {
			msg, err := list()
			if err != nil {
				return nil, err
			}
			rows := make(map[string]watchRow, len(msg.GetResources()))
			for _, r := range msg.GetResources() {
				rows[r.GetRef()] = watchRow{cols: cols(r), msg: r}
			}
			return rows, nil
		})
	}
	msg, err := list()
	if err != nil {
		return err
	}
	if done, err := co.emit(msg); done || err != nil {
		return err
	}
	if *co.output == "name" {
		for _, r := range msg.GetResources() {
			fmt.Fprintln(out, r.GetRef())
		}
		return nil
	}
	rows := make([][]string, 0, len(msg.GetResources()))
	for _, r := range msg.GetResources() {
		rows = append(rows, cols(r))
	}
	table(header, rows)
	return nil
}

func resGetOne(ctx context.Context, co *common, ref string) error {
	d, err := co.dial()
	if err != nil {
		return err
	}
	resp, err := d.Resources.Get(ctx, connect.NewRequest(&managementv1.GetRequest{Ref: ref}))
	if err != nil {
		return err
	}
	if done, err := co.emit(resp.Msg); done || err != nil {
		return err
	}
	r := resp.Msg.GetResource()
	fmt.Fprintf(out, "ref    %s\nphase  %s\nowner  %s\nlabels %s\n",
		r.GetRef(), r.GetPhase(), r.GetOwner(), labelsCell(r.GetLabels()))
	if len(r.GetSpec()) > 0 {
		fmt.Fprintf(out, "spec   %s\n", compactJSON(r.GetSpec()))
	}
	if len(r.GetState()) > 0 {
		fmt.Fprintf(out, "state  %s\n", compactJSON(r.GetState()))
	}
	return nil
}

func cmdTree(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tree", flag.ExitOnError)
	co := commonFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	ref, rest, err := targetRef(pos)
	if err != nil || len(rest) != 0 {
		return fmt.Errorf("usage: tree <owner-ref>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	resp, err := d.Resources.Tree(ctx, connect.NewRequest(&managementv1.TreeRequest{Owner: ref}))
	if err != nil {
		return err
	}
	if done, err := co.emit(resp.Msg); done || err != nil {
		return err
	}
	fmt.Fprintln(out, ref)
	for _, root := range resp.Msg.GetRoots() {
		printTree(root, "  ")
	}
	return nil
}

func printTree(node *managementv1.TreeNode, indent string) {
	r := node.GetResource()
	fmt.Fprintf(out, "%s%s (%s)\n", indent, r.GetRef(), r.GetPhase())
	for _, child := range node.GetChildren() {
		printTree(child, indent+"  ")
	}
}

func cmdDelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	co := commonFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	ref, rest, err := targetRef(pos)
	if err != nil || len(rest) != 0 {
		return fmt.Errorf("usage: delete <kind> <id>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	if _, err := d.Resources.Delete(ctx, connect.NewRequest(&managementv1.DeleteRequest{Ref: ref})); err != nil {
		return err
	}
	fmt.Fprintf(out, "%s: deletion signaled\n", ref)
	return nil
}

func cmdTransfer(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("transfer", flag.ExitOnError)
	co := commonFlags(fs)
	keep := fs.Duration("keep", 0, "TTL under the new owner (stands only); 0 keeps until deleted")
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	ref, rest, err := targetRef(pos)
	if err != nil || len(rest) != 1 {
		return fmt.Errorf("usage: transfer <kind> <id> <new-owner>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	_, err = d.Resources.Transfer(ctx, connect.NewRequest(&managementv1.TransferRequest{
		Ref:         ref,
		NewOwner:    rest[0],
		KeepSeconds: int64(*keep / time.Second),
	}))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "%s -> %s\n", ref, rest[0])
	return nil
}

func cmdInvoke(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("invoke", flag.ExitOnError)
	co := commonFlags(fs)
	data := fs.String("data", "", "command payload as JSON")
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	ref, rest, err := targetRef(pos)
	if err != nil || len(rest) != 1 {
		return fmt.Errorf("usage: invoke <kind> <id> <command>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	resp, err := d.Resources.Invoke(ctx, connect.NewRequest(&managementv1.InvokeRequest{
		Ref:     ref,
		Command: rest[0],
		Payload: []byte(*data),
	}))
	if err != nil {
		return err
	}
	if done, err := co.emit(resp.Msg); done || err != nil {
		return err
	}
	fmt.Fprintln(out, string(resp.Msg.GetResult()))
	return nil
}

// compactJSON re-renders raw JSON on one line; invalid input passes
// through.
func compactJSON(raw []byte) string {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return string(raw)
	}
	compact, err := json.Marshal(v)
	if err != nil {
		return string(raw)
	}
	return string(compact)
}
