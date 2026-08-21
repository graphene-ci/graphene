package ctl

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"connectrpc.com/connect"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// cmdRes is the record surface: listing, the five dimensions, the tree,
// and the lifecycle verbs.
func cmdRes(ctx context.Context, args []string) error {
	word, rest, err := need(args, "list, get, tree, delete, transfer, invoke, events, logs, metrics, trace")
	if err != nil {
		return err
	}
	switch word {
	case "list":
		return resList(ctx, rest)
	case "get":
		return resGet(ctx, rest)
	case "tree":
		return resTree(ctx, rest)
	case "delete":
		return resDelete(ctx, rest)
	case "transfer":
		return resTransfer(ctx, rest)
	case "invoke":
		return resInvoke(ctx, rest)
	case "events", "logs", "metrics", "trace":
		return observeDimension(ctx, word, rest, "")
	default:
		return fmt.Errorf("res %q: unknown verb", word)
	}
}

func resList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("res list", flag.ExitOnError)
	co := commonFlags(fs)
	kind := fs.String("k", "", "kind filter")
	phase := fs.String("p", "", "phase filter")
	owner := fs.String("owner", "", "owner ref filter")
	watch := fs.Bool("w", false, "watch: print the snapshot, then only changes")
	var labels labelFlag
	fs.Var(&labels, "l", "label selector k=v (repeatable)")
	_, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	list := func() (*managementv1.ListResponse, error) {
		resp, err := d.Resources.List(ctx, connect.NewRequest(&managementv1.ListRequest{
			Selector: &managementv1.Selector{Kind: *kind, Phase: *phase, Owner: *owner, Labels: labels.m},
		}))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}
	if *watch {
		return watchList(ctx, co, []string{"REF", "PHASE", "OWNER", "LABELS"}, func() (map[string]watchRow, error) {
			msg, err := list()
			if err != nil {
				return nil, err
			}
			rows := make(map[string]watchRow, len(msg.GetResources()))
			for _, r := range msg.GetResources() {
				rows[r.GetRef()] = watchRow{
					cols: []string{r.GetRef(), r.GetPhase(), r.GetOwner(), labelsCell(r.GetLabels())},
					msg:  r,
				}
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
	rows := make([][]string, 0, len(msg.GetResources()))
	for _, r := range msg.GetResources() {
		rows = append(rows, []string{r.GetRef(), r.GetPhase(), r.GetOwner(), labelsCell(r.GetLabels())})
	}
	table([]string{"REF", "PHASE", "OWNER", "LABELS"}, rows)
	return nil
}

func resGet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("res get", flag.ExitOnError)
	co := commonFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: res get <ref>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	resp, err := d.Resources.Get(ctx, connect.NewRequest(&managementv1.GetRequest{Ref: pos[0]}))
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

func resTree(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("res tree", flag.ExitOnError)
	co := commonFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: res tree <owner>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	resp, err := d.Resources.Tree(ctx, connect.NewRequest(&managementv1.TreeRequest{Owner: pos[0]}))
	if err != nil {
		return err
	}
	if done, err := co.emit(resp.Msg); done || err != nil {
		return err
	}
	fmt.Fprintln(out, pos[0])
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

func resDelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("res delete", flag.ExitOnError)
	co := commonFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: res delete <ref>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	if _, err := d.Resources.Delete(ctx, connect.NewRequest(&managementv1.DeleteRequest{Ref: pos[0]})); err != nil {
		return err
	}
	fmt.Fprintf(out, "%s: deletion signaled\n", pos[0])
	return nil
}

func resTransfer(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("res transfer", flag.ExitOnError)
	co := commonFlags(fs)
	keep := fs.Duration("keep", 0, "TTL under the new owner (stands only); 0 keeps until deleted")
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("usage: res transfer <ref> <new-owner>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	_, err = d.Resources.Transfer(ctx, connect.NewRequest(&managementv1.TransferRequest{
		Ref:         pos[0],
		NewOwner:    pos[1],
		KeepSeconds: int64(*keep / time.Second),
	}))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "%s -> %s\n", pos[0], pos[1])
	return nil
}

func resInvoke(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("res invoke", flag.ExitOnError)
	co := commonFlags(fs)
	data := fs.String("data", "", "command payload as JSON")
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("usage: res invoke <ref> <command>")
	}
	d, err := co.dial()
	if err != nil {
		return err
	}
	resp, err := d.Resources.Invoke(ctx, connect.NewRequest(&managementv1.InvokeRequest{
		Ref:     pos[0],
		Command: pos[1],
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
