package ctl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

var (
	// ErrNoResources — an apply input carried nothing.
	ErrNoResources = errors.New("ctl: input contains no resources")
	// ErrBadSelector — a selector term is not "path=value".
	ErrBadSelector = errors.New("ctl: selector term must be path=value")
)

// Get reads one resource by kind and path.
func (c *Client) Get(ctx context.Context, kind string, path []string) (*graphenepbv1.Resource, error) {
	got, err := c.Resources.Get(ctx, &graphenepbv1.GetRequest{
		Key: &graphenepbv1.Key{Kind: kind, Path: path},
	})
	if err != nil {
		return nil, fmt.Errorf("ctl: get: %w", err)
	}

	return got.GetResource(), nil
}

// List reads a kind, optionally narrowed by a path prefix and selector
// terms ("spec.placement=k1"), following pagination to the end.
func (c *Client) List(
	ctx context.Context,
	kind string,
	prefix []string,
	selector []*graphenepbv1.FieldMatch,
) ([]*graphenepbv1.Resource, error) {
	var (
		out   []*graphenepbv1.Resource
		token string
	)

	for {
		page, err := c.Resources.List(ctx, &graphenepbv1.ListRequest{
			Kind:       kind,
			PathPrefix: prefix,
			Selector:   selector,
			PageToken:  token,
		})
		if err != nil {
			return nil, fmt.Errorf("ctl: list: %w", err)
		}

		out = append(out, page.GetResources()...)

		token = page.GetNextPageToken()
		if token == "" {
			return out, nil
		}
	}
}

// Apply writes resources read from a YAML stream. Each write carries the
// revision the document was read at, so a concurrent change is reported as
// a conflict instead of being overwritten; a resource without a revision
// is a create.
func (c *Client) Apply(ctx context.Context, raw []byte) ([]*graphenepbv1.Key, error) {
	resources, err := DecodeResources(raw)
	if err != nil {
		return nil, err
	}

	if len(resources) == 0 {
		return nil, ErrNoResources
	}

	applied := make([]*graphenepbv1.Key, 0, len(resources))

	for _, res := range resources {
		if _, err := c.Resources.Put(ctx, &graphenepbv1.PutRequest{
			Resource:         res,
			ExpectedRevision: res.GetRevision(),
		}); err != nil {
			return applied, fmt.Errorf("ctl: apply %s: %w", keyText(res.GetKey()), err)
		}

		applied = append(applied, res.GetKey())
	}

	return applied, nil
}

// Delete removes a resource; revision 0 means "whatever is there now",
// which requires a read first — the API itself always demands the CAS
// token.
func (c *Client) Delete(ctx context.Context, kind string, path []string, revision uint64) error {
	if revision == 0 {
		current, err := c.Get(ctx, kind, path)
		if err != nil {
			return err
		}

		revision = current.GetRevision()
	}

	if _, err := c.Resources.Delete(ctx, &graphenepbv1.DeleteRequest{
		Key:              &graphenepbv1.Key{Kind: kind, Path: path},
		ExpectedRevision: revision,
	}); err != nil {
		return fmt.Errorf("ctl: delete: %w", err)
	}

	return nil
}

// WatchFunc consumes one watch event; returning an error stops the watch.
type WatchFunc func(event *graphenepbv1.WatchEvent) error

// Watch streams events of a kind until the context ends or the handler
// stops it. Events start with the catch-up already delivered: the stream
// opens with the current state, then the sync marker, then live changes.
func (c *Client) Watch(
	ctx context.Context,
	kind string,
	prefix []string,
	selector []*graphenepbv1.FieldMatch,
	handle WatchFunc,
) error {
	stream, err := c.Resources.Watch(ctx, &graphenepbv1.WatchRequest{
		Kind:       kind,
		PathPrefix: prefix,
		Selector:   selector,
	})
	if err != nil {
		return fmt.Errorf("ctl: watch: %w", err)
	}

	for {
		event, err := stream.Recv()

		switch {
		case errors.Is(err, io.EOF):
			return nil // the server closed the stream
		case err != nil && ctx.Err() != nil:
			return nil //nolint:nilerr // a cancelled watch is a clean exit
		case err != nil:
			return fmt.Errorf("ctl: watch stream: %w", err)
		}

		if err := handle(event); err != nil {
			return err
		}
	}
}

// Definitions lists the kinds the kernel knows, latest version each.
func (c *Client) Definitions(ctx context.Context) ([]*graphenepbv1.ResourceDefinition, error) {
	resp, err := c.Resources.ListDefinitions(ctx, &graphenepbv1.ListDefinitionsRequest{})
	if err != nil {
		return nil, fmt.Errorf("ctl: definitions: %w", err)
	}

	return resp.GetDefinitions(), nil
}

func keyText(key *graphenepbv1.Key) string {
	var out strings.Builder

	out.WriteString(key.GetKind())

	for _, seg := range key.GetPath() {
		out.WriteByte('/')
		out.WriteString(seg)
	}

	return out.String()
}
