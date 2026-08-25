// Package rbacflow holds the records of authorization: roles, their
// bindings, and the service accounts of this installation. They are
// ordinary entity records — listed, read, deleted and audited like
// everything else, because a permission change is exactly the kind of
// event a history should carry.
package rbacflow

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/pipeline/pkg/flow/ownership"
)

// The kinds of the authorization contour.
const (
	RoleKind           = entity.KindName("role")
	BindingKind        = entity.KindName("rolebinding")
	ServiceAccountKind = entity.KindName("serviceaccount")
)

// --- role ---

// RoleSpec is what a role IS: its rules. Rules are validated when the
// record is written.
type RoleSpec struct {
	Rules authz.Rules `json:"rules"`
	// Description is for humans reading a list of roles.
	Description string `json:"description,omitempty"`
}

// RoleState carries the ownership half and when the role last changed.
type RoleState struct {
	ownership.State
	Rules     authz.Rules `json:"rules,omitempty"`
	UpdatedAt string      `json:"updatedAt,omitempty"`
}

// SetRulesCmd replaces a role's rules — the whole set, never a patch:
// a permission change must be readable as one decision.
type SetRulesCmd struct {
	Rules authz.Rules `json:"rules"`
}

// Name is the command's wire identity.
func (SetRulesCmd) Name() entity.CommandName { return "set-rules" }

// Result binds the response type.
func (SetRulesCmd) Result() RoleRes { return RoleRes{} }

// Validate refuses an unknown verb or kind before anything is written.
func (c SetRulesCmd) Validate() error { return c.Rules.Validate() }

// RoleRes reports the role after a command.
type RoleRes struct {
	Rules authz.Rules `json:"rules"`
}

// NewRole builds the role definition.
func NewRole() *entdefine.Definition[RoleSpec, RoleState] {
	def := entdefine.New[RoleSpec, RoleState](RoleKind,
		entdefine.WithSearchAttributes[RoleSpec, RoleState](true),
		entdefine.WithInit[RoleSpec, RoleState](func(ctx workflow.Context, spec RoleSpec) (RoleState, error) {
			var st RoleState
			if err := spec.Rules.Validate(); err != nil {
				return st, err
			}
			ownership.Init(ctx, &st.State, "")
			st.Rules = spec.Rules
			st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
			return st, nil
		}),
	)
	ownership.Register(def, func(st *RoleState) *ownership.State { return &st.State })
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[RoleSpec, RoleState], cmd SetRulesCmd) (RoleRes, error) {
		st := ec.State()
		st.Rules = cmd.Rules
		st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
		return RoleRes{Rules: st.Rules}, nil
	})
	return def
}

// --- rolebinding ---

// BindingSpec grants one role to subjects in one namespace ("*" is
// every namespace — how a cluster admin is made).
type BindingSpec struct {
	Role      string          `json:"role"`
	Subjects  []authz.Subject `json:"subjects"`
	Namespace string          `json:"namespace"`
}

// Validate refuses a binding that names nothing.
func (s BindingSpec) Validate() error {
	if s.Role == "" {
		return fmt.Errorf("a binding needs a role")
	}
	if len(s.Subjects) == 0 {
		return fmt.Errorf("a binding needs at least one subject")
	}
	for _, sub := range s.Subjects {
		if _, err := authz.ParseSubject(sub.String()); err != nil {
			return err
		}
	}
	return nil
}

// BindingState is the binding as it stands.
type BindingState struct {
	ownership.State
	Role      string          `json:"role"`
	Subjects  []authz.Subject `json:"subjects"`
	Namespace string          `json:"namespace"`
	UpdatedAt string          `json:"updatedAt,omitempty"`
}

// SetSubjectsCmd replaces a binding's subjects.
type SetSubjectsCmd struct {
	Subjects []authz.Subject `json:"subjects"`
}

// Name is the command's wire identity.
func (SetSubjectsCmd) Name() entity.CommandName { return "set-subjects" }

// Result binds the response type.
func (SetSubjectsCmd) Result() BindingRes { return BindingRes{} }

// Validate refuses an empty or malformed subject set.
func (c SetSubjectsCmd) Validate() error {
	if len(c.Subjects) == 0 {
		return fmt.Errorf("a binding needs at least one subject")
	}
	for _, sub := range c.Subjects {
		if _, err := authz.ParseSubject(sub.String()); err != nil {
			return err
		}
	}
	return nil
}

// BindingRes reports the binding after a command.
type BindingRes struct {
	Subjects []authz.Subject `json:"subjects"`
}

// NewBinding builds the rolebinding definition.
func NewBinding() *entdefine.Definition[BindingSpec, BindingState] {
	def := entdefine.New[BindingSpec, BindingState](BindingKind,
		entdefine.WithSearchAttributes[BindingSpec, BindingState](true),
		entdefine.WithInit[BindingSpec, BindingState](func(ctx workflow.Context, spec BindingSpec) (BindingState, error) {
			var st BindingState
			if err := spec.Validate(); err != nil {
				return st, err
			}
			ownership.Init(ctx, &st.State, "")
			st.Role, st.Subjects, st.Namespace = spec.Role, spec.Subjects, spec.Namespace
			if st.Namespace == "" {
				st.Namespace = "default"
			}
			st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
			return st, nil
		}),
	)
	ownership.Register(def, func(st *BindingState) *ownership.State { return &st.State })
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[BindingSpec, BindingState], cmd SetSubjectsCmd) (BindingRes, error) {
		st := ec.State()
		st.Subjects = cmd.Subjects
		st.UpdatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
		return BindingRes{Subjects: st.Subjects}, nil
	})
	return def
}

// --- serviceaccount ---

// AccountSpec is a machine of this installation: an agent, a script, a
// CI job. People never get one — they come from the identity provider.
type AccountSpec struct {
	Description string `json:"description,omitempty"`
}

// AccountState holds the ISSUED tokens — by hash only. A token value
// exists exactly once, in the response that created it.
type AccountState struct {
	ownership.State
	Tokens    []TokenRef `json:"tokens,omitempty"`
	CreatedAt string     `json:"createdAt,omitempty"`
}

// TokenRef is one issued token: its id, its hash, and when it expires.
type TokenRef struct {
	Id     string `json:"id"`
	Hash   string `json:"hash"`
	Issued string `json:"issued"`
	// Expires is empty for a token that lives until it is revoked.
	Expires string `json:"expires,omitempty"`
	Comment string `json:"comment,omitempty"`
}

// IssueTokenCmd records a token that was just minted. The VALUE never
// reaches the record: only its hash, so a leaked history leaks nothing.
type IssueTokenCmd struct {
	Id      string `json:"id"`
	Hash    string `json:"hash"`
	Expires string `json:"expires,omitempty"`
	Comment string `json:"comment,omitempty"`
}

// Name is the command's wire identity.
func (IssueTokenCmd) Name() entity.CommandName { return "issue-token" }

// Result binds the response type.
func (IssueTokenCmd) Result() AccountRes { return AccountRes{} }

// Validate refuses a token record without an id or a hash.
func (c IssueTokenCmd) Validate() error {
	if c.Id == "" || c.Hash == "" {
		return fmt.Errorf("a token needs an id and a hash")
	}
	return nil
}

// RevokeTokenCmd forgets one token — the account keeps working with
// the rest.
type RevokeTokenCmd struct {
	Id string `json:"id"`
}

// Name is the command's wire identity.
func (RevokeTokenCmd) Name() entity.CommandName { return "revoke-token" }

// Result binds the response type.
func (RevokeTokenCmd) Result() AccountRes { return AccountRes{} }

// AccountRes reports the account's tokens (hashes, never values).
type AccountRes struct {
	Tokens []TokenRef `json:"tokens"`
}

// NewAccount builds the serviceaccount definition.
func NewAccount() *entdefine.Definition[AccountSpec, AccountState] {
	def := entdefine.New[AccountSpec, AccountState](ServiceAccountKind,
		entdefine.WithSearchAttributes[AccountSpec, AccountState](true),
		entdefine.WithInit[AccountSpec, AccountState](func(ctx workflow.Context, _ AccountSpec) (AccountState, error) {
			var st AccountState
			ownership.Init(ctx, &st.State, "")
			st.CreatedAt = workflow.Now(ctx).UTC().Format(time.RFC3339)
			return st, nil
		}),
	)
	ownership.Register(def, func(st *AccountState) *ownership.State { return &st.State })
	entdefine.Handle(def, func(ctx workflow.Context, ec *entdefine.Ctx[AccountSpec, AccountState], cmd IssueTokenCmd) (AccountRes, error) {
		st := ec.State()
		st.Tokens = append(st.Tokens, TokenRef{
			Id: cmd.Id, Hash: cmd.Hash, Expires: cmd.Expires, Comment: cmd.Comment,
			Issued: workflow.Now(ctx).UTC().Format(time.RFC3339),
		})
		return AccountRes{Tokens: st.Tokens}, nil
	})
	entdefine.Handle(def, func(_ workflow.Context, ec *entdefine.Ctx[AccountSpec, AccountState], cmd RevokeTokenCmd) (AccountRes, error) {
		st := ec.State()
		kept := st.Tokens[:0]
		for _, t := range st.Tokens {
			if t.Id != cmd.Id {
				kept = append(kept, t)
			}
		}
		st.Tokens = kept
		return AccountRes{Tokens: st.Tokens}, nil
	})
	return def
}

// HashToken is how a token value becomes the record's hash. The value
// is never stored anywhere.
func HashToken(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}
