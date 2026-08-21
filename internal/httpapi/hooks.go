package httpapi

// The webhook door: POST /hooks/{ns}/{pipeline}/{trigger} fires a
// declared webhook trigger. Authentication is the trigger's own
// declared secret — an HMAC signature or a bearer token — never the
// installation's API tokens: hook callers (forges, monitors) hold the
// hook secret and nothing else.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/gopherex/xlog"
)

// maxHookBody bounds a webhook payload; forges send kilobytes.
const maxHookBody = 1 << 20

// hooks serves the webhook door.
func (d Deps) hooks(w http.ResponseWriter, r *http.Request) {
	ns, pipelineId, trigger := r.PathValue("ns"), r.PathValue("pipeline"), r.PathValue("trigger")
	b, err := d.Bundles.Get(ns)
	if err != nil {
		http.Error(w, "unknown namespace", http.StatusNotFound)
		return
	}
	spec, err := b.Worker.DescribeTrigger(r.Context(), pipelineId, trigger)
	if err != nil || spec.Kind != "webhook" {
		http.Error(w, "unknown trigger", http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxHookBody))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	if spec.SecretName != "" {
		secret, err := d.Secrets.Get(ns, spec.SecretName)
		if err != nil {
			d.Log.Error("hook secret missing", xlog.String("trigger", trigger), xlog.Err(err))
			http.Error(w, "hook secret is not set", http.StatusInternalServerError)
			return
		}
		if !hookAuthorized(r, body, secret) {
			http.Error(w, "signature mismatch", http.StatusForbidden)
			return
		}
	}
	if err := b.Worker.DeliverHook(r.Context(), pipelineId, trigger, json.RawMessage(body)); err != nil {
		d.Log.Error("hook delivery", xlog.String("trigger", trigger), xlog.Err(err))
		http.Error(w, "delivery failed", http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"accepted"}`))
}

// hookAuthorized accepts either the GitHub-style HMAC signature of the
// body or the bare secret as a bearer token.
func hookAuthorized(r *http.Request, body []byte, secret string) bool {
	if sig := strings.TrimPrefix(r.Header.Get("X-Hub-Signature-256"), "sha256="); sig != r.Header.Get("X-Hub-Signature-256") {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(body)
		want := hex.EncodeToString(mac.Sum(nil))
		return hmac.Equal([]byte(want), []byte(sig))
	}
	if bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "); bearer != "" {
		return hmac.Equal([]byte(bearer), []byte(secret))
	}
	return false
}
