package pipelineflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/converter"
)

func infoWith(pipeline, digest string) *workflowpb.WorkflowExecutionInfo {
	info := &workflowpb.WorkflowExecutionInfo{Type: &commonpb.WorkflowType{Name: pipeline}}
	if digest != "" {
		p, _ := converter.GetDefaultDataConverter().ToPayload(digest)
		info.Memo = &commonpb.Memo{Fields: map[string]*commonpb.Payload{MemoRequest: p}}
	}
	return info
}

// The same request — pipeline and params — is the same run; another
// pipeline, other params, or an execution that never recorded its request
// is not.
func TestSameRequestIsPipelineAndParams(t *testing.T) {
	params := []byte(`{"zone":"a","keep":"1h"}`)
	mine := RequestDigest("perf", params)
	require.True(t, SameRequest(infoWith("perf", mine), "perf", params))
	require.False(t, SameRequest(infoWith("perf", mine), "perf", []byte(`{"zone":"b","keep":"1h"}`)), "other params")
	require.False(t, SameRequest(infoWith("smoke", RequestDigest("smoke", params)), "perf", params), "other pipeline")
	require.False(t, SameRequest(infoWith("perf", ""), "perf", params), "an execution without its request cannot vouch")
	require.NotEqual(t, RequestDigest("perf", params), RequestDigest("perf", nil))
	require.NotEqual(t, RequestDigest("a", []byte("b")), RequestDigest("ab", nil), "the separator keeps pipeline and params apart")
}
