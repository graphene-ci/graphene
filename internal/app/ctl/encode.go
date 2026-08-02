package ctl

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/utils/protoyaml"
)

const (
	docSeparator = "---"
	scanStart    = 64 * 1024
	scanMax      = 16 * 1024 * 1024
)

// EncodeResources renders resources as a YAML stream in their canonical
// protojson shape: what ctl prints is exactly what ctl (and anything else
// speaking the API) reads back.
func EncodeResources(resources []*graphenepbv1.Resource) ([]byte, error) {
	var out []byte

	for i, res := range resources {
		chunk, err := protoyaml.Marshal(res)
		if err != nil {
			return nil, fmt.Errorf("ctl: encode resource: %w", err)
		}

		if i > 0 {
			out = append(out, []byte(docSeparator+"\n")...)
		}

		out = append(out, chunk...)
	}

	return out, nil
}

// DecodeResources parses a YAML stream of resources.
func DecodeResources(raw []byte) ([]*graphenepbv1.Resource, error) {
	var out []*graphenepbv1.Resource

	for _, chunk := range splitDocuments(raw) {
		if len(bytes.TrimSpace(chunk)) == 0 {
			continue
		}

		res := &graphenepbv1.Resource{}
		if err := protoyaml.UnmarshalStrict(chunk, res); err != nil {
			return nil, fmt.Errorf("ctl: decode resource: %w", err)
		}

		out = append(out, res)
	}

	return out, nil
}

// splitDocuments cuts a multi-document stream on "---" lines: protoyaml
// converts through JSON and handles one document at a time.
func splitDocuments(raw []byte) [][]byte {
	var (
		chunks  [][]byte
		current bytes.Buffer
	)

	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, scanStart), scanMax)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimRight(line, " \t") == docSeparator {
			chunks = append(chunks, bytes.Clone(current.Bytes()))
			current.Reset()

			continue
		}

		current.WriteString(line)
		current.WriteByte('\n')
	}

	return append(chunks, bytes.Clone(current.Bytes()))
}
