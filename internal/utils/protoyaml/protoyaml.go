// Package protoyaml converts proto messages to and from YAML via their
// canonical protojson mapping.
package protoyaml

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"sigs.k8s.io/yaml"
)

// Unmarshal option presets are effectively constants; protojson options
// cannot be const in Go.
//
//nolint:gochecknoglobals // see above
var (
	nonStrict = protojson.UnmarshalOptions{DiscardUnknown: true}
	strict    = protojson.UnmarshalOptions{DiscardUnknown: false}
)

// Marshal writes the given proto.Message in YAML format.
func Marshal(m proto.Message) ([]byte, error) {
	json, err := protojson.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("protoyaml: marshal to json: %w", err)
	}

	out, err := yaml.JSONToYAML(json)
	if err != nil {
		return nil, fmt.Errorf("protoyaml: json to yaml: %w", err)
	}

	return out, nil
}

// Unmarshal reads the given []byte into the given proto.Message, discarding
// any unknown fields in the input.
func Unmarshal(b []byte, message proto.Message) error {
	json, err := yaml.YAMLToJSON(b)
	if err != nil {
		return fmt.Errorf("protoyaml: yaml to json: %w", err)
	}

	if err := nonStrict.Unmarshal(json, message); err != nil {
		return fmt.Errorf("protoyaml: unmarshal: %w", err)
	}

	return nil
}

// UnmarshalStrict reads the given []byte into the given proto.Message. If there
// are any unknown fields in the input, an error is returned.
func UnmarshalStrict(b []byte, message proto.Message) error {
	json, err := yaml.YAMLToJSON(b)
	if err != nil {
		return fmt.Errorf("protoyaml: yaml to json: %w", err)
	}

	if err := strict.Unmarshal(json, message); err != nil {
		return fmt.Errorf("protoyaml: unmarshal strict: %w", err)
	}

	return nil
}
