package sourceflow

// Reading a checkout: a source's bytes live in one tar.gz, unpacked
// into memory when a file is read or a revision is materialized.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
)

// CleanPath refuses anything that would escape the tree. Both an
// absolute path and one that climbs out are refused rather than
// quietly rewritten: a caller who asked for /etc/passwd meant the
// host's file, and answering with the tree's own etc/passwd would be a
// different question silently answered.
func CleanPath(p string) (string, error) {
	clean := path.Clean(strings.TrimPrefix(strings.TrimSpace(strings.ReplaceAll(p, "\\", "/")), "./"))
	switch {
	case clean == "" || clean == "." || clean == "/":
		return "", fmt.Errorf("path is required")
	case path.IsAbs(clean):
		return "", fmt.Errorf("path %q is absolute; a source path is relative to the tree root", p)
	case clean == ".." || strings.HasPrefix(clean, "../"):
		return "", fmt.Errorf("path %q escapes the tree", p)
	}
	return clean, nil
}

// UnpackTar reads a tar.gz into memory: path -> content.
func UnpackTar(raw []byte, maxFileBytes int64) (map[string][]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("source is not a gzip archive: %w", err)
	}
	defer func() { _ = zr.Close() }()
	out := map[string][]byte{}
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		clean, err := CleanPath(hdr.Name)
		if err != nil {
			continue
		}
		content, err := io.ReadAll(io.LimitReader(tr, maxFileBytes+1))
		if err != nil {
			return nil, err
		}
		if int64(len(content)) > maxFileBytes {
			return nil, fmt.Errorf("file %q is larger than %d bytes", clean, maxFileBytes)
		}
		out[clean] = content
	}
	return out, nil
}

// PackTar renders a tree as a deterministic tar.gz — what a build
// receives. Paths sorted, no timestamps, no ownership: the same files
// give the same archive, so an unchanged tree deduplicates to the same
// revision.
func PackTar(files map[string][]byte) ([]byte, error) {
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	for _, p := range paths {
		content := files[p]
		if err := tw.WriteHeader(&tar.Header{
			Name: p, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg,
		}); err != nil {
			return nil, err
		}
		if _, err := tw.Write(content); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
