package sourceflow

// A managed tree is kept FILE BY FILE, not as one archive. Editing a
// file used to mean downloading the whole tar.gz, unpacking it,
// changing one entry, packing it again and uploading it back — work
// proportional to the whole project on every autosave. Here a write
// stores one blob and one index; the archive is built once, when a
// revision is materialized.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
)

// Entry is one file of the tree.
type Entry struct {
	// Blob names the file's own object in the store.
	Blob string `json:"blob"`
	Size int64  `json:"size"`
	// Digest pins the content, and is what makes a write cheap: an
	// unchanged file keeps its blob.
	Digest string `json:"digest"`
}

// Index is the whole tree: path -> entry. Sorted on the wire, so the
// same files always render the same bytes and the same digest.
type Index map[string]Entry

// Digest is the tree's identity: the digest of the rendered index.
func (ix Index) Digest() (string, []byte, error) {
	raw, err := ix.Marshal()
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), raw, nil
}

// Marshal renders the index deterministically.
func (ix Index) Marshal() ([]byte, error) {
	paths := make([]string, 0, len(ix))
	for p := range ix {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, p := range paths {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		val, err := json.Marshal(ix[p])
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteByte(':')
		buf.Write(val)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// ParseIndex reads an index blob.
func ParseIndex(raw []byte) (Index, error) {
	ix := Index{}
	if len(raw) == 0 {
		return ix, nil
	}
	if err := json.Unmarshal(raw, &ix); err != nil {
		return nil, fmt.Errorf("source index: %w", err)
	}
	return ix, nil
}

// BlobPath is where one file's content lives: content-addressed under
// the source, so two revisions of a file coexist and an unchanged file
// is never rewritten.
func BlobPath(sourceId, digest string) string {
	return fmt.Sprintf("sources/%s/files/%s", sourceId, strings.TrimPrefix(digest, "sha256:"))
}

// IndexPath is where one version of the index lives.
func IndexPath(sourceId, digest string) string {
	return fmt.Sprintf("sources/%s/index/%s.json", sourceId, strings.TrimPrefix(digest, "sha256:"))
}

// FileDigest is the digest of one file's content.
func FileDigest(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

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

// UnpackTar reads a tar.gz into memory: path -> content. This is how
// an archive becomes a managed tree, once.
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
		if err == io.EOF {
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
