package backend

import (
	"context"
	"io"

	"github.com/containerd/containerd/content"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type Backend interface {
	Push(ctx context.Context, ra content.ReaderAt, desc ocispec.Descriptor) error
	Pull(blobDigest digest.Digest) (io.ReadCloser, error)
	External() bool
}
