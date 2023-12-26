package backend

import (
	"context"
	"io"

	"github.com/containerd/containerd/content"
	"github.com/nydusaccelerator/nydus-cli/pkg/remote"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

type Registry struct {
	remote *remote.Remote
}

func (r *Registry) push(ctx context.Context, ra content.ReaderAt, desc ocispec.Descriptor) error {
	if err := r.remote.Push(ctx, desc, true, io.NewSectionReader(ra, 0, ra.Size())); err != nil {
		if remote.RetryWithHTTP(err) {
			r.remote.MaybeWithHTTP(err)
			if err := r.remote.Push(ctx, desc, true, io.NewSectionReader(ra, 0, ra.Size())); err != nil {
				return errors.Wrap(err, "push blob")
			}
		} else {
			return errors.Wrap(err, "push blob")
		}
	}
	return nil
}

func (r *Registry) Push(ctx context.Context, ra content.ReaderAt, desc ocispec.Descriptor) error {
	return remote.WithRetry(func() error {
		return r.push(ctx, ra, desc)
	})
}

func (r *Registry) Pull(blobDigest digest.Digest) (io.ReadCloser, error) {
	panic("not implemented")
}

func (r *Registry) External() bool {
	return false
}

func NewRegistryBackend(remote *remote.Remote) (*Registry, error) {
	return &Registry{
		remote: remote,
	}, nil
}
