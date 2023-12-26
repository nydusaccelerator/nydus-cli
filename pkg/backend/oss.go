/*
 * Copyright (c) 2022. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package backend

import (
	"context"
	"fmt"
	"io"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/containerd/containerd/content"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/nydusaccelerator/nydus-cli/pkg/config"
	"github.com/nydusaccelerator/nydus-cli/pkg/remote"
)

type OSSBackend struct {
	// OSS storage does not support directory. Therefore add a prefix to each object
	// to make it a path-like object.
	objectPrefix string
	bucket       *oss.Bucket
	forcePush    bool
}

func NewOSSBackend(cfg *config.OSS, forcePush bool) (*OSSBackend, error) {
	endpoint := cfg.Endpoint
	bucketName := cfg.BucketName

	if endpoint == "" || bucketName == "" {
		return nil, fmt.Errorf("oss `endpoint` and `bucket_name` fields is required")
	}

	// Below fields are not mandatory.
	accessKeyID := cfg.AccessKeyID
	accessKeySecret := cfg.AccessKeySecret
	objectPrefix := cfg.ObjectPrefix

	client, err := oss.New(endpoint, accessKeyID, accessKeySecret)
	if err != nil {
		return nil, errors.Wrap(err, "Create client")
	}

	bucket, err := client.Bucket(bucketName)
	if err != nil {
		return nil, errors.Wrap(err, "Create bucket")
	}

	return &OSSBackend{
		objectPrefix: objectPrefix,
		bucket:       bucket,
		forcePush:    forcePush,
	}, nil
}

// Ported from https://github.com/aliyun/aliyun-oss-go-sdk/blob/v2.2.6/oss/utils.go#L259
func splitFileByPartSize(blobSize, chunkSize int64) ([]oss.FileChunk, error) {
	if chunkSize <= 0 {
		return nil, errors.New("invalid chunk size")
	}

	var chunkN = blobSize / chunkSize
	if chunkN >= 10000 {
		return nil, errors.New("too many parts, please increase chunk size")
	}

	var chunks []oss.FileChunk
	var chunk = oss.FileChunk{}
	for i := int64(0); i < chunkN; i++ {
		chunk.Number = int(i + 1)
		chunk.Offset = i * chunkSize
		chunk.Size = chunkSize
		chunks = append(chunks, chunk)
	}

	if blobSize%chunkSize > 0 {
		chunk.Number = len(chunks) + 1
		chunk.Offset = int64(len(chunks)) * chunkSize
		chunk.Size = blobSize % chunkSize
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// Upload nydus blob to oss storage backend.
func (b *OSSBackend) push(ctx context.Context, ra content.ReaderAt, desc ocispec.Descriptor) error {
	blobID := desc.Digest.Hex()
	blobObjectKey := b.objectPrefix + blobID

	if exist, err := b.bucket.IsObjectExist(blobObjectKey); err != nil {
		return errors.Wrap(err, "check object existence")
	} else if exist && !b.forcePush {
		return nil
	}

	chunks, err := splitFileByPartSize(ra.Size(), remote.ChunkSize)
	if err != nil {
		return errors.Wrap(err, "split blob by part num")
	}

	imur, err := b.bucket.InitiateMultipartUpload(blobObjectKey)
	if err != nil {
		return errors.Wrap(err, "initiate multipart upload")
	}
	partsChan := make(chan oss.UploadPart, len(chunks))

	g := new(errgroup.Group)
	for _, chunk := range chunks {
		ck := chunk
		g.Go(func() error {
			return remote.WithRetry(func() error {
				p, err := b.bucket.UploadPart(imur, io.NewSectionReader(ra, ck.Offset, ck.Size), ck.Size, ck.Number)
				if err != nil {
					return errors.Wrap(err, "upload part")
				}
				partsChan <- p
				return nil
			})
		})
	}

	if err := g.Wait(); err != nil {
		_ = b.bucket.AbortMultipartUpload(imur)
		close(partsChan)
		return errors.Wrap(err, "upload parts")
	}
	close(partsChan)

	var parts []oss.UploadPart
	for p := range partsChan {
		parts = append(parts, p)
	}

	_, err = b.bucket.CompleteMultipartUpload(imur, parts)
	if err != nil {
		return errors.Wrap(err, "complete multipart upload")
	}

	return nil
}

func (b *OSSBackend) Push(ctx context.Context, ra content.ReaderAt, desc ocispec.Descriptor) error {
	return remote.WithRetry(func() error {
		return b.push(ctx, ra, desc)
	})
}

func (b *OSSBackend) Pull(blobDigest digest.Digest) (io.ReadCloser, error) {
	blobID := blobDigest.Hex()
	blobObjectKey := b.objectPrefix + blobID
	return b.bucket.GetObject(blobObjectKey)
}

func (b *OSSBackend) External() bool {
	return true
}
