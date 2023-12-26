// Copyright 2023 Ant Group. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package distribution

import (
	"fmt"
	"strings"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/reference/docker"
	"github.com/containerd/containerd/remotes"
	"github.com/nydusaccelerator/nydus-cli/pkg/remote"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var NydusRefSuffix = "_nydus_v2"

type Distribution struct {
	resolverFunc func(bool) remotes.Resolver
}

// AppendNydusSuffix appends nydus suffix to the image `ref` and return nydus image.
func AppendNydusSuffix(ref string) (string, error) {
	named, err := docker.ParseDockerRef(ref)
	if err != nil {
		return "", errors.Wrapf(err, "invalid image reference: %s", ref)
	}
	if _, ok := named.(docker.Digested); ok {
		return "", fmt.Errorf("unsupported digested image reference: %s", ref)
	}
	named = docker.TagNameOnly(named)
	if strings.HasSuffix(named.String(), NydusRefSuffix) {
		return ref, nil
	}
	target := named.String() + NydusRefSuffix
	return target, nil
}

// HasNydusSuffix checks weather if the image `ref` has `_nydus_v2` suffix.
func HasNydusSuffix(ref string) (bool, error) {
	named, err := docker.ParseDockerRef(ref)
	if err != nil {
		return false, errors.Wrapf(err, "invalid image reference: %s", ref)
	}
	if _, ok := named.(docker.Digested); ok {
		return false, fmt.Errorf("unsupported digested image reference: %s", ref)
	}
	named = docker.TagNameOnly(named)
	return strings.HasSuffix(named.String(), NydusRefSuffix), nil
}

// New creates Distribution by distribution username, password.
func New(username, password string) (*Distribution, error) {
	resolverFunc := func(plainHTTP bool) remotes.Resolver {
		return remote.NewResolver(true, plainHTTP, func(ref string) (string, string, error) {
			return username, password, nil
		})
	}
	return &Distribution{
		resolverFunc: resolverFunc,
	}, nil
}

// IsImageExists checks if the image `ref` is exists in distribution.
func (d *Distribution) IsImageExists(ctx context.Context, ref string) (bool, error) {
	remoter, err := remote.New(ref, d.resolverFunc)
	if err != nil {
		return false, errors.Wrap(err, "create remote")
	}

	_, err = remoter.Resolve(ctx)
	if err != nil {
		if errors.Is(err, errdefs.ErrNotFound) {
			return false, nil
		}
		return false, errors.Wrap(err, "resolve image")
	}

	return true, nil
}

// IsNydusImageExists checks if the associated nydus image of `ref` is exists in distribution.
func (d *Distribution) IsNydusImageExists(ctx context.Context, ref string) (bool, error) {
	nydusRef, err := AppendNydusSuffix(ref)
	if err != nil {
		return false, errors.Wrap(err, "append nydus suffix")
	}

	return d.IsImageExists(ctx, nydusRef)
}
