package workflow

import (
	"testing"

	"github.com/containerd/containerd/mount"
	"github.com/nydusaccelerator/nydus-cli/pkg/container"
	"github.com/stretchr/testify/require"
)

func TestPrepareMounts(t *testing.T) {
	containerMounts := []container.Mount{
		{
			Source:      "/host/ossfs",
			Destination: "/guest/ossfs",
		},
	}
	targetPaths := []string{
		"/guest/ossfs/foo",
		"/guest/ossfs/bar",
	}

	targetMounts, err := prepareMounts(containerMounts, targetPaths)
	require.NoError(t, err)

	require.Equal(t, []mount.Mount{
		{
			Type:   "bind",
			Source: "/host/ossfs/foo",
			Target: "guest/ossfs/foo",
			Options: []string{
				"ro",
				"rbind",
			},
		},
		{
			Type:   "bind",
			Source: "/host/ossfs/bar",
			Target: "guest/ossfs/bar",
			Options: []string{
				"ro",
				"rbind",
			},
		},
	}, targetMounts)
}
