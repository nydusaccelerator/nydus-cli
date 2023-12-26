package container

import (
	"fmt"
	"strings"

	"github.com/containerd/containerd/mount"
)

// findOverlayLowerdirs returns the index of lowerdir in mount's options and
// all the lowerdir target.
func findOverlayLowerdirs(opts []string) (int, []string) {
	var (
		idx    = -1
		prefix = "lowerdir="
	)

	for i, opt := range opts {
		if strings.HasPrefix(opt, prefix) {
			idx = i
			break
		}
	}

	if idx == -1 {
		return -1, nil
	}
	return idx, strings.Split(opts[idx][len(prefix):], ":")
}

func GetLowerDirs(mountpoint string) ([]string, error) {
	info, err := mount.Lookup(mountpoint)
	if err != nil {
		return nil, fmt.Errorf("lookup mount info for %s", mountpoint)
	}

	_, lowerDirs := findOverlayLowerdirs(strings.Split(info.VFSOptions, ","))

	return lowerDirs, nil
}
