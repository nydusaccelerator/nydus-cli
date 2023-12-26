package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nydusaccelerator/nydus-cli/smoke/tests/tool"
	"github.com/stretchr/testify/require"
)

func testImage(t *testing.T, backend string) {
	workDir, err := os.MkdirTemp("./", "smoke-")
	require.Nil(t, err)
	workDir, err = filepath.Abs(workDir)
	require.Nil(t, err)
	defer os.RemoveAll(workDir)

	image := tool.Image{
		WorkDir: workDir,
	}

	ossBackend := " --backend-type oss --backend-config-file ./smoke/tests/texture/backend.config.oss.json"

	// Lower files
	image.Build(t, "localhost:5000/nginx:lower", map[string][]byte{
		"dir-lower/file-1": []byte("file-1"),
		"dir-lower/file-2": []byte("file-2"),
	})

	convert := "nydusify convert --fs-version 5 --compressor lz4_block --source localhost:5000/nginx:lower --target localhost:5000/nginx:lower_nydus_v2"
	if backend == "oss" {
		convert += ossBackend
	}
	tool.Run(t, convert)

	mount := "nydusify mount --mount-path %s --target localhost:5000/nginx:lower_nydus_v2"
	if backend == "oss" {
		mount += ossBackend
	}
	go tool.Run(t, fmt.Sprintf(mount, filepath.Join(workDir, "lower")))
	defer func() {
		tool.Run(t, "pkill -15 nydusd")
		tool.Run(t, "pkill -15 nydusify")
	}()

	time.Sleep(time.Second * 5)

	container := tool.Container{
		WorkDir: workDir,
	}
	container.Mount(t)

	// Upper files
	container.AddFileToUpper(t, map[string][]byte{
		"dir-upper/file-1": []byte("file-1"),
		"dir-upper/file-2": []byte("file-2"),
	})

	// Mount files (based dir-upper)
	container.AddFileToMount(t, map[string][]byte{
		"file-3": []byte("file-3"),
	})

	go container.Serve(t)
	defer container.Destory(t)

	config := "./smoke/tests/texture/config.registry.yml"
	if backend == "oss" {
		config = "./smoke/tests/texture/config.oss.yml"
	}
	tool.Run(t, fmt.Sprintf("./nydus-cli --config %s commit --docker.addr %s --container docker://%s --target localhost:5000/nginx:committed --with-mount-path /dir-lower", config, filepath.Join(workDir, "dockerd.sock"), "container"))

	image = tool.Image{
		WorkDir: workDir,
	}

	// Committed files
	image.Build(t, "localhost:5000/nginx:committed_oci", map[string][]byte{
		"dir-lower/file-3": []byte("file-3"),
		"dir-upper/file-1": []byte("file-1"),
		"dir-upper/file-2": []byte("file-2"),
	})

	check := "nydusify check --source localhost:5000/nginx:committed_oci --target localhost:5000/nginx:committed_nydus_v2"
	if backend == "oss" {
		check += ossBackend
	}
	tool.Run(t, check)
}

func TestImage(t *testing.T) {
	testImage(t, "registry")
}
