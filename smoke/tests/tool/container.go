package tool

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	echo "github.com/labstack/echo/v4"
	"github.com/nydusaccelerator/nydus-cli/pkg/nsenter"
	"github.com/stretchr/testify/require"
)

type Container struct {
	WorkDir string

	lower  string
	upper  string
	work   string
	merged string
	mount  string

	pid int
}

func (container *Container) Mount(t *testing.T) {
	container.lower = filepath.Join(container.WorkDir, "lower")
	container.upper = filepath.Join(container.WorkDir, "upper")
	container.work = filepath.Join(container.WorkDir, "work")
	container.merged = filepath.Join(container.WorkDir, "merged")
	container.mount = filepath.Join(container.WorkDir, "mount")

	err := os.MkdirAll(container.upper, 0755)
	require.Nil(t, err)
	err = os.MkdirAll(container.work, 0755)
	require.Nil(t, err)
	err = os.MkdirAll(container.merged, 0755)
	require.Nil(t, err)
	err = os.MkdirAll(container.mount, 0755)
	require.Nil(t, err)

	Run(t, fmt.Sprintf("mount -t overlay overlay -o rw,relatime,lowerdir=%s,upperdir=%s,workdir=%s %s", container.lower, container.upper, container.work, container.merged))

	cmd := exec.Command("/bin/bash")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWNS,
	}

	err = cmd.Start()
	require.Nil(t, err)
	container.pid = cmd.Process.Pid

	Run(t, "umount /dir-lower")
	err = os.MkdirAll("/dir-lower", 0755)
	require.Nil(t, err)

	config := &nsenter.Config{
		Mount:  true,
		Target: container.pid,
	}
	_, err = config.ExecuteContext(context.Background(), os.Stdout, "mount", "--bind", container.mount, "/dir-lower")
	require.Nil(t, err)

	go func() {
		err := cmd.Wait()
		require.Nil(t, err)
	}()
}

func addFiles(t *testing.T, baseDir string, files map[string][]byte) {
	for name, data := range files {
		fileName := filepath.Join(baseDir, name)

		err := os.MkdirAll(filepath.Dir(fileName), 0755)
		require.Nil(t, err)

		err = os.WriteFile(fileName, data, 0755)
		require.Nil(t, err)
	}
}

func (container *Container) AddFileToUpper(t *testing.T, files map[string][]byte) {
	addFiles(t, container.merged, files)
}

func (container *Container) AddFileToMount(t *testing.T, files map[string][]byte) {
	addFiles(t, container.mount, files)
}

func (container *Container) Serve(t *testing.T) {
	listener, err := net.Listen("unix", filepath.Join(container.WorkDir, "dockerd.sock"))
	require.Nil(t, err)

	e := echo.New()
	e.Listener = listener

	e.GET("/containers/:id/json", func(c echo.Context) error {
		return c.JSONBlob(http.StatusOK, []byte(fmt.Sprintf(`
		{
			"GraphDriver": {
					"Data": {
							"LowerDir": "%s",
							"MergedDir": "%s",
							"UpperDir": "%s",
							"WorkDir": "%s"
					}
			},
			"Config": {
					"Image": "localhost:5000/nginx:lower_nydus_v2",
					"Labels": {
							"io.kubernetes.container.image": "localhost:5000/nginx:lower_nydus_v2"
					}
			},
			"Mounts": [
				{
					"Source": "/data",
					"Destination": "/data"
				}
			],
			"State": {
				"Pid": %d
			}
		}
	`, container.lower, container.merged, container.upper, container.work, container.pid)))
	})

	server := new(http.Server)
	err = e.StartServer(server)
	require.Nil(t, err)
}

func (container *Container) Destory(t *testing.T) {
	merged := filepath.Join(container.merged)
	defer Run(t, fmt.Sprintf("umount %s", merged))
}
