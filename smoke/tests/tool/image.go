package tool

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type Image struct {
	WorkDir string
}

func (image *Image) Build(t *testing.T, name string, files map[string][]byte) {
	dockerfile := []string{"FROM localhost:5000/nginx"}

	contextDir := filepath.Join(image.WorkDir, "image")
	err := os.MkdirAll(contextDir, 0755)
	require.Nil(t, err)

	for name, data := range files {
		fileName := filepath.Join(contextDir, name)

		err := os.MkdirAll(filepath.Dir(fileName), 0755)
		require.Nil(t, err)

		err = os.WriteFile(fileName, data, 0755)
		require.Nil(t, err)

		dockerfile = append(dockerfile, fmt.Sprintf("ADD %s /%s", name, name))
	}

	err = os.WriteFile(filepath.Join(image.WorkDir, "image", "Dockerfile"), []byte(strings.Join(dockerfile, "\n")), 0660)
	require.Nil(t, err)

	Run(t, fmt.Sprintf("docker build -t %s %s", name, contextDir))
	Run(t, fmt.Sprintf("docker push %s", name))
}
