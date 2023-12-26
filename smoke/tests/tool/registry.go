package tool

import (
	"fmt"
	"net/http"
	"testing"

	echo "github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

var manifestJSON = `
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {
    "mediaType": "application/vnd.oci.image.config.v1+json",
    "digest": "sha256:bd6fb18be1945dbc70f7e36b44a83a25dca3fc71491d8d5b213e975d1fb60627",
    "size": 7209
  },
  "layers": [
    {
      "mediaType": "application/vnd.oci.image.layer.nydus.blob.v1",
      "digest": "sha256:e4d7b5e0b0ad896b29de0b1fd5954be5c688b2b76301e6d1975b01f07c390151",
      "size": 41186796,
      "annotations": {
        "containerd.io/snapshot/nydus-blob": "true"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.layer.nydus.blob.v1",
      "digest": "sha256:d9efbe95fd13541cb7ad8f0796351998c6fef9d2acd18701bcad4d7ae383e9cf",
      "size": 51572846,
      "annotations": {
        "containerd.io/snapshot/nydus-blob": "true"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.layer.nydus.blob.v1",
      "digest": "sha256:ca7ed1c94ddcb58a96b9769b21cbeec57b4cf7ecba78f3b48ebb673f69ca8831",
      "size": 10483,
      "annotations": {
        "containerd.io/snapshot/nydus-blob": "true"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.layer.nydus.blob.v1",
      "digest": "sha256:83e5c09d3f9f76238a007cc09247ece1db2621921a17ce30db02630af761327a",
      "size": 10990,
      "annotations": {
        "containerd.io/snapshot/nydus-blob": "true"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.layer.nydus.blob.v1",
      "digest": "sha256:b057f5fdd3a216e824bb972021847e2fff3af14d111ad754ad7f6caea8ad621d",
      "size": 10145,
      "annotations": {
        "containerd.io/snapshot/nydus-blob": "true"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.layer.nydus.blob.v1",
      "digest": "sha256:ac2899bbd7cb95da76ad9b9eff1222f799edbc9881fb22fbc6ccaca90d08970c",
      "size": 11419,
      "annotations": {
        "containerd.io/snapshot/nydus-blob": "true"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.layer.nydus.blob.v1",
      "digest": "sha256:5b66258d24e6d6aa38ffa6a5b2c7e9ee237ad11ef4fdd2ef0234eee80212a6b3",
      "size": 11835,
      "annotations": {
        "containerd.io/snapshot/nydus-blob": "true"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
      "digest": "sha256:a630d93cbee4bd283296802f2c57288fa07b33b1681cb790bad122b6d08b6d26",
      "size": 502993,
      "annotations": {
        "containerd.io/snapshot/nydus-bootstrap": "true",
        "containerd.io/snapshot/nydus-fs-version": "5"
      }
    }
  ],
  "annotations": {
    "containerd.io/snapshot/nydus-builder-version": "v2.2.4",
    "containerd.io/snapshot/nydus-fs-version": "5",
    "containerd.io/snapshot/nydus-source-digest": "sha256:10d1f5b58f74683ad34eb29287e07dab1e90f10af243f151bb50aa5dbb4d62ee",
    "containerd.io/snapshot/nydus-source-reference": "docker.io/library/nginx:latest"
  }
}
`

type Registry struct {
}

func (registry *Registry) Serve(t *testing.T) {
	e := echo.New()

	dgst := digest.FromBytes([]byte(manifestJSON))

	e.HEAD("/v2/nginx/manifests/latest_nydus_v2", func(c echo.Context) error {
		return c.JSON(http.StatusOK, []byte(``))
	})

	e.GET("/v2/nginx/manifests/latest_nydus_v2", func(c echo.Context) error {
		c.Response().Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
		return c.JSON(http.StatusOK, []byte(manifestJSON))
	})

	e.GET(fmt.Sprintf("/v2/nginx/manifests/%s", dgst), func(c echo.Context) error {
		c.Response().Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
		return c.JSONBlob(http.StatusOK, []byte(manifestJSON))
	})

	e.GET("/v2/nginx/blobs/:digest", func(c echo.Context) error {
		switch c.Param("digest") {
		case "sha256:a630d93cbee4bd283296802f2c57288fa07b33b1681cb790bad122b6d08b6d26":
			return c.File("./smoke/tests/texture/base/nginx.bootstrap.tar.gz")
		default:
			return c.JSONBlob(http.StatusOK, []byte(`{}`))
		}
	})

	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "method=${method}, uri=${uri}, status=${status}\n",
	}))

	err := e.Start(":5432")
	require.Nil(t, err)
}

func (registry *Registry) Destory(t *testing.T) {
}
