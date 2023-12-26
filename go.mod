module github.com/nydusaccelerator/nydus-cli

go 1.20

require (
	github.com/Microsoft/hcsshim v0.11.4
	github.com/aliyun/aliyun-oss-go-sdk v2.2.6+incompatible
	github.com/containerd/containerd v1.7.0-rc.1
	github.com/containerd/continuity v0.4.2
	github.com/containerd/log v0.1.0
	github.com/containerd/nydus-snapshotter v0.7.0
	github.com/docker/cli v23.0.3+incompatible
	github.com/docker/distribution v2.8.1+incompatible
	github.com/docker/engine-api v0.4.0
	github.com/dustin/go-humanize v1.0.1
	github.com/klauspost/compress v1.16.0
	github.com/labstack/echo/v4 v4.11.3
	github.com/moby/buildkit v0.11.3
	github.com/moby/sys/sequential v0.5.0
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.1.0-rc2.0.20221005185240-3a7f492d3f1b
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.8.4
	github.com/urfave/cli/v2 v2.25.0
	github.com/yalp/jsonpath v0.0.0-20180802001716-5cc68e5049a0
	golang.org/x/net v0.17.0
	golang.org/x/sync v0.3.0
	golang.org/x/sys v0.13.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/AdaLogics/go-fuzz-headers v0.0.0-20230811130428-ced1acdcaa24 // indirect
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/containerd/cgroups v1.1.0 // indirect
	github.com/containerd/fifo v1.1.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/docker/docker v23.0.3+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.7.0 // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/labstack/gommon v0.4.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/moby/locker v1.0.1 // indirect
	github.com/moby/sys/mountinfo v0.6.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/otel v1.19.0 // indirect
	go.opentelemetry.io/otel/metric v1.19.0 // indirect
	go.opentelemetry.io/otel/trace v1.19.0 // indirect
	golang.org/x/crypto v0.14.0 // indirect
	golang.org/x/mod v0.11.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	golang.org/x/tools v0.10.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230711160842-782d3b101e98 // indirect
	google.golang.org/grpc v1.58.3 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	gotest.tools/v3 v3.4.0 // indirect
)

replace github.com/containerd/containerd => github.com/nydusaccelerator/containerd v0.0.0-20231212110719-1bceeae1231f
