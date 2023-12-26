package workflow

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/nydusaccelerator/nydus-cli/pkg/nsenter"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Counter struct {
	n int64
}

func (c *Counter) Write(p []byte) (n int, err error) {
	atomic.AddInt64(&c.n, int64(len(p)))
	return len(p), nil
}

func (c *Counter) Size() (n int64) {
	return c.n
}

func copyFromContainer(ctx context.Context, containerPid int, source string, target io.Writer) error {
	config := &nsenter.Config{
		Mount:  true,
		Target: containerPid,
	}

	stderr, err := config.ExecuteContext(ctx, target, "tar", "--xattrs", "--ignore-failed-read", "--absolute-names", "-cf", "-", source)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("execute tar: %s", strings.TrimSpace(stderr)))
	}
	if stderr != "" {
		logrus.Warnf("from container: %s", stderr)
	}

	return nil
}
