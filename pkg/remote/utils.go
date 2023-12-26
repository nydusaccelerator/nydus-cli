package remote

import (
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const defaultRetryAttempts = 3
const defaultRetryInterval = time.Second * 2

// IsErrHTTPResponseToHTTPSClient returns whether err is
// "http: server gave HTTP response to HTTPS client"
func isErrHTTPResponseToHTTPSClient(err error) bool {
	// The error string is unexposed as of Go 1.16, so we can't use `errors.Is`.
	// https://github.com/golang/go/issues/44855
	const unexposed = "server gave HTTP response to HTTPS client"
	return strings.Contains(err.Error(), unexposed)
}

// IsErrConnectionRefused return whether err is
// "connect: connection refused"
func isErrConnectionRefused(err error) bool {
	const errMessage = "connect: connection refused"
	return strings.Contains(err.Error(), errMessage)
}

func WithRetry(op func() error) error {
	var err error
	attempts := defaultRetryAttempts
	for attempts > 0 {
		attempts--
		if err != nil {
			if RetryWithHTTP(err) {
				return err
			}
			logrus.Warnf("Retry due to error: %s", err)
			time.Sleep(defaultRetryInterval)
		}
		if err = op(); err == nil {
			break
		}
	}
	return err
}

func RetryWithHTTP(err error) bool {
	return err != nil && (isErrHTTPResponseToHTTPSClient(err) || isErrConnectionRefused(err))
}
