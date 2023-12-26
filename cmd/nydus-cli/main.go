package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nydusaccelerator/nydus-cli/pkg/config"
	"github.com/nydusaccelerator/nydus-cli/pkg/workflow"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var revision string
var buildTime string

func main() {
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	})

	version := fmt.Sprintf("%s.%s", revision, buildTime)
	logrus.Infof("version %s\n", version)

	printOption := func(c *cli.Context, options []string) {
		logrus.Infof("options:")
		for _, option := range options {
			if c.String(option) != "" {
				logrus.Infof("\t%s: %s", option, c.String(option))
			}
		}
	}

	parsePaths := func(c *cli.Context, paths []string) ([]string, []string) {
		withPaths := []string{}
		withoutPaths := []string{}

		for _, path := range paths {
			path = strings.TrimSpace(path)
			if strings.HasPrefix(path, "!") {
				path = strings.TrimLeft(path, "!")
				path = strings.TrimRight(path, "/")
				withoutPaths = append(withoutPaths, path)
			} else {
				withPaths = append(withPaths, path)
			}
		}

		return withPaths, withoutPaths
	}

	app := &cli.App{
		Name:    "nydus-cli",
		Usage:   "Nydus utility tool to operate nydus image",
		Version: version,
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: "log-level", Value: "info", Usage: "Set the logging level [trace, debug, info, warn, error, fatal, panic]"},
		&cli.StringFlag{
			Name:    "config",
			Usage:   "Path to configuration file",
			EnvVars: []string{"CONFIG"},
		},
	}

	baseFlags := []cli.Flag{
		&cli.StringFlag{
			Name:        "workdir",
			Required:    false,
			DefaultText: "/tmp",
			Value:       "/tmp",
		},
		&cli.StringFlag{
			Name:        "builder",
			Required:    false,
			DefaultText: "nydus-image",
			Value:       "nydus-image",
		},
		&cli.StringFlag{
			Name:        "pouch.addr",
			Required:    false,
			DefaultText: "/var/run/pouchd.sock",
			Value:       "/var/run/pouchd.sock",
		},
		&cli.StringFlag{
			Name:        "docker.addr",
			Required:    false,
			DefaultText: "/var/run/docker.sock",
			Value:       "/var/run/docker.sock",
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:  "commit",
			Usage: "Commit a container into nydus image based a nydus image",
			Flags: append([]cli.Flag{
				&cli.StringFlag{
					Name:     "container",
					Required: true,
					Usage:    "Target container id",
					EnvVars:  []string{"CONTAINER"},
				},
				&cli.StringFlag{
					Name:     "target",
					Required: true,
					Usage:    "Target nydus image reference",
					EnvVars:  []string{"TARGET"},
				},
				&cli.BoolFlag{
					Name:     "pause-container",
					Required: false,
					Usage:    "Pause container during commit",
					EnvVars:  []string{"PAUSE_CONTAINER"},
				},
				&cli.IntFlag{
					Name:        "maximum-times",
					Required:    false,
					DefaultText: "400",
					Value:       400,
					Usage:       "The maximum times allowed to be committed",
					EnvVars:     []string{"MAXIMUM_TIMES"},
				},
				&cli.StringSliceFlag{
					Name:     "with-path",
					Aliases:  []string{"with-mount-path"},
					Required: false,
					Usage:    "The directory that need to be committed",
					EnvVars:  []string{"WITH_PATH"},
				},
			}, baseFlags...),
			Action: func(c *cli.Context) error {
				cfg, err := config.Parse(c, c.String("config"))
				if err != nil {
					return errors.Wrap(err, "parse config file")
				}

				wf, err := workflow.NewWorkflow(cfg)
				if err != nil {
					return errors.Wrap(err, "create workflow")
				}
				defer wf.Destory() //nolint:errcheck

				printOption(c, []string{"container", "target", "with-path", "maximum-times"})
				withPaths, withoutPaths := parsePaths(c, c.StringSlice("with-path"))

				return wf.Commit(c.Context, workflow.CommitOption{
					ContainerIDWithType: c.String("container"),
					TargetRef:           c.String("target"),
					WithPaths:           withPaths,
					WithoutPaths:        withoutPaths,
					PauseContainer:      c.Bool("pause-container"),
					MaximumTimes:        c.Int("maximum-times"),
				})
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		logrus.Fatal(err)
	}
}
