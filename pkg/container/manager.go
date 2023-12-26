package container

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/nydusaccelerator/nydus-cli/pkg/config"
	"github.com/nydusaccelerator/nydus-cli/pkg/distribution"

	"github.com/docker/distribution/reference"
	"github.com/docker/engine-api/client"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/yalp/jsonpath"
)

type InspectResult struct {
	LowerDirs string
	UpperDir  string
	Image     string
	Mounts    []Mount
	Pid       int
}

type Manager struct {
	cfg *config.Runtime
}

type EngineType string

const (
	EngineUnknown EngineType = "unknown"
	EngineDocker  EngineType = "docker"
	EnginePouch   EngineType = "pouch"
)

type Mount struct {
	Destination string
	Source      string
}

// parseID returns engine type (pouch/docker) and container id.
func parseID(containerID string) (EngineType, string, error) {
	ids := strings.Split(containerID, "://")
	if len(ids) == 1 {
		return EngineUnknown, ids[0], nil
	}
	if len(ids) == 2 {
		return EngineType(ids[0]), ids[1], nil
	}
	return "", "", fmt.Errorf("invalid container id format: %s", containerID)
}

// parseDir returns dir path in struct data by json path.
func parseDir(data interface{}, path string) (string, error) {
	ret, err := jsonpath.Read(data, path)
	if err != nil {
		return "", errors.Wrapf(err, "find json path '%s'", path)
	}

	dir, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("invalid json path value %s", path)
	}

	if dir == "" {
		return "", fmt.Errorf("invalid dir string")
	}

	info, err := os.Stat(dir)
	if err != nil {
		return "", errors.Wrapf(err, "stat path %s", dir)
	}

	if !info.IsDir() {
		return "", errors.Wrapf(err, "path %s is not a directory", dir)
	}

	return dir, nil
}

func NewManager(cfg *config.Runtime) (*Manager, error) {
	return &Manager{
		cfg: cfg,
	}, nil
}

func (m *Manager) getEngineAddr(engineType EngineType) (string, error) {
	switch engineType {
	case EngineDocker:
		return m.cfg.DockerAddr, nil
	case EnginePouch:
		return m.cfg.PouchAddr, nil
	default:
		return "", fmt.Errorf("invalid engine type: %s", engineType)
	}
}

func (m *Manager) createClient(ctx context.Context, containerIDWithType string) (EngineType, string, *client.Client, error) {
	engineType, containerID, err := parseID(containerIDWithType)
	if err != nil {
		return "", "", nil, errors.Wrap(err, "parse container id")
	}

	addr, err := m.getEngineAddr(engineType)
	if err != nil {
		return "", "", nil, errors.Wrap(err, "parse engine type")
	}

	client, err := client.NewClient("unix://"+addr, "", nil, nil)
	if err != nil {
		return "", "", nil, errors.Wrapf(err, "connect to pouch/docker on %s", addr)
	}

	return engineType, containerID, client, nil
}

func (m *Manager) Pause(ctx context.Context, containerIDWithType string) error {
	_, containerID, client, err := m.createClient(ctx, containerIDWithType)
	if err != nil {
		return errors.Wrapf(err, "create client")
	}

	return client.ContainerPause(ctx, containerID)
}

func (m *Manager) UnPause(ctx context.Context, containerIDWithType string) error {
	_, containerID, client, err := m.createClient(ctx, containerIDWithType)
	if err != nil {
		return errors.Wrapf(err, "create client")
	}

	return client.ContainerUnpause(ctx, containerID)
}

func (m *Manager) inspectImage(ctx context.Context, data interface{}, jsonPath string) (string, error) {
	_image, err := jsonpath.Read(data, jsonPath)
	if err != nil {
		return "", errors.Wrapf(err, "find json path '%s'", jsonPath)
	}

	image, ok := _image.(string)
	if !ok {
		return "", fmt.Errorf("image name should be string")
	}
	if image == "" {
		return "", fmt.Errorf("empty image name")
	}

	if _, err := reference.ParseNormalizedNamed(image); err != nil {
		return "", errors.Wrapf(err, "invalid image name '%s'", image)
	}

	return image, nil
}

func (m *Manager) Inspect(ctx context.Context, containerIDWithType string) (*InspectResult, error) {
	engineType, containerID, client, err := m.createClient(ctx, containerIDWithType)
	if err != nil {
		return nil, errors.Wrapf(err, "create client")
	}

	_, bytes, err := client.ContainerInspectWithRaw(ctx, containerID, false)
	if err != nil {
		return nil, errors.Wrapf(err, "inspect container")
	}

	var data interface{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		return nil, errors.Wrapf(err, "unmarshal json")
	}

	lowerDirs := ""
	if engineType != EngineDocker {
		_lowerDirs, err := jsonpath.Read(data, "$.GraphDriver.Data.LowerDir")
		if err != nil {
			return nil, errors.Wrapf(err, "find json path '$.GraphDriver.Data.LowerDir'")
		}
		var ok bool
		lowerDirs, ok = _lowerDirs.(string)
		if !ok {
			return nil, fmt.Errorf("invalid lower dir string")
		}
	} else {
		mergedDir, err := jsonpath.Read(data, "$.GraphDriver.Data.MergedDir")
		if err != nil {
			return nil, errors.Wrapf(err, "find json path '$.GraphDriver.Data.MergedDir'")
		}
		_lowerDirs, err := GetLowerDirs(mergedDir.(string))
		if err != nil {
			return nil, errors.Wrapf(err, "get lower dirs for docker")
		}
		lowerDirs = strings.Join(_lowerDirs, ",")
	}

	logrus.Info("container lower dirs: ", lowerDirs)

	upperDir, err := parseDir(data, "$.GraphDriver.Data.UpperDir")
	if err != nil {
		return nil, errors.Wrapf(err, "parse upper dir")
	}

	jsonPath := "$.Config.Labels[\"io.kubernetes.container.image\"]"
	image, err := m.inspectImage(ctx, data, jsonPath)
	if err != nil {
		jsonPath = "$.Config.Image"
		logrus.Warnf("failed to inspect image: %s, retry json path '%s'", err.Error(), jsonPath)
		image, err = m.inspectImage(ctx, data, jsonPath)
		if err != nil {
			return nil, errors.Wrapf(err, "inspect container image name")
		}
	}
	hasNydusSuffix, err := distribution.HasNydusSuffix(image)
	if err != nil {
		return nil, errors.Wrapf(err, "check nydus image name '%s'", image)
	}
	if !hasNydusSuffix {
		return nil, fmt.Errorf("invalid nydus image name '%s'", image)
	}

	_mounts, err := jsonpath.Read(data, "$.Mounts")
	if err != nil {
		return nil, errors.Wrapf(err, "find json path '%s'", "$.Mounts")
	}
	mounts := []Mount{}
	for _, mount := range _mounts.([]interface{}) {
		value := mount.(map[string]interface{})
		mounts = append(mounts, Mount{
			Destination: value["Destination"].(string),
			Source:      value["Source"].(string),
		})
	}

	_pid, err := jsonpath.Read(data, "$.State.Pid")
	if err != nil {
		return nil, errors.Wrapf(err, "find json path '%s'", "$.State.Pid")
	}
	pid := int(_pid.(float64))

	return &InspectResult{
		LowerDirs: lowerDirs,
		UpperDir:  upperDir,
		Image:     image,
		Mounts:    mounts,
		Pid:       pid,
	}, nil
}
