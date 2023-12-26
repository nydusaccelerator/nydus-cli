package config

import (
	"os"

	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

type Config struct {
	// From config file
	Distribution Distribution `yaml:"distribution"`
	OSS          OSS          `yaml:"oss"`

	// From CLI flags
	Base Base
}

type OSS struct {
	Endpoint        string `yaml:"endpoint"`
	AccessKeyID     string `yaml:"access_key_id"`
	AccessKeySecret string `yaml:"access_key_secret"`
	BucketName      string `yaml:"bucket_name"`
	ObjectPrefix    string `yaml:"object_prefix"`
}

type Distribution struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func Parse(c *cli.Context, configPath string) (*Config, error) {
	bytes, err := os.ReadFile(configPath)
	if err != nil {
		return nil, errors.Wrapf(err, "load config: %s", configPath)
	}

	var cfg Config
	if err := yaml.Unmarshal(bytes, &cfg); err != nil {
		return nil, errors.Wrapf(err, "parse config: %s", configPath)
	}

	cfg.Base.WorkDir = c.String("workdir")
	cfg.Base.Builder = c.String("builder")
	cfg.Base.Runtime = Runtime{
		PouchAddr:  c.String("pouch.addr"),
		DockerAddr: c.String("docker.addr"),
	}

	return &cfg, nil
}
