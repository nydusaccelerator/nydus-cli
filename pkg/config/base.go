package config

type Base struct {
	WorkDir string
	Builder string
	Runtime Runtime
}

type Runtime struct {
	PouchAddr  string
	DockerAddr string
}
