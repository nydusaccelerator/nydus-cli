package tool

import (
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

func RunWithCombinedOutput(cmd string) (string, error) {
	_cmd := exec.Command("sh", "-c", cmd)
	output, err := _cmd.CombinedOutput()

	return string(output), err
}

func Run(t *testing.T, cmd string) {
	_cmd := exec.Command("sh", "-c", cmd)
	_cmd.Stdout = os.Stdout
	_cmd.Stderr = os.Stderr
	err := _cmd.Run()
	assert.Nil(t, err)
	assert.Zero(t, _cmd.ProcessState.ExitCode())
}

func RunWithoutOutput(t *testing.T, cmd string) {
	_cmd := exec.Command("sh", "-c", cmd)
	_cmd.Stdout = io.Discard
	_cmd.Stderr = os.Stderr
	err := _cmd.Run()
	assert.Nil(t, err)
}

func RunWithOutput(cmd string) string {
	_cmd := exec.Command("sh", "-c", cmd)
	_cmd.Stderr = os.Stderr

	output, err := _cmd.Output()
	if err != nil {
		panic(err)
	}

	return string(output)
}
