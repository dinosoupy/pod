package pod

import (
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

type Datasource struct {
	Name   string            `yaml:"name"`
	Type   string            `yaml:"type"`
	URL    string            `yaml:"url"`
	Fields map[string]string `yaml:"fields"`
}

type Preprocessor struct {
	Filename string `yaml:"filename"`
	Type     string `yaml:"type"`
}

type Model struct {
	Name     string `yaml:"name"`
	Filename string `yaml:"filename"`
}

type Endpoint struct {
	Name   string            `yaml:"name"`
	Type   string            `yaml:"type"`
	URL    string            `yaml:"url"`
	Fields map[string]string `yaml:"fields"`
}

type Pod struct {
	Name         string       `yaml:"name"`
	Version      string       `yaml:"version"`
	Datasource   Datasource   `yaml:"datasource"`
	Preprocessor Preprocessor `yaml:"preprocessor"`
	Model        Model        `yaml:"model"`
	Endpoint     Endpoint     `yaml:"endpoint"`
}

// New initializes a Pod using the given config filename
func New(filename string) (*Pod, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error: while opening %s\n%v", filename, err)
	}
	defer file.Close()

	b, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("error: while reading %s\n%v", filename, err)
	}

	var pod Pod
	if err = yaml.Unmarshal(b, &pod); err != nil {
		return nil, fmt.Errorf("error: while parsing %s\n%v", filename, err)
	}

	return &pod, nil
}
