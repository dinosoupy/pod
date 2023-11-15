package pod

import (
	"context"
	"fmt"
	"io"
	"os"

	kafka "github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
)

type Datasource interface {
	Fetch() ([]byte, error)
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
	Name         string
	Version      string
	Datasource   Datasource
	Preprocessor Preprocessor
	Model        Model
	Endpoint     Endpoint
}

// Yaml file is unmarshalled into Config before creating a pod
type Config struct {
	Name       string `yaml:"name"`
	Version    string `yaml:"version"`
	Datasource struct {
		Name    string   `yaml:"name"`
		Type    string   `yaml:"type"`
		Brokers []string `yaml:"brokers"`
		Topic   string   `yaml:"topic"`
	} `yaml:"datasource"`
	Preprocessor struct {
		Filename string `yaml:"filename"`
		Type     string `yaml:"type"`
	} `yaml:"preprocessor"`
	Model struct {
		Name     string `yaml:"name"`
		Filename string `yaml:"filename"`
	} `yaml:"model"`
	Endpoint struct {
		Name   string            `yaml:"name"`
		Type   string            `yaml:"type"`
		URL    string            `yaml:"url"`
		Fields map[string]string `yaml:"fields"`
	} `yaml:"endpoint"`
}

// KafkaReader is a wrapper around kafka.Reader that extends
// a Fetch() method to implement the Datasource interface
type KafkaReader struct {
	reader *kafka.Reader
}

func (k KafkaReader) Fetch() ([]byte, error) {
	m, err := k.reader.ReadMessage(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error: while fetching from kafka topic\n%v", err)
	}
	return m.Value, nil
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

	var config Config
	if err = yaml.Unmarshal(b, &config); err != nil {
		return nil, fmt.Errorf("error: while parsing %s\n%v", filename, err)
	}

	var datasource Datasource
	switch config.Datasource.Type {
	case "kafka":
		kr := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  config.Datasource.Brokers,
			Topic:    config.Datasource.Topic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		})
		datasource = &KafkaReader{reader: kr}
	}

	pod := &Pod{
		Name:       config.Name,
		Version:    config.Version,
		Datasource: datasource,
	}

	return pod, nil
}
