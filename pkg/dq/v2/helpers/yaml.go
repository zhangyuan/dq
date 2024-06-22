package helpers

import (
	"os"

	"gopkg.in/yaml.v3"
)

func ParseYAML[T any](data []byte) (*T, error) {
	var t T
	if err := yaml.Unmarshal(data, &t); err != nil {
		return nil, err
	}

	return &t, nil
}

func ParseYAMLFromFile[T any](path string) (*T, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseYAML[T](bytes)
}
