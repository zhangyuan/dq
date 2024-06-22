package spec

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Model struct {
	Table   string   `yaml:"table"`
	Columns []Column `yaml:"columns"`
	Filter  string   `yaml:"filter"`
}

type Column struct {
	Name  string `yaml:"name"`
	Tests []interface{}
}

type Spec struct {
	Tables []Model `yaml:"models"`
}

func Parse[T any](data []byte, validator func(*T) error) (*T, error) {
	var t T
	if err := yaml.Unmarshal(data, &t); err != nil {
		return nil, err
	}

	if err := validator(&t); err != nil {
		return nil, err
	}

	return &t, nil
}

func ParseFromFile[T any](path string, validator func(*T) error) (*T, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse[T](bytes, validator)
}

func LoadRulesFomPath(path string) (*Spec, error) {
	return ParseFromFile(path, func(t *Spec) error { return nil })
}
