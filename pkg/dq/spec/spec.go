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

func LoadRules(data []byte) (*Spec, error) {
	rulesConfig := Spec{}
	if err := yaml.Unmarshal(data, &rulesConfig); err != nil {
		return nil, err
	}

	if err := Validate(&rulesConfig); err != nil {
		return nil, err
	}

	return &rulesConfig, nil
}

func LoadRulesFomPath(path string) (*Spec, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return LoadRules(bytes)
}

func Validate(rulesConfig *Spec) error {
	return nil
}
