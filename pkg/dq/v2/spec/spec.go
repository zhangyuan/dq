package spec

import (
	"os"

	"gopkg.in/yaml.v3"
)

type InsertMode struct {
	Partition string
	Overwrite bool
}

type ResultTable struct {
	Name          string
	Type          string
	PartitionedBy string
	InsertMode    InsertMode
}

type Expect struct {
	GT  *int `json:"GT,omitempty"`
	GTE *int `json:"GTE,omitempty"`
	LT  *int `json:"LT,omitempty"`
	LTE *int `json:"LTE,omitempty"`
	EQ  *int `json:"EQ,omitempty"`
}

type Rule struct {
	Expect      Expect
	Name        string
	Validator   string
	Filter      string
	ExtraFilter string
	Query       string
	Columns     []string
	Column      string
}

type Model struct {
	Table  string
	Filter string
	Rules  []Rule
}

type Spec struct {
	Version string
	Models  []Model
}

func Parse[T any](data []byte, specValidator func(*T) error) (*T, error) {
	var t T
	if err := yaml.Unmarshal(data, &t); err != nil {
		return nil, err
	}

	if err := specValidator(&t); err != nil {
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
