package spec

import (
	"os"

	"gopkg.in/yaml.v3"
)

type InsertMode struct {
	Overwrite bool
	Partition string
}

type ResultTable struct {
	Name          string
	Type          string
	PartitionedBy string
	InsertMode    InsertMode
}

type Expect struct {
	GT  *int
	GTE *int
	LT  *int
	LTE *int
	EQ  *int
}

type Rule struct {
	Name        string
	Validator   string
	Columns     []string
	Filter      string
	ExtraFilter string
	Query       string
	Expect      Expect
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
