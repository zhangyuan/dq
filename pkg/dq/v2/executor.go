package v2

import (
	"dq/pkg/dq/v2/adapters"
	"dq/pkg/dq/v2/adapters/odps"
	"dq/pkg/dq/v2/db"
	"dq/pkg/dq/v2/spec"
	"os"

	"github.com/jmoiron/sqlx"
)

type Executor struct {
	adapter  *adapters.Adapter
	db       *sqlx.DB
	compiler *Compiler
}

func NewExecutor(adapter *adapters.Adapter, compiler *Compiler) *Executor {
	return &Executor{
		adapter:  adapter,
		compiler: compiler,
	}
}

type Record []interface{}

type Result struct {
	ColumnNames []string `json:"column_names"`
	Records     []Record `json:"records"`
}

func (executor *Executor) ConnectDB() error {
	var db *sqlx.DB
	var err error

	if executor.adapter.Name == odps.Name {
		db, err = odps.NewDB(executor.adapter.DSN)
		if err != nil {
			return err
		}
	}

	if err := db.Ping(); err != nil {
		return err
	}
	executor.db = db

	return nil
}

func (executor *Executor) Close() error {
	if executor.db != nil {
		return executor.db.Close()
	}
	return nil
}

func (executor *Executor) Query(spec *spec.Spec) (*Result, error) {
	sql, err := executor.compiler.ToQuery(spec)
	if err != nil {
		return nil, err
	}

	if executor.adapter.Name == odps.Name {
		sql += ";"
	}

	var result = Result{}

	if err := db.Query(executor.db, sql, func(columNames []string) error {
		return nil
	}, func(columNames []string) error {
		result.ColumnNames = columNames
		return nil
	}, func(values []any) error {
		result.Records = append(result.Records, values)
		return nil
	}); err != nil {
		return nil, err
	}

	return &result, nil
}

func ParseSpec(rulesPath string) (*spec.Spec, error) {
	bytes, err := os.ReadFile(rulesPath)
	if err != nil {
		return nil, err
	}
	spec, err := spec.Parse(bytes, func(*spec.Spec) error {
		return nil
	})

	if err != nil {
		return nil, err
	}
	return spec, nil
}
