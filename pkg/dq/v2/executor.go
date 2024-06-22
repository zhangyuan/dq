package v2

import (
	"dq/pkg/dq/v2/db"
	"dq/pkg/dq/v2/spec"
	"dq/pkg/dq/v2/vendors/odps"
	"errors"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
)

type Executor struct {
	dsn      string
	db       *sqlx.DB
	compiler *Compiler
}

func NewExecutor(dsn string, compiler *Compiler) *Executor {
	return &Executor{
		dsn:      dsn,
		compiler: compiler,
	}
}

type Record []interface{}

type Result struct {
	ColumnNames []string `json:"column_names"`
	Records     []Record `json:"records"`
}

func (executor *Executor) ConnectDB() error {
	if IsOdps(executor.dsn) {
		db, err := odps.NewDB(executor.dsn)
		if err != nil {
			return err
		}
		if err := db.Ping(); err != nil {
			return err
		}
		executor.db = db
	} else {
		return errors.New("not supported")
	}

	return nil
}

func (executor *Executor) Close() error {
	if executor.db != nil {
		return executor.db.Close()
	}
	return nil
}

func IsOdps(dsn string) bool {
	return strings.Contains(dsn, "maxcompute")
}

func (executor *Executor) Query(spec *spec.Spec) (*Result, error) {
	sql, err := executor.compiler.ToQuery(spec)
	if err != nil {
		return nil, err
	}

	if IsOdps(executor.dsn) {
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
