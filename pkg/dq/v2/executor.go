package v2

import (
	"dq/pkg/dq/v2/spec"
	"dq/pkg/dq/v2/vendors/odps"
	"errors"
	"os"
	"strings"
	"time"

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

type ResultRow struct {
	ProcTime  time.Time `json:"proc_time" db:"proc_time"`
	TableName string    `json:"table_name" db:"table_name"`
	RuleName  string    `json:"rule_name" db:"rule_name"`
	Validator string    `json:"validator" db:"validator"`
	IsFailed  string    `json:"is_failed" db:"is_failed"`
	IsOk      string    `json:"is_ok" db:"is_ok"`
	Value     int64     `json:"value" db:"value"`
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

func (executor *Executor) Query(spec *spec.Spec) ([]ResultRow, error) {
	sql, err := executor.compiler.ToQuery(spec)
	if err != nil {
		return nil, err
	}

	if IsOdps(executor.dsn) {
		sql += ";"
	}

	var resultRows []ResultRow
	rows, err := executor.db.Queryx(sql)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var resultRow ResultRow
		if err := rows.StructScan(&resultRow); err != nil {
			return nil, err
		}
		resultRows = append(resultRows, resultRow)
	}

	return resultRows, nil
}

// func (executor *Executor) GenerateSingleQuery(rulesPath string, format string) (string, error) {
// 	statements, err := executor.GenerateQueries(rulesPath, format)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return nil, nil
// }

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
