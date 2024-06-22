package v2

import (
	"dq/pkg/dq/v2/spec"
	"dq/pkg/dq/v2/vendors/odps"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
)

type Executor struct {
	dsn      string
	db       *sqlx.DB
	compiler *Compiler
}

func NewExecutor(dsn string) *Executor {
	return &Executor{
		dsn: dsn,
	}
}

type ResultRow struct {
	IsFailed string
	IsOk     string
}

func (executor *Executor) Execute(rulesPath string, format string) (bool, error) {
	sql, err := executor.GenerateSQL(rulesPath, format)
	if err != nil {
		return false, err
	}

	var resultRows []ResultRow
	rows, err := executor.db.Queryx(sql)
	if err != nil {
		for rows.Next() {
			var resultRow ResultRow
			if err := rows.StructScan(&resultRow); err != nil {
				return false, nil
			}
			resultRows = append(resultRows, resultRow)
		}
	}

	fmt.Println(resultRows)

	return true, nil
}

func (executor *Executor) GenerateSQL(rulesPath string, format string) (string, error) {
	bytes, err := os.ReadFile(rulesPath)
	if err != nil {
		return "", err
	}
	spec, err := spec.Parse(bytes, func(*spec.Spec) error {
		return nil
	})

	if err != nil {
		return "", err
	}

	if strings.Contains(executor.dsn, "maxcompute") {
		templates := odps.OdpsTemplates{}
		compiler := NewCompiler(templates)
		return compiler.Compile(spec)
	} else {
		return "", errors.New("invalid vendor")
	}
}
