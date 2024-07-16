package v2

import (
	"dq/pkg/dq/v2/adapters"
	"dq/pkg/dq/v2/adapters/odps"
	"dq/pkg/dq/v2/spec"
	"dq/pkg/dq/v2/templates/simple"
	"time"

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

type ResultRecord struct {
	ProcTime  time.Time `db:"proc_time"`
	TableName string    `db:"table_name"`
	RuleName  string    `db:"rule_name"`
	Validator string    `db:"validator"`
	Context   string    `db:"context"`
	IsFailed  int       `db:"is_failed"`
	IsOk      int       `db:"is_ok"`
	Value     int       `db:"value"`
}

type Result struct {
	ColumnNames []string       `json:"column_names"`
	Records     []ResultRecord `json:"records"`
	IsOk        bool
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

func (executor *Executor) Query(spec *spec.Spec, params *map[string]any) (*Result, error) {
	sql, err := executor.compiler.ToQuery(spec, params)
	if err != nil {
		return nil, err
	}

	// odps requires semicolon.
	if executor.adapter.Name == odps.Name {
		sql += ";"
	}

	var result = Result{
		IsOk: true,
	}

	var resultRows []ResultRecord

	sql, err = simple.Compile(sql, *params)
	if err != nil {
		return nil, err
	}

	if err := executor.db.Select(&resultRows, sql); err != nil {
		return nil, err
	}

	for idx := range resultRows {
		if resultRows[idx].IsFailed == 1 {
			result.IsOk = false
			break
		}
	}
	result.Records = resultRows
	return &result, nil
}

func ParseSpec(rulesPath string) (*spec.Spec, error) {
	return spec.ParseFromFile(rulesPath, func(*spec.Spec) error { return nil })
}
