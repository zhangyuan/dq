package executor

import (
	"dq/pkg/dq/db"
	"dq/pkg/dq/spec"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type Executor struct {
	db     *sqlx.DB
	spec   *spec.Spec
	logger *zap.Logger
}

func NewDB() (*sqlx.DB, error) {
	dsn := os.Getenv("DSN")
	dbDriverName := strings.Split(dsn, ":")[0]
	db, err := db.NewConnection(dbDriverName, dsn)
	if err != nil {
		return nil, err
	}
	return db, err
}

func NewExecutor(rulesConfig *spec.Spec, db *sqlx.DB, logger *zap.Logger) (*Executor, error) {
	return &Executor{
		spec:   rulesConfig,
		db:     db,
		logger: logger,
	}, nil
}

func (executor *Executor) CheckUnique(table string, column string, filter string) *TestResult {
	title := fmt.Sprintf("%s should be unique", column)
	var whereClause string
	if filter != "" {
		whereClause = fmt.Sprintf(" WHERE %s", filter)
	}
	sql := fmt.Sprintf(`SELECT COUNT(*) rows_count, COUNT(DISTINCT %s) distinct_rows_count FROM %s%s`, column, table, whereClause)

	var rowsCount int64
	var distinctRowsCount int64

	executor.logger.Debug("CheckUnique", zap.String("table", table), zap.String("column", column), zap.String("filter", filter))
	executor.logger.Debug("CheckUnique", zap.String("sql", sql))

	if err := executor.db.QueryRowx(sql).Scan(&rowsCount, &distinctRowsCount); err != nil {
		return &TestResult{
			Spec:  "unique",
			Title: title,
			SQL:   sql,
			Error: err,
		}
	}
	return &TestResult{
		Spec:  "unique",
		Title: title,
		SQL:   sql,
		IsOk:  rowsCount == distinctRowsCount,
		Info:  map[string]int64{"rows_count": rowsCount, "distinct_rows_count": distinctRowsCount},
	}
}

func (executor *Executor) CheckNotNull(table string, column string, filter string) *TestResult {
	title := fmt.Sprintf("%s should not be null", column)
	var whereClause string
	if filter != "" {
		whereClause = fmt.Sprintf(" AND %s", filter)
	}
	sql := fmt.Sprintf(`SELECT COUNT(*) rows_count FROM %s WHERE %s IS NULL%s`, table, column, whereClause)

	var rowsCount int64

	executor.logger.Debug("CheckNotNull", zap.String("table", table), zap.String("column", column), zap.String("filter", filter))
	executor.logger.Debug("CheckNotNull", zap.String("sql", sql))

	if err := executor.db.QueryRowx(sql).Scan(&rowsCount); err != nil {
		return &TestResult{
			Title: title,
			SQL:   sql,
			Error: err,
			Spec:  "not_null",
		}
	}
	return &TestResult{
		Title: title,
		SQL:   sql,
		IsOk:  rowsCount == 0,
		Info:  map[string]int64{"rows_count": rowsCount},
		Spec:  "not_null",
	}
}

func (executor *Executor) CheckSQL(sqlTest *SQLTest) *TestResult {
	title := fmt.Sprintf("%s ", sqlTest.Name)
	sql := fmt.Sprintf("SELECT COUNT(*) FROM (%s) a", sqlTest.SQL)

	executor.logger.Debug("CheckNotNull", zap.String("query", sqlTest.SQL))
	executor.logger.Debug("CheckNotNull", zap.String("sql", sql))

	var rowsCount int64

	if err := executor.db.QueryRowx(sql).Scan(&rowsCount); err != nil {
		return &TestResult{
			Title: title,
			SQL:   sql,
			Error: err,
			Spec:  sqlTest,
		}
	}
	return &TestResult{
		Title: title,
		SQL:   sql,
		IsOk:  rowsCount == 0,
		Info:  map[string]int64{"rows_count": rowsCount},
		Spec:  sqlTest,
	}
}

type SQLTest struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}

func (executor *Executor) Execute() *DQReport {
	var modelReports []ModelTestReport
	for _, table := range executor.spec.Tables {
		var columnTestResults []ColumnTestResult
		for _, column := range table.Columns {
			var testResults []TestResult
			for _, test := range column.Tests {
				var testResult *TestResult
				if test == "unique" {
					testResult = executor.CheckUnique(table.Table, column.Name, table.Filter)
				} else if test == "not_null" {
					testResult = executor.CheckNotNull(table.Table, column.Name, table.Filter)
				} else {
					sqlTest := SQLTest{}
					bytes, _ := json.Marshal(test)
					_ = json.Unmarshal(bytes, &sqlTest)
					testResult = executor.CheckSQL(&sqlTest)
				}

				testResults = append(testResults, *testResult)
			}

			columnTestResults = append(columnTestResults, ColumnTestResult{
				Column: column.Name,
				Tests:  testResults,
			})
		}

		modelReports = append(modelReports, ModelTestReport{
			Model:   table.Table,
			Columns: columnTestResults,
		})
	}
	return &DQReport{
		Models: modelReports,
	}
}

func FormatIsOk(isOk bool) string {
	if isOk {
		return "OK"
	} else {
		return "FAILED"
	}
}

type TestResult struct {
	Spec  interface{} `json:"spec"`
	Title string      `json:"title"`
	SQL   string      `json:"sql"`
	Error error       `json:"error,omitempty"`
	IsOk  bool        `json:"is_ok"`
	Info  interface{} `json:"info"`
}

type ColumnTestResult struct {
	Column string       `json:"column"`
	Tests  []TestResult `json:"tests"`
}

type ModelTestReport struct {
	Model   string             `json:"model"`
	Columns []ColumnTestResult `json:"columns"`
}

type DQReport struct {
	Models []ModelTestReport `json:"models"`
}

func Execute(rulesPath string, format string) (bool, error) {
	rulesConfig, err := spec.LoadRulesFomPath(rulesPath)
	if err != nil {
		return false, err
	}

	db, err := NewDB()
	if err != nil {
		return false, err
	}
	defer db.Close()

	var logger *zap.Logger
	if strings.ToLower(os.Getenv("DEBUG")) == "true" {
		logger, _ = zap.NewDevelopment()
	} else {
		logger, _ = zap.NewProduction()
	}
	defer func() {
		_ = logger.Sync()
	}()

	shouldDisplayData := strings.ToLower(os.Getenv("DISPLAY_DATA")) == "true"

	executor, err := NewExecutor(rulesConfig, db, logger)
	if err != nil {
		return false, err
	}

	report := executor.Execute()

	passed := IsPassed(report)

	if format == "json" {
		bytes, _ := json.Marshal(report)
		fmt.Println(string(bytes))
	} else {
		for _, model := range report.Models {
			fmt.Println(model.Model)
			for _, column := range model.Columns {
				for _, test := range column.Tests {
					if shouldDisplayData {
						bytes, _ := json.Marshal(test.Info)
						fmt.Printf("- %s [%s] %s", test.Title, FormatIsOk(test.IsOk), string(bytes))
					} else {
						fmt.Printf("- %s [%s]", test.Title, FormatIsOk(test.IsOk))
					}

					fmt.Println()
				}
			}
		}
		fmt.Println()
	}

	return passed, nil
}

func IsPassed(report *DQReport) bool {
	for _, model := range report.Models {
		for _, column := range model.Columns {
			for _, test := range column.Tests {
				if !test.IsOk {
					return false
				}
			}
		}
	}
	return true
}
