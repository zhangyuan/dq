package v2

import (
	"dq/pkg/dq/v2/adapters"
	"dq/pkg/dq/v2/adapters/odps"
	"dq/pkg/dq/v2/spec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewOdpsAdapter(dsn string) *adapters.Adapter {
	return &adapters.Adapter{
		Name:      odps.Name,
		DSN:       dsn,
		Templates: odps.OdpsTemplates{},
	}
}

func TestCompileRowCountRule(t *testing.T) {
	compiler := NewCompiler(NewOdpsAdapter("dummy"))
	model := spec.Model{Table: "orders", Filter: "deleted = false"}
	gtValue := 0
	rule := spec.Rule{
		Name:      "table should not be empty",
		Validator: "rows_count",
		Expect: spec.Expect{
			GT: &gtValue,
		},
	}

	expected := strings.TrimSpace(`
WITH result AS (
  SELECT COUNT(*) AS value FROM orders WHERE deleted = false
)
SELECT
  GETDATE() AS proc_time,
  IF(value > 0, 0, 1) is_failed,
  IF(value > 0, 1, 0) is_ok,
  "orders" AS table_name,
  "table should not be empty" AS rule_name,
  "rows_count" AS validator,
  '{"Expect":{"GT":0},"Filter":"deleted = false","Validator":"rows_count"}' AS context,
  value AS value
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileRowCountRuleWithExtraFilter(t *testing.T) {
	compiler := NewCompiler(NewOdpsAdapter("dummy"))
	model := spec.Model{Table: "orders", Filter: "deleted = false"}
	gtValue := 0
	rule := spec.Rule{
		Name:        "table should not be empty",
		Validator:   "rows_count",
		ExtraFilter: "type IS NOT NULL",
		Expect: spec.Expect{
			GT: &gtValue,
		},
	}

	expected := strings.TrimSpace(`
WITH result AS (
  SELECT COUNT(*) AS value FROM orders WHERE deleted = false AND type IS NOT NULL
)
SELECT
  GETDATE() AS proc_time,
  IF(value > 0, 0, 1) is_failed,
  IF(value > 0, 1, 0) is_ok,
  "orders" AS table_name,
  "table should not be empty" AS rule_name,
  "rows_count" AS validator,
  '{"Expect":{"GT":0},"Filter":"deleted = false AND type IS NOT NULL","Validator":"rows_count"}' AS context,
  value AS value
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileRowCountRuleWithFilterOverwritten(t *testing.T) {
	compiler := NewCompiler(NewOdpsAdapter("dummy"))
	model := spec.Model{Table: "orders", Filter: "deleted = false"}
	gtValue := 0
	rule := spec.Rule{
		Name:      "table should not be empty",
		Validator: "rows_count",
		Filter:    "1=1",
		Expect: spec.Expect{
			GT: &gtValue,
		},
	}

	expected := strings.TrimSpace(`
WITH result AS (
  SELECT COUNT(*) AS value FROM orders WHERE 1=1
)
SELECT
  GETDATE() AS proc_time,
  IF(value > 0, 0, 1) is_failed,
  IF(value > 0, 1, 0) is_ok,
  "orders" AS table_name,
  "table should not be empty" AS rule_name,
  "rows_count" AS validator,
  '{"Expect":{"GT":0},"Filter":"1=1","Validator":"rows_count"}' AS context,
  value AS value
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileDuplicatesRule(t *testing.T) {
	compiler := NewCompiler(NewOdpsAdapter("dummy"))

	model := spec.Model{Table: "orders", Filter: "deleted = false"}
	expectValue := 0
	rule := spec.Rule{
		Name:      "order_no should be unique",
		Validator: "duplicates",
		Columns:   []string{"order_no"},
		Expect: spec.Expect{
			EQ: &expectValue,
		},
	}

	expected := strings.TrimSpace(`
WITH query AS (
  SELECT order_no FROM orders WHERE deleted = false
  GROUP BY order_no
  HAVING COUNT(*) > 1
), result AS (
  SELECT COUNT(*) AS value FROM query
)
SELECT
  GETDATE() AS proc_time,
  IF(value = 0, 0, 1) is_failed,
  IF(value = 0, 1, 0) is_ok,
  "orders" AS table_name,
  "order_no should be unique" AS rule_name,
  "duplicates" AS validator,
  '{"Columns":["order_no"],"Expect":{"EQ":0},"Filter":"deleted = false","Validator":"duplicates"}' AS context,
  value AS value
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileDuplicatesRuleGivenMultileColumns(t *testing.T) {
	compiler := NewCompiler(NewOdpsAdapter("dummy"))

	model := spec.Model{Table: "work_orders", Filter: "deleted = false"}
	expectValue := 0
	rule := spec.Rule{
		Name:      "order_no, production_date should be unique",
		Validator: "duplicates",
		Columns:   []string{"order_no", "production_date"},
		Expect: spec.Expect{
			EQ: &expectValue,
		},
	}

	expected := strings.TrimSpace(`
WITH query AS (
  SELECT order_no, production_date FROM work_orders WHERE deleted = false
  GROUP BY order_no, production_date
  HAVING COUNT(*) > 1
), result AS (
  SELECT COUNT(*) AS value FROM query
)
SELECT
  GETDATE() AS proc_time,
  IF(value = 0, 0, 1) is_failed,
  IF(value = 0, 1, 0) is_ok,
  "work_orders" AS table_name,
  "order_no, production_date should be unique" AS rule_name,
  "duplicates" AS validator,
  '{"Columns":["order_no","production_date"],"Expect":{"EQ":0},"Filter":"deleted = false","Validator":"duplicates"}' AS context,
  value AS value
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileSqlRule(t *testing.T) {
	compiler := NewCompiler(NewOdpsAdapter("dummy"))

	model := spec.Model{Table: "work_orders", Filter: "deleted = false"}
	expectValue := 0
	rule := spec.Rule{
		Name:      "status should be valid",
		Validator: "sql",
		Query: strings.TrimSpace(`
SELECT count(*) AS value from work_orders WHERE status NOT IN (SELECT * FROM VALUES ('CREATED'), ('COMPLELTED') AS t(status))
		`),
		Expect: spec.Expect{
			EQ: &expectValue,
		},
	}

	expected := strings.TrimSpace(`
WITH query AS (
  SELECT count(*) AS value from work_orders WHERE status NOT IN (SELECT * FROM VALUES ('CREATED'), ('COMPLELTED') AS t(status))
), result AS (
  SELECT value FROM query LIMIT 1
)
SELECT
  GETDATE() AS proc_time,
  IF(value = 0, 0, 1) is_failed,
  IF(value = 0, 1, 0) is_ok,
  "work_orders" AS table_name,
  "status should be valid" AS rule_name,
  "sql" AS validator,
  "{"Expect":{"EQ":0},"Filter":"deleted = false","Query":"SELECT count(*) AS value from work_orders WHERE status NOT IN (SELECT * FROM VALUES (\'CREATED\'), (\'COMPLELTED\') AS t(status))","Validator":"sql"}" AS context,
  value AS value
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}
