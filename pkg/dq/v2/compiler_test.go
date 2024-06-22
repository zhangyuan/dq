package v2

import (
	"dq/pkg/dq/v2/spec"
	"dq/pkg/dq/v2/vendors/odps"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompileRowCountRule(t *testing.T) {
	templates := odps.OdpsTemplates{}
	compiler := NewCompiler(templates)
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
SELECT 	GETDATE() AS proc_time,
	IF(value > 0, 0, 1) is_failed,
	IF(value > 0, 1, 0) is_ok,
	"orders" AS table_name,
	"rows_count" AS validator
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileRowCountRuleWithExtraFilter(t *testing.T) {
	templates := odps.OdpsTemplates{}
	compiler := NewCompiler(templates)
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
SELECT 	GETDATE() AS proc_time,
	IF(value > 0, 0, 1) is_failed,
	IF(value > 0, 1, 0) is_ok,
	"orders" AS table_name,
	"rows_count" AS validator
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileRowCountRuleWithFilterOverwritten(t *testing.T) {
	templates := odps.OdpsTemplates{}
	compiler := NewCompiler(templates)
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
SELECT 	GETDATE() AS proc_time,
	IF(value > 0, 0, 1) is_failed,
	IF(value > 0, 1, 0) is_ok,
	"orders" AS table_name,
	"rows_count" AS validator
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileDuplicatesRule(t *testing.T) {
	templates := odps.OdpsTemplates{}
	compiler := NewCompiler(templates)

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
	HAVINT COUNT(*) > 1
), result AS (
	SELECT COUNT(*) AS value FROM query
),
SELECT 	GETDATE() AS proc_time,
	IF(value = 0, 0, 1) is_failed,
	IF(value = 0, 1, 0) is_ok,
	"orders" AS table_name,
	"duplicates" AS validator
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileDuplicatesRuleGivenMultileColumns(t *testing.T) {
	templates := odps.OdpsTemplates{}
	compiler := NewCompiler(templates)

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
	HAVINT COUNT(*) > 1
), result AS (
	SELECT COUNT(*) AS value FROM query
),
SELECT 	GETDATE() AS proc_time,
	IF(value = 0, 0, 1) is_failed,
	IF(value = 0, 1, 0) is_ok,
	"work_orders" AS table_name,
	"duplicates" AS validator
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileSqlRule(t *testing.T) {
	templates := odps.OdpsTemplates{}
	compiler := NewCompiler(templates)

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
),
SELECT 	GETDATE() AS proc_time,
	IF(value = 0, 0, 1) is_failed,
	IF(value = 0, 1, 0) is_ok,
	"work_orders" AS table_name,
	"sql" AS validator
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}
