package odps

import (
	"dq/pkg/dq/v2/spec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompileRowCountRule(t *testing.T) {
	compiler := Compiler{}
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
	SELECT COUNT(*) as value FROM orders WHERE deleted = false
)
SELECT 	GETDATE() AS proc_time,
		IF(value > 0, 0, 1) is_failed,
		IF(value > 0, 1, 0) is_ok,
		"orders" as table_name,
		"rows_count" as validator
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileRowCountRuleWithExtraFilter(t *testing.T) {
	compiler := Compiler{}
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
	SELECT COUNT(*) as value FROM orders WHERE deleted = false AND type IS NOT NULL
)
SELECT 	GETDATE() AS proc_time,
		IF(value > 0, 0, 1) is_failed,
		IF(value > 0, 1, 0) is_ok,
		"orders" as table_name,
		"rows_count" as validator
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}

func TestCompileRowCountRuleWithFilterOverwritten(t *testing.T) {
	compiler := Compiler{}
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
	SELECT COUNT(*) as value FROM orders WHERE 1=1
)
SELECT 	GETDATE() AS proc_time,
		IF(value > 0, 0, 1) is_failed,
		IF(value > 0, 1, 0) is_ok,
		"orders" as table_name,
		"rows_count" as validator
FROM result`)

	stmt, err := compiler.CompileRule(&model, &rule)
	assert.Nil(t, err)
	assert.Equal(t, expected, stmt)
}
