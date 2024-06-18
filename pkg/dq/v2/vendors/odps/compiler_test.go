package odps

import (
	"dq/pkg/dq/v2/spec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompileRule(t *testing.T) {
	compiler := Compiler{}
	model := spec.Model{Table: "orders", DefaultFilter: "deleted = false"}
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
