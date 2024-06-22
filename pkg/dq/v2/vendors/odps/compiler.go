package odps

import (
	"bytes"
	"dq/pkg/dq/v2/spec"
	"fmt"
	"strings"
	"text/template"
)

const RowsCountTemplateContent = `WITH result AS (
	SELECT COUNT(*) as value FROM {{ .TableName }}{{ if .Filter }} WHERE {{ .Filter }}{{end}}
)
SELECT 	GETDATE() AS proc_time,
		IF({{.Conditions }}, 0, 1) is_failed,
		IF({{ .Conditions }}, 1, 0) is_ok,
		"{{ .TableName }}" as table_name,
		"{{ .Validator }}" as validator
FROM result`

type Compiler struct {
}

func (c *Compiler) Compile(spec *spec.Spec) (string, error) {
	return Compile(spec)
}

func (c *Compiler) CompileRule(table *spec.Model, rule *spec.Rule) (string, error) {
	return CompileRule(table, rule)
}

func CompileExpect(expect *spec.Expect) []string {
	conditions := []string{}

	if expect.EQ != nil {
		conditions = append(conditions, fmt.Sprintf("value = %d", *expect.EQ))
	}
	if expect.GT != nil {
		conditions = append(conditions, fmt.Sprintf("value > %d", *expect.GT))
	}
	if expect.LT != nil {
		conditions = append(conditions, fmt.Sprintf("value < %d", *expect.LT))
	}
	if expect.GTE != nil {
		conditions = append(conditions, fmt.Sprintf("value >= %d", *expect.GTE))
	}
	if expect.LTE != nil {
		conditions = append(conditions, fmt.Sprintf("value <= %d", *expect.LTE))
	}

	return conditions
}

func CompileRule(model *spec.Model, rule *spec.Rule) (string, error) {
	var filter string
	if rule.Filter != "" {
		filter = rule.Filter
	} else {
		filter = model.Filter
	}
	if rule.ExtraFilter != "" {
		filter = fmt.Sprintf("%s AND %s", filter, rule.ExtraFilter)
	}

	data := map[string]interface{}{
		"TableName": model.Table,
		"Filter":    filter,
		"Validator": rule.Validator,
	}

	if rule.Validator == "rows_count" {
		rowsCountTemplate, err := template.New("rowsCountTemplate").Parse(RowsCountTemplateContent)
		if err != nil {
			return "", nil
		}

		data["Conditions"] = strings.Join(CompileExpect(&rule.Expect), " AND ")

		var buf bytes.Buffer
		if err := rowsCountTemplate.Execute(&buf, data); err != nil {
			return "", err
		}
		return buf.String(), nil
	} else {
		return "", fmt.Errorf("invalid validator %s", rule.Validator)
	}
}

func Compile(spec *spec.Spec) (string, error) {
	statements := []string{}
	for idx := range spec.Models {
		model := spec.Models[idx]
		for ruleIdx := range model.Rules {
			rule := model.Rules[ruleIdx]
			statement, err := CompileRule(&model, &rule)
			if err != nil {
				return "", nil
			}
			statements = append(statements, statement)
		}
	}
	return strings.Join(statements, "\n"), nil
}
