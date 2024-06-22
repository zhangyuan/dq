package v2

import (
	"bytes"
	"dq/pkg/dq/v2/spec"
	"dq/pkg/dq/v2/templates"
	"fmt"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
)

const RowsCountValidator = "rows_count"
const DuplicatesValidator = "duplicates"
const NotNullValidator = "not_null"
const SqlValidator = "sql"

type Compiler struct {
	templates templates.Templates
}

func NewCompiler(templates templates.Templates) *Compiler {
	return &Compiler{
		templates: templates,
	}
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

func (c *Compiler) CompileRule(model *spec.Model, rule *spec.Rule) (string, error) {
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
		"TableName":  model.Table,
		"Filter":     filter,
		"Validator":  rule.Validator,
		"Conditions": strings.Join(CompileExpect(&rule.Expect), " AND "),
	}

	if rule.Validator == RowsCountValidator {
		sqlTemplate, err := template.New("sql").Parse(c.templates.RowsCount())
		if err != nil {
			return "", nil
		}
		return executeTemplate(sqlTemplate, data)
	} else if rule.Validator == DuplicatesValidator {
		data["Columns"] = rule.Columns
		sqlTemplate, err := template.New("sql").Funcs(sprig.FuncMap()).Parse(c.templates.Duplicates())
		if err != nil {
			return "", err
		}
		return executeTemplate(sqlTemplate, data)
	} else if rule.Validator == SqlValidator {
		sqlTemplate, err := template.New("sql").Funcs(sprig.FuncMap()).Parse(c.templates.CustomSql())
		if err != nil {
			return "", err
		}
		data["Query"] = rule.Query
		return executeTemplate(sqlTemplate, data)
	} else {
		return "", fmt.Errorf("invalid validator %s", rule.Validator)
	}
}

func executeTemplate(template *template.Template, data map[string]any) (string, error) {
	var buf bytes.Buffer
	if err := template.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (c *Compiler) Compile(spec *spec.Spec) (string, error) {
	statements := []string{}
	for idx := range spec.Models {
		model := spec.Models[idx]
		for ruleIdx := range model.Rules {
			rule := model.Rules[ruleIdx]
			statement, err := c.CompileRule(&model, &rule)
			if err != nil {
				return "", nil
			}
			statements = append(statements, statement)
		}
	}
	return strings.Join(statements, "\n"), nil
}
