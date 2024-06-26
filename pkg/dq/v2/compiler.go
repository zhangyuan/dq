package v2

import (
	"bytes"
	"dq/pkg/dq/v2/adapters"
	"dq/pkg/dq/v2/spec"
	"dq/pkg/dq/v2/templates/simple"
	"encoding/json"
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
	Adatper *adapters.Adapter
}

func NewCompiler(adapter *adapters.Adapter) *Compiler {
	return &Compiler{
		Adatper: adapter,
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
		"Rule":       rule,
		"Expect":     rule.Expect,
		"Conditions": strings.Join(CompileExpect(&rule.Expect), " AND "),
	}

	context := map[string]interface{}{
		"Validator": rule.Validator,
		"Expect":    rule.Expect,
		"Filter":    filter,
	}

	if rule.Validator == RowsCountValidator {
		sqlTemplate, err := NewTexTemplate("sql").Parse(c.Adatper.Templates.RowsCount())
		if err != nil {
			return "", nil
		}
		jsonBytes, err := json.Marshal(context)
		if err != nil {
			return "", err
		}
		data["Context"] = c.Adatper.Templates.EsacpeStringValue(string(jsonBytes))

		return executeTemplate(sqlTemplate, data)
	} else if rule.Validator == DuplicatesValidator {

		sqlTemplate, err := NewTexTemplate("sql").Funcs(sprig.FuncMap()).Parse(c.Adatper.Templates.Duplicates())
		if err != nil {
			return "", err
		}

		data["Columns"] = rule.Columns
		context["Columns"] = rule.Columns

		jsonBytes, err := json.Marshal(context)
		if err != nil {
			return "", err
		}
		data["Context"] = c.Adatper.Templates.EsacpeStringValue(string(jsonBytes))

		return executeTemplate(sqlTemplate, data)
	} else if rule.Validator == SqlValidator {
		sqlTemplate, err := NewTexTemplate("sql").Funcs(sprig.FuncMap()).Parse(c.Adatper.Templates.CustomSql())
		if err != nil {
			return "", err
		}
		data["Query"] = rule.Query
		context["Query"] = rule.Query

		jsonBytes, err := json.Marshal(context)
		if err != nil {
			return "", err
		}
		data["Context"] = c.Adatper.Templates.EsacpeStringValue(string(jsonBytes))

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

func (c *Compiler) ToQueries(spec *spec.Spec, params *map[string]any) ([]string, error) {
	statements := []string{}
	for idx := range spec.Models {
		model := spec.Models[idx]
		for ruleIdx := range model.Rules {
			rule := model.Rules[ruleIdx]
			statement, err := c.CompileRule(&model, &rule)
			if err != nil {
				return nil, nil
			}

			rendered, err := simple.Compile(statement, *params)
			if err != nil {
				return nil, err
			}

			statements = append(statements, rendered)
		}
	}
	return statements, nil
}

func IsLast(index, length int) bool {
	return index == length-1
}

func NewTexTemplate(name string) *template.Template {
	return template.New(name).Funcs(sprig.TxtFuncMap()).Funcs(template.FuncMap{
		"isLast": IsLast,
	})
}

func (c *Compiler) ToQuery(spec *spec.Spec, params *map[string]any) (string, error) {
	queries, err := c.ToQueries(spec, params)
	if err != nil {
		return "", err
	}

	sqlTemplate, err := NewTexTemplate("sql").Parse(c.Adatper.Templates.Union())
	if err != nil {
		return "", err
	}

	data := map[string]interface{}{
		"Queries": queries,
	}

	rendered, err := executeTemplate(sqlTemplate, data)
	if err != nil {
		return "", err
	}
	return simple.Compile(rendered, *params)
}
