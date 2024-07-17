package postgres

import (
	_ "embed"
	"strings"
)

//go:embed templates/rows_count.tmpl.sql
var count string

//go:embed templates/duplicates.tmpl.sql
var duplicateValue string

//go:embed templates/custom_sql.tmpl.sql
var customSQL string

//go:embed templates/union.tmpl.sql
var unionSQL string

//go:embed templates/null_value.tmpl.sql
var nullValue string

//go:embed templates/not_null_value.tmpl.sql
var notNullValue string

//go:embed templates/empty_text_value.tmpl.sql
var emptyTextValue string

type PostgresTemplates struct{}

func (t PostgresTemplates) RowsCount() string {
	return strings.TrimSpace(count)
}

func (t PostgresTemplates) Duplicates() string {
	return strings.TrimSpace(duplicateValue)
}

func (t PostgresTemplates) CustomSql() string {
	return strings.TrimSpace(customSQL)
}

func (t PostgresTemplates) Union() string {
	return strings.TrimSpace(unionSQL)
}

func (t PostgresTemplates) NullValue() string {
	return strings.TrimSpace(nullValue)
}

func (t PostgresTemplates) NotNullValue() string {
	return notNullValue
}

func (t PostgresTemplates) EmptyTextValue() string {
	return emptyTextValue
}

func (t PostgresTemplates) EsacpeStringValue(str string) string {
	return str
}
