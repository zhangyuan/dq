package postgres

import (
	_ "embed"
	"strings"
)

//go:embed templates/rows_count.tmpl.sql
var rowsCount string

//go:embed templates/duplicates.tmpl.sql
var duplicates string

//go:embed templates/custom_sql.tmpl.sql
var customSQL string

//go:embed templates/union.tmpl.sql
var unionSQL string

type PostgresTemplates struct{}

func (t PostgresTemplates) RowsCount() string {
	return strings.TrimSpace(rowsCount)
}

func (t PostgresTemplates) Duplicates() string {
	return strings.TrimSpace(duplicates)
}

func (t PostgresTemplates) CustomSql() string {
	return strings.TrimSpace(customSQL)
}

func (t PostgresTemplates) Union() string {
	return strings.TrimSpace(unionSQL)
}
