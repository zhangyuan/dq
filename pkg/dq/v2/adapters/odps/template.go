package odps

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

type OdpsTemplates struct{}

func (t OdpsTemplates) RowsCount() string {
	return strings.TrimSpace(rowsCount)
}

func (t OdpsTemplates) Duplicates() string {
	return strings.TrimSpace(duplicates)
}

func (t OdpsTemplates) CustomSql() string {
	return strings.TrimSpace(customSQL)
}

func (t OdpsTemplates) Union() string {
	return strings.TrimSpace(unionSQL)
}

func (t OdpsTemplates) EsacpeStringValue(str string) string {
	str = strings.ReplaceAll(str, `\`, `\\`)
	str = strings.ReplaceAll(str, `'`, `\'`)
	str = strings.ReplaceAll(str, `;`, `\;`)
	return str
}
