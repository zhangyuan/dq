package odps

import _ "embed"

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
	return rowsCount
}

func (t OdpsTemplates) Duplicates() string {
	return duplicates
}

func (t OdpsTemplates) CustomSql() string {
	return customSQL
}

func (t OdpsTemplates) Union() string {
	return unionSQL
}
