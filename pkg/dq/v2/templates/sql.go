package templates

type SqlTemplates interface {
	RowsCount() string
	Duplicates() string
	CustomSql() string
	Union() string
	EsacpeStringValue(str string) string
}
