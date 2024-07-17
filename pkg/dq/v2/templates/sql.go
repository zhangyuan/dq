package templates

type SqlTemplates interface {
	RowsCount() string
	Duplicates() string
	CustomSql() string
	NullValue() string
	NotNullValue() string
	EmptyTextValue() string
	Union() string
	EsacpeStringValue(str string) string
}
