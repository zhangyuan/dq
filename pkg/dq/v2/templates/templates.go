package templates

type Templates interface {
	RowsCount() string
	Duplicates() string
	CustomSql() string
}
