package report

import (
	v2 "dq/pkg/dq/v2"

	"github.com/jedib0t/go-pretty/v6/table"
)

func NewTable(result *v2.Result) table.Writer {
	writer := table.NewWriter()
	columnNames := make([]interface{}, len(result.ColumnNames))
	for idx := range result.ColumnNames {
		columnNames[idx] = result.ColumnNames[idx]
	}
	writer.AppendHeader(columnNames)

	for idx := range result.Records {
		record := result.Records[idx]
		writer.AppendRow(table.Row(record))
	}
	return writer
}
