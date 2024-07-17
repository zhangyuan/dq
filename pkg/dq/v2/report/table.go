package report

import (
	v2 "dq/pkg/dq/v2"

	"github.com/jedib0t/go-pretty/v6/table"
)

func NewTable(result *v2.Result) table.Writer {
	writer := table.NewWriter()

	writer.AppendHeader(table.Row{
		"proc_time",
		"table_name",
		"rule_name",
		"is_ok",
		"is_failed",
		"value",
		"context",
	})

	for idx := range result.Results {
		record := result.Results[idx]
		writer.AppendRow(table.Row{
			record.ProcTime,
			record.TableName,
			record.RuleName,
			record.IsOk,
			record.IsFailed,
			record.Value,
			record.Context,
		})
	}
	return writer
}
