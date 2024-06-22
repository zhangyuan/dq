package db

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
)

func Query(db *sqlx.DB, query string, onColumnTypesFunc func(columNames []string) error, onColumnNamesFunc func(columNames []string) error, onRowFunc func(values []any) error) error {
	rows, err := db.Queryx(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	columnTypeNames := lo.Map(columnTypes, func(t *sql.ColumnType, index int) string {
		return t.DatabaseTypeName()
	})

	if err := onColumnTypesFunc(columnTypeNames); err != nil {
		return err
	}

	columnNames, err := rows.Columns()
	if err != nil {
		return err
	}

	if err := onColumnNamesFunc(columnNames); err != nil {
		return err
	}

	for rows.Next() {
		var record []any
		record, err := rows.SliceScan()
		if err != nil {
			return err
		}
		if err := onRowFunc(record); err != nil {
			return err
		}
	}

	return nil
}
