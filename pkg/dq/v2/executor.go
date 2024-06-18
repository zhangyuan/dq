package v2

import (
	"dq/pkg/dq/v2/spec"
	"dq/pkg/dq/v2/vendors/odps"
	"fmt"
	"os"
)

type Executor struct {
	Vendor string
	DSN    string
}

type ResultRow struct {
	IsFailed string
	IsOk     string
}

func (e *Executor) Execute(rulesPath string, format string) (bool, error) {
	bytes, err := os.ReadFile(rulesPath)
	if err != nil {
		return false, err
	}
	spec, err := spec.Parse(bytes, func(*spec.Spec) error {
		return nil
	})
	if err != nil {
		return false, err
	}

	var resultRows []ResultRow

	if e.Vendor == "odps" {
		sql, err := odps.Compile(spec)
		if err != nil {
			return false, err
		}
		client, err := odps.NewClient(e.DSN)
		if err != nil {
			return false, err
		}

		rows, err := client.DB.Queryx(sql)
		if err != nil {
			for rows.Next() {
				var resultRow ResultRow
				if err := rows.StructScan(&resultRow); err != nil {
					return false, nil
				}
				resultRows = append(resultRows, resultRow)
			}
		}
	}

	fmt.Println(resultRows)

	return true, nil
}
