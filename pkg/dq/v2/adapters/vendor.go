package adapters

import (
	"dq/pkg/dq/v2/adapters/odps"
	"dq/pkg/dq/v2/adapters/postgres"
	"dq/pkg/dq/v2/templates"
	"fmt"
)

type Adapter struct {
	Name      string
	DSN       string
	Templates templates.SqlTemplates
}

func NewAdapterFromDSN(driver string, dsn string) (*Adapter, error) {
	if driver == "odps" || driver == "maxcompute" {
		return &Adapter{
			Name:      odps.Name,
			DSN:       dsn,
			Templates: odps.OdpsTemplates{},
		}, nil
	} else if driver == "postgres" || driver == "hologres" {
		return &Adapter{
			Name:      postgres.Name,
			DSN:       dsn,
			Templates: postgres.PostgresTemplates{},
		}, nil
	}

	return nil, fmt.Errorf("invalid vendor")
}
