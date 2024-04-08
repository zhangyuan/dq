package db

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type PostgresConnection struct {
}

func NewConnection(driverName string, dataSourceName string) (*sqlx.DB, error) {
	return sqlx.Connect(driverName, dataSourceName)
}
