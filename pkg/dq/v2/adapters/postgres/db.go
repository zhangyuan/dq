package postgres

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func NewDB(dsn string) (*sqlx.DB, error) {
	return sqlx.Open("postgres", dsn)
}
