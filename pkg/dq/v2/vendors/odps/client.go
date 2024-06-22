package odps

import (
	_ "github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"github.com/jmoiron/sqlx"
)

func NewDB(dsn string) (*sqlx.DB, error) {
	return sqlx.Open("odps", dsn)
}
