package odps

import (
	_ "github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"github.com/jmoiron/sqlx"
)

type Client struct {
	DB *sqlx.DB
}

func NewClient(dsn string) (*Client, error) {
	db, err := sqlx.Open("odps", dsn)
	if err != nil {
		return nil, err
	}
	return &Client{
		DB: db,
	}, nil
}

func (client *Client) Close() error {
	return client.DB.Close()
}
