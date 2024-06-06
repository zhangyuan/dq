package odps

import (
	"database/sql"

	_ "github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
)

type Client struct {
	DB *sql.DB
}

func NewClient(dsn string) (*Client, error) {
	db, err := sql.Open("odps", dsn)
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
