package adapters

import (
	"dq/pkg/dq/v2/adapters/odps"
	"dq/pkg/dq/v2/templates"
	"fmt"
	"strings"
)

type Adapter struct {
	Name      string
	DSN       string
	Templates templates.SqlTemplates
}

func NewAdapter(dsn string) (*Adapter, error) {
	if strings.Contains(dsn, "maxcompute.aliyun.com/api") {
		return &Adapter{
			Name:      odps.Name,
			DSN:       dsn,
			Templates: odps.OdpsTemplates{},
		}, nil
	}

	return nil, fmt.Errorf("invalid vendor")
}
