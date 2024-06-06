package v2

import (
	"dq/pkg/dq/v2/spec"
	"dq/pkg/dq/v2/vendors/odps"

	"github.com/pkg/errors"
)

func Compile(dbType string, spec *spec.Spec) (string, error) {
	if dbType == "odps" {
		return odps.Compile(spec)
	}
	return "", errors.Errorf("%s is not supported", dbType)
}
