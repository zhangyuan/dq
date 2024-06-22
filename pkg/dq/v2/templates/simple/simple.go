package simple

import (
	"fmt"
	"regexp"
	"strings"
)

var re = regexp.MustCompile(`\${([\w.]+)}`)

func Compile(text string, params map[string]any) (string, error) {
	compiled := re.ReplaceAllStringFunc(text, func(s string) string {
		for key, value := range params {
			if fmt.Sprintf("${%v}", key) == s {
				return fmt.Sprint(value)
			}
		}
		return s
	})
	return strings.ReplaceAll(compiled, "\\$", "$"), nil
}
