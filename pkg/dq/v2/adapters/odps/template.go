package odps

const rowsCount = `WITH result AS (
  SELECT COUNT(*) AS value FROM {{ .TableName }}{{ if .Filter }} WHERE {{ .Filter }}{{ end }}
)
SELECT
  GETDATE() AS proc_time,
  IF({{ .Conditions }}, 0, 1) is_failed,
  IF({{ .Conditions }}, 1, 0) is_ok,
  "{{ .TableName }}" AS table_name,
  "{{ .Rule.Name }}" AS rule_name,
  "{{ .Rule.Validator }}" AS validator,
  value AS value
FROM result`

const duplicates = `WITH query AS (
  SELECT {{ .Columns | join ", " }} FROM {{ .TableName }} {{ if .Filter }}WHERE {{ .Filter }}{{ end }}
  GROUP BY {{ .Columns | join ", " }}
  HAVING COUNT(*) > 1
), result AS (
  SELECT COUNT(*) AS value FROM query
)
SELECT
  GETDATE() AS proc_time,
  IF({{ .Conditions }}, 0, 1) is_failed,
  IF({{ .Conditions }}, 1, 0) is_ok,
  "{{ .TableName }}" AS table_name,
  "{{ .Rule.Name }}" AS rule_name,
  "{{ .Rule.Validator }}" AS validator,
  value AS value
FROM result`

const customSQL = `WITH query AS (
{{ indent 2 .Query }}
), result AS (
  SELECT value FROM query LIMIT 1
)
SELECT
  GETDATE() AS proc_time,
  IF({{ .Conditions }}, 0, 1) is_failed,
  IF({{ .Conditions }}, 1, 0) is_ok,
  "{{ .TableName }}" AS table_name,
  "{{ .Rule.Name }}" AS rule_name,
  "{{ .Rule.Validator }}" AS validator,
  value AS value
FROM result`

const unionSQL = `WITH final AS (
{{- $length := len .Queries -}}
{{- range $idx, $query := .Queries }}
  (
{{ indent 4 $query }}
  )
  {{ if not (isLast $idx $length) }}UNION ALL{{end -}}
{{- end }}
)
SELECT GETDATE() AS proc_time, table_name, rule_name, validator, is_failed, is_ok, value
FROM final
`

type OdpsTemplates struct{}

func (t OdpsTemplates) RowsCount() string {
	return rowsCount
}

func (t OdpsTemplates) Duplicates() string {
	return duplicates
}

func (t OdpsTemplates) CustomSql() string {
	return customSQL
}

func (t OdpsTemplates) Union() string {
	return unionSQL
}
