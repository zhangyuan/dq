package templates

const RowsCount = `WITH result AS (
	SELECT COUNT(*) AS value FROM {{ .TableName }}{{ if .Filter }} WHERE {{ .Filter }}{{ end }}
)
SELECT 	GETDATE() AS proc_time,
	IF({{ .Conditions }}, 0, 1) is_failed,
	IF({{ .Conditions }}, 1, 0) is_ok,
	"{{ .TableName }}" AS table_name,
	"{{ .Validator }}" AS validator
FROM result`

const Duplicates = `WITH query AS (
	SELECT {{ .Columns | join ", " }} FROM {{ .TableName }} {{ if .Filter }}WHERE {{ .Filter }}{{ end }}
	GROUP BY {{ .Columns | join ", " }}
	HAVINT COUNT(*) > 1
), result AS (
	SELECT COUNT(*) AS value FROM query
),
SELECT 	GETDATE() AS proc_time,
	IF({{ .Conditions }}, 0, 1) is_failed,
	IF({{ .Conditions }}, 1, 0) is_ok,
	"{{ .TableName }}" AS table_name,
	"{{ .Validator }}" AS validator
FROM result`

const CustomSQL = `WITH query AS (
	{{ .Query }}
), result AS (
	SELECT value FROM query LIMIT 1
),
SELECT 	GETDATE() AS proc_time,
	IF({{ .Conditions }}, 0, 1) is_failed,
	IF({{ .Conditions }}, 1, 0) is_ok,
	"{{ .TableName }}" AS table_name,
	"{{ .Validator }}" AS validator
FROM result`
