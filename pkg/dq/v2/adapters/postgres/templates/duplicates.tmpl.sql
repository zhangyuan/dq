WITH query AS (
  SELECT {{ .Columns | join ", " }} FROM {{ .TableName }} {{ if .Filter }}WHERE {{ .Filter }}{{ end }}
  GROUP BY {{ .Columns | join ", " }}
  HAVING COUNT(*) > 1
), result AS (
  SELECT COUNT(*) AS value FROM query
)
SELECT
  NOW() AS proc_time,
  CASE WHEN {{ .Conditions }} THEN 0 ELSE 1 END is_failed,
  CASE WHEN {{ .Conditions }} THEN 1 ELSE 0 END is_ok,
  '{{ .TableName }}' AS table_name,
  '{{ .Rule.Name }}' AS rule_name,
  '{{ .Rule.Validator }}' AS validator,
  '{{ .Context }}' AS context,
  value AS value
FROM result
