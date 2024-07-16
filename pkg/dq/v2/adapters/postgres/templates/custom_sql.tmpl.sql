WITH query AS (
{{ indent 2 .Query }}
), result AS (
  SELECT value FROM query LIMIT 1
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
