WITH query AS (
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
FROM result
