WITH final AS (
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
