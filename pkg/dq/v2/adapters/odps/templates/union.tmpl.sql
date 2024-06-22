WITH final AS (
{{- $length := len .Queries -}}

{{- if eq $length 0 -}}

{{- range $idx, $query := .Queries }}
  (
{{ indent 4 $query }}
  )
  {{ if not (isLast $idx $length) }}UNION ALL{{end -}}
{{- end -}}

{{- else -}}

{{- range $idx, $query := .Queries }}
{{ indent 2 $query }}
{{- end -}}

{{- end }}
)
SELECT GETDATE() AS proc_time, table_name, rule_name, validator, is_failed, is_ok, value
FROM final
