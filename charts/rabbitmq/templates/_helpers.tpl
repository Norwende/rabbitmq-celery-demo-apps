{{/*
Noms fixes (docs/contracts.md) : pas de préfixe .Release.Name, une installation par
namespace. Seuls les labels portent le nom de la release (app.kubernetes.io/instance).
*/}}

{{- define "rabbitmq.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Labels des objets (metadata.labels uniquement : les pods sont créés par l'opérateur, et
un changement de version du chart ne doit relancer aucun pod). */}}
{{- define "rabbitmq.labels" -}}
helm.sh/chart: {{ include "rabbitmq.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: orderflow
app.kubernetes.io/name: rabbitmq
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: broker
{{- end }}
