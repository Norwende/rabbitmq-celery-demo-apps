{{/*
Noms fixes (docs/contracts.md) : pas de préfixe .Release.Name, une installation par
namespace. Seuls les labels portent le nom de la release (app.kubernetes.io/instance).
*/}}

{{- define "redis.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Labels communs (metadata.labels uniquement, jamais dans un sélecteur ni un pod template :
un changement de version du chart ne doit relancer aucun pod). */}}
{{- define "redis.labels" -}}
helm.sh/chart: {{ include "redis.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: orderflow
{{- end }}

{{/* Sélecteur des pods Redis (immuable sur le StatefulSet). */}}
{{- define "redis.selectorLabels" -}}
app.kubernetes.io/name: redis
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "redis.image" -}}
{{- with .Values.image.registry }}{{ trimSuffix "/" . }}/{{ end }}{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end }}

{{/* Secret qui porte le mot de passe Redis (clé "password"). */}}
{{- define "redis.secretName" -}}
{{- .Values.auth.existingSecret | default "redis-auth" }}
{{- end }}

{{/* Contenu des scripts (files/*.sh) : données de la ConfigMap et base de son checksum,
posé en annotation du pod template (un script modifié => rolling update). */}}
{{- define "redis.scripts" -}}
{{ (.Files.Glob "files/*.sh").AsConfig }}
{{- end }}

{{/* Variables lues par les scripts (init container et preStop du conteneur redis).
POD_NAMESPACE vient de la downward API : les FQDN annoncés suivent le namespace réel du
pod, rien n'est écrit en dur. */}}
{{- define "redis.scriptEnv" -}}
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "redis.secretName" . }}
      key: password
- name: MASTER_NAME
  value: {{ .Values.masterName | quote }}
- name: REPLICAS
  value: {{ .Values.replicas | quote }}
- name: SENTINEL_QUORUM
  value: {{ .Values.sentinel.quorum | quote }}
- name: REDIS_MAXMEMORY
  value: {{ .Values.maxmemory | quote }}
- name: HEADLESS_SERVICE
  value: redis-headless
- name: CLUSTER_DOMAIN
  value: {{ .Values.clusterDomain | quote }}
{{- end }}

{{/* REDISCLI_AUTH : lu par redis-cli (probes, kubectl exec), mot de passe jamais en argument. */}}
{{- define "redis.cliAuthEnv" -}}
- name: REDISCLI_AUTH
  valueFrom:
    secretKeyRef:
      name: {{ include "redis.secretName" . }}
      key: password
{{- end }}

{{- define "redis.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end }}
