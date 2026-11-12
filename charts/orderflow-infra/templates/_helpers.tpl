{{/*
Noms fixes (docs/contracts.md) : pas de préfixe .Release.Name, une installation par
namespace. Seuls les labels portent le nom de la release (app.kubernetes.io/instance).
*/}}

{{- define "orderflow-infra.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Labels communs (metadata.labels uniquement, jamais dans un sélecteur ni un pod template :
un changement de version du chart ne doit relancer aucun pod). */}}
{{- define "orderflow-infra.labels" -}}
helm.sh/chart: {{ include "orderflow-infra.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: orderflow
{{- end }}

{{/* Sélecteur des pods Redis (immuable sur le StatefulSet). */}}
{{- define "orderflow-infra.redis.selectorLabels" -}}
app.kubernetes.io/name: redis
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Secret qui porte le mot de passe Redis (clé "password"). */}}
{{- define "orderflow-infra.redis.secretName" -}}
{{- .Values.redis.auth.existingSecret | default "redis-auth" }}
{{- end }}

{{/* Contenu des scripts Redis (files/redis/*.sh) : données de la ConfigMap et base de son
checksum, posé en annotation du pod template (un script modifié => rolling update). */}}
{{- define "orderflow-infra.redis.scripts" -}}
{{ (.Files.Glob "files/redis/*.sh").AsConfig }}
{{- end }}

{{/* Variables lues par les scripts (init container et preStop du conteneur redis). */}}
{{- define "orderflow-infra.redis.scriptEnv" -}}
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
      name: {{ include "orderflow-infra.redis.secretName" . }}
      key: password
- name: MASTER_NAME
  value: {{ .Values.redis.masterName | quote }}
- name: REPLICAS
  value: {{ .Values.redis.replicas | quote }}
- name: SENTINEL_QUORUM
  value: {{ .Values.redis.sentinel.quorum | quote }}
- name: REDIS_MAXMEMORY
  value: {{ .Values.redis.maxmemory | quote }}
- name: HEADLESS_SERVICE
  value: redis-headless
- name: CLUSTER_DOMAIN
  value: {{ .Values.clusterDomain | quote }}
{{- end }}

{{/* REDISCLI_AUTH : lu par redis-cli (probes, kubectl exec), mot de passe jamais en argument. */}}
{{- define "orderflow-infra.redis.cliAuthEnv" -}}
- name: REDISCLI_AUTH
  valueFrom:
    secretKeyRef:
      name: {{ include "orderflow-infra.redis.secretName" . }}
      key: password
{{- end }}

{{- define "orderflow-infra.redis.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end }}
