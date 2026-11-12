{{/*
Noms fixes (docs/contracts.md) : pas de préfixe .Release.Name, une installation par
namespace. Les helpers qui prennent un dict attendent "root" (le contexte racine $),
"name" (nom du Deployment) et, selon le cas, "component" ou "image".
*/}}

{{- define "orderflow.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Sélecteur d'un Deployment (immuable) : dict root, name. */}}
{{- define "orderflow.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end }}

{{/* Labels des pods : sélecteur + composant. Ni version de chart ni tag : un changement
de chart ne relance pas les pods. dict root, name, component. */}}
{{- define "orderflow.podLabels" -}}
{{ include "orderflow.selectorLabels" . }}
app.kubernetes.io/component: {{ .component }}
app.kubernetes.io/part-of: orderflow
{{- end }}

{{/* Labels des objets (metadata.labels) : dict root, name, component. */}}
{{- define "orderflow.labels" -}}
{{ include "orderflow.podLabels" . }}
app.kubernetes.io/version: {{ include "orderflow.tag" .root | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ include "orderflow.chart" .root }}
{{- end }}

{{- define "orderflow.tag" -}}
{{- .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/* Référence d'image : dict root, image (nom court, ex. gateway). */}}
{{- define "orderflow.image" -}}
{{- with .root.Values.image.registry }}{{ trimSuffix "/" . }}/{{ end }}orderflow/{{ .image }}:{{ include "orderflow.tag" .root }}
{{- end }}

{{/* Données de ConfigMap/orderflow-config ; aussi base du checksum/config des pods. */}}
{{- define "orderflow.configData" -}}
RABBITMQ_HOST: {{ .Values.rabbitmq.host | quote }}
RABBITMQ_PORT: {{ .Values.rabbitmq.port | quote }}
RABBITMQ_VHOST: {{ .Values.rabbitmq.vhost | quote }}
REDIS_SENTINELS: {{ .Values.redis.sentinels | quote }}
REDIS_MASTER_NAME: {{ .Values.redis.masterName | quote }}
REDIS_DB: {{ .Values.redis.db | quote }}
LOG_LEVEL: {{ .Values.logLevel | quote }}
{{- end }}

{{/* Annotations des pods : dict root, image.
  checksum/config : une modification de la configuration commune relance les pods ;
  checksum/image  : identifiant de l'image locale (imageIds), tag dev mutable. */}}
{{- define "orderflow.podAnnotations" -}}
checksum/config: {{ include "orderflow.configData" .root | sha256sum }}
{{- with index (.root.Values.imageIds | default dict) .image }}
checksum/image: {{ . | quote }}
{{- end }}
{{- end }}

{{/* Début commun des pod specs : pas de token d'API, UID/GID 10001 (créés dans les images). */}}
{{- define "orderflow.podSpecCommon" -}}
automountServiceAccountToken: false
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
securityContext:
  runAsNonRoot: true
  runAsUser: 10001
  runAsGroup: 10001
  seccompProfile:
    type: RuntimeDefault
{{- end }}

{{- define "orderflow.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end }}

{{/* Configuration commune + identifiants. dict root, redis (bool : REDIS_PASSWORD). */}}
{{- define "orderflow.envFrom" -}}
- configMapRef:
    name: orderflow-config
{{- end }}

{{- define "orderflow.secretEnv" -}}
- name: RABBITMQ_USER
  valueFrom:
    secretKeyRef: {name: {{ .root.Values.rabbitmq.secretName }}, key: username}
- name: RABBITMQ_PASSWORD
  valueFrom:
    secretKeyRef: {name: {{ .root.Values.rabbitmq.secretName }}, key: password}
{{- if .redis }}
- name: REDIS_PASSWORD
  valueFrom:
    secretKeyRef: {name: {{ .root.Values.redis.secretName }}, key: password}
{{- end }}
{{- end }}

{{/* Seul /tmp est inscriptible (racine en lecture seule). */}}
{{- define "orderflow.tmpVolumeMount" -}}
- name: tmp
  mountPath: /tmp
{{- end }}

{{- define "orderflow.tmpVolume" -}}
- name: tmp
  emptyDir:
    sizeLimit: 16Mi
{{- end }}
