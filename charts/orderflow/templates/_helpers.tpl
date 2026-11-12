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

{{/* Adresses de l'infrastructure (autre namespace) : FQDN dérivés de infraNamespace et
clusterDomain, sauf valeur explicite. */}}
{{- define "orderflow.rabbitmqHost" -}}
{{- .Values.rabbitmq.host | default (printf "rabbitmq.%s.svc.%s" .Values.infraNamespace .Values.clusterDomain) }}
{{- end }}

{{- define "orderflow.redisSentinels" -}}
{{- .Values.redis.sentinels | default (printf "redis-sentinel.%s.svc.%s:26379" .Values.infraNamespace .Values.clusterDomain) }}
{{- end }}

{{/* Données de ConfigMap/orderflow-config ; aussi base du checksum/config des pods. */}}
{{- define "orderflow.configData" -}}
RABBITMQ_HOST: {{ include "orderflow.rabbitmqHost" . | quote }}
RABBITMQ_PORT: {{ .Values.rabbitmq.port | quote }}
RABBITMQ_VHOST: {{ .Values.rabbitmq.vhost | quote }}
REDIS_SENTINELS: {{ include "orderflow.redisSentinels" . | quote }}
REDIS_MASTER_NAME: {{ .Values.redis.masterName | quote }}
REDIS_DB: {{ .Values.redis.db | quote }}
LOG_LEVEL: {{ .Values.logLevel | quote }}
{{- end }}

{{/*
Identifiants : deux Secrets du namespace de la release (un Secret ne se référence pas d'un
namespace à l'autre), provisionnés AVANT le chart par scripts/deploy-apps.ps1 (ou à la main,
cf. NOTES.txt) : rabbitmq.secretName (username, password) et redis.secretName (password).

orderflow.credentialsCheck : face à un vrai cluster, échoue avec un message clair si l'un
manque (même technique que le contrôle du CRD du chart rabbitmq : hors ligne, lookup renvoie
toujours vide et le contrôle est sauté, helm lint / template fonctionnent sans cluster).
*/}}
{{- define "orderflow.credentialsCheck" -}}
{{- if lookup "v1" "Namespace" "" "kube-system" }}
{{- $checks := list (list .Values.rabbitmq.secretName (list "username" "password")) (list .Values.redis.secretName (list "password")) }}
{{- range $checks }}
{{- $name := index . 0 }}
{{- $secret := lookup "v1" "Secret" $.Release.Namespace $name }}
{{- range $key := index . 1 }}
{{- if not (and $secret $secret.data (index $secret.data $key)) }}
{{- fail (printf "Secret/%s (clé %s) absent du namespace %s : lancer scripts/deploy-apps.ps1, qui le provisionne avant helm, ou le créer à la main (cf. docs/exploitation.md, « Identifiants des applications »)" $name $key $.Release.Namespace) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
checksum/credentials : empreinte des données des Secrets d'identifiants utilisés par le pod
(dict root, redis). Calculée ici par lookup plutôt que transmise par le script : elle suit
le Secret réel quel que soit l'outil qui l'a modifié (script, kubectl, rotation manuelle) dès
le helm upgrade suivant, et rien de dérivé du mot de passe ne transite par la ligne de
commande ni ne s'inscrit dans les values de la release. Seule une empreinte sha256 figure sur
les pods. Hors ligne, lookup renvoie vide : empreinte constante, sans effet.
*/}}
{{- define "orderflow.credentialsChecksum" -}}
{{- $data := dict }}
{{- $rabbit := lookup "v1" "Secret" .root.Release.Namespace .root.Values.rabbitmq.secretName }}
{{- $_ := set $data "rabbitmq" (($rabbit | default dict).data | default dict) }}
{{- if .redis }}
{{- $redis := lookup "v1" "Secret" .root.Release.Namespace .root.Values.redis.secretName }}
{{- $_ := set $data "redis" (($redis | default dict).data | default dict) }}
{{- end }}
{{- $data | toJson | sha256sum }}
{{- end }}

{{/* Annotations des pods : dict root, image, redis (bool : le pod lit REDIS_PASSWORD).
  checksum/config      : une modification de la configuration commune relance les pods ;
  checksum/credentials : un identifiant modifié (rotation) relance les pods qui l'utilisent ;
  checksum/image       : identifiant de l'image locale (imageIds), tag dev mutable. */}}
{{- define "orderflow.podAnnotations" -}}
checksum/config: {{ include "orderflow.configData" .root | sha256sum }}
checksum/credentials: {{ include "orderflow.credentialsChecksum" . }}
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
