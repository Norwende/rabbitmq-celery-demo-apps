{{/*
Noms fixes, identiques à ceux que crée le RabbitMQ Cluster Operator (docs/contracts.md) :
StatefulSet rabbitmq-server, Services rabbitmq et rabbitmq-nodes, Secret
rabbitmq-default-user. Une installation par namespace ; seuls les labels portent le nom de
la release (app.kubernetes.io/instance).
*/}}

{{- define "rabbitmq.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "rabbitmq.statefulSetName" -}}rabbitmq-server{{- end }}
{{- define "rabbitmq.headlessService" -}}rabbitmq-nodes{{- end }}

{{/* Labels des objets (jamais dans un sélecteur ni un pod template : un changement de
version du chart ne relance aucun pod). */}}
{{- define "rabbitmq.labels" -}}
helm.sh/chart: {{ include "rabbitmq.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: orderflow
{{ include "rabbitmq.selectorLabels" . }}
app.kubernetes.io/component: broker
{{- end }}

{{/* Sélecteur des pods (immuable sur le StatefulSet). */}}
{{- define "rabbitmq.selectorLabels" -}}
app.kubernetes.io/name: rabbitmq
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "rabbitmq.image" -}}
{{- with .Values.image.registry }}{{ trimSuffix "/" . }}/{{ end }}{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end }}

{{- define "rabbitmq.adminSecret" -}}
{{- .Values.auth.existingSecret | default "rabbitmq-default-user" }}
{{- end }}

{{- define "rabbitmq.cookieSecret" -}}
{{- .Values.erlangCookie.existingSecret | default "rabbitmq-erlang-cookie" }}
{{- end }}

{{/* Nom de nœud Erlang (nom long, comme l'opérateur) : rabbit@<pod>.<headless>.<namespace>,
résolu par le domaine de recherche du pod. Stable : il désigne aussi le répertoire de
données sur le PVC, ne jamais le changer sur un cluster existant. */}}
{{- define "rabbitmq.nodeName" -}}
rabbit@{{ include "rabbitmq.statefulSetName" .root }}-{{ .ordinal }}.{{ include "rabbitmq.headlessService" .root }}.{{ .root.Release.Namespace }}
{{- end }}

{{/* Quantité Kubernetes (600Mi, 1Gi, 512M…) -> octets, × facteur. */}}
{{- define "rabbitmq.memoryBytes" -}}
{{- $q := toString .quantity }}
{{- $units := dict "" 1.0 "Ki" 1024.0 "Mi" 1048576.0 "Gi" 1073741824.0 "Ti" 1099511627776.0 "k" 1000.0 "M" 1000000.0 "G" 1000000000.0 "T" 1000000000000.0 }}
{{- $number := regexFind "^[0-9]+(\\.[0-9]+)?" $q }}
{{- $unit := trimPrefix $number $q }}
{{- if or (not $number) (not (hasKey $units $unit)) }}
{{- fail (printf "resources.limits.memory=%s : quantité non reconnue (ex. 600Mi, 1Gi)" $q) }}
{{- end }}
{{- mulf (float64 $number) (get $units $unit) .factor | floor | int64 }}
{{- end }}

{{/* Configuration générée (conf.d/10-orderflow.conf) ; aussi base du checksum/config. */}}
{{- define "rabbitmq.baseConfig" -}}
# Généré par le chart rabbitmq-standalone : ne pas modifier dans le cluster.
cluster_name = rabbitmq
log.console = true
auth_mechanisms.1 = PLAIN
auth_mechanisms.2 = AMQPLAIN
cluster_partition_handling = pause_minority

# Peer discovery sans API Kubernetes : liste statique des nœuds, dérivée de replicas et du
# Service headless. Un nœud qui a déjà des données rejoint ses pairs connus sans passer par
# la découverte ; un nœud neuf rejoint le premier pair joignable déjà en cluster.
cluster_formation.peer_discovery_backend = classic_config
{{- range $i := until (int .Values.replicas) }}
cluster_formation.classic_config.nodes.{{ add $i 1 }} = {{ include "rabbitmq.nodeName" (dict "root" $ "ordinal" $i) }}
{{- end }}
# Sonde de démarrage /api/health/checks/reached-target-cluster-size.
cluster_formation.target_cluster_size_hint = {{ .Values.replicas }}

{{- with (.Values.resources.limits | default dict).memory }}

# 80 % de limits.memory (même calcul que l'opérateur) : référence des watermarks relatifs.
total_memory_available_override_value = {{ include "rabbitmq.memoryBytes" (dict "quantity" . "factor" 0.8) }}
{{- end }}

# Administrateur créé au premier démarrage du cluster. Les valeurs viennent des variables
# d'environnement du conteneur (interpolation $(…) de rabbitmq.conf), elles-mêmes lues dans
# le Secret : le mot de passe n'est jamais écrit dans une ConfigMap.
default_user = $(RABBITMQ_ADMIN_USERNAME)
default_pass = $(RABBITMQ_ADMIN_PASSWORD)
{{- end }}

{{- define "rabbitmq.enabledPlugins" -}}
{{- $plugins := concat (list "rabbitmq_management" "rabbitmq_prometheus") .Values.additionalPlugins | uniq }}
{{- printf "[%s]." (join "," $plugins) }}
{{- end }}

{{- define "rabbitmq.securityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
capabilities:
  drop: ["ALL"]
{{- end }}

{{/* Identité du nœud (comme l'opérateur) : nom long rabbit@<pod>.<headless>.<namespace>.
RABBITMQ_USE_LONGNAME vaut aussi pour les outils CLI lancés dans le conteneur. */}}
{{- define "rabbitmq.nodeEnv" -}}
- name: MY_POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: MY_POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: K8S_SERVICE_NAME
  value: {{ include "rabbitmq.headlessService" . }}
- name: RABBITMQ_USE_LONGNAME
  value: "true"
- name: RABBITMQ_NODENAME
  value: rabbit@$(MY_POD_NAME).$(K8S_SERVICE_NAME).$(MY_POD_NAMESPACE)
{{- end }}

{{/* Contenu qui doit relancer les nœuds quand il change (annotation checksum/config) :
données de la ConfigMap, sans ses labels (une nouvelle version du chart seule ne relance rien). */}}
{{- define "rabbitmq.configChecksum" -}}
{{- list (include "rabbitmq.baseConfig" .) .Values.additionalConfig (include "rabbitmq.enabledPlugins" .) (.Files.Get "files/init.sh") | toJson | sha256sum }}
{{- end }}
