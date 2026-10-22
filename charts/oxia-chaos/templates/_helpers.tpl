{{/* Chart name. */}}
{{- define "oxia-chaos.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fully qualified release name. */}}
{{- define "oxia-chaos.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* Common labels. */}}
{{- define "oxia-chaos.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
app.kubernetes.io/name: {{ include "oxia-chaos.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Oxia endpoint used by every runner. */}}
{{- define "oxia-chaos.serviceAddress" -}}
{{- if .Values.serviceAddress -}}
{{- .Values.serviceAddress -}}
{{- else if (index .Values "oxia-cluster").enabled -}}
{{- printf "%s.%s.svc.cluster.local:6648" .Release.Name .Release.Namespace -}}
{{- else -}}
{{- fail "serviceAddress is required when oxia-cluster.enabled is false" -}}
{{- end -}}
{{- end }}

{{/* Service account name. */}}
{{- define "oxia-chaos.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "oxia-chaos.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name is required when serviceAccount.create is false" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Seconds in a duration such as 2h40m or 90s. Takes a dict with value and field, the value's name for errors. */}}
{{- define "oxia-chaos.durationSeconds" -}}
{{- $value := toString (required (printf "%s is required" .field) .value) -}}
{{- if not (regexMatch "^([0-9]+h)?([0-9]+m)?([0-9]+s)?$" $value) -}}
{{- fail (printf "%s must be a duration such as 2h40m, 5m or 30s, got %q" .field $value) -}}
{{- end -}}
{{- $seconds := 0 -}}
{{- range $part := regexFindAll "[0-9]+[hms]" $value -1 -}}
{{- $seconds = add $seconds (mul (regexReplaceAll "[hms]" $part "") (get (dict "h" 3600 "m" 60 "s" 1) (regexReplaceAll "[0-9]" $part ""))) -}}
{{- end -}}
{{- if eq $seconds 0 -}}
{{- fail (printf "%s must be longer than zero, got %q" .field $value) -}}
{{- end -}}
{{- $seconds -}}
{{- end }}

{{/* Workflow task that waits until every Oxia server reports healthy and the leader of every shard
of every namespace serves a range scan. Takes a dict with root and name. */}}
{{- define "oxia-chaos.healthCheckTemplate" -}}
{{- $cluster := index .root.Values "oxia-cluster" -}}
- name: {{ .name }}
  templateType: Task
  task:
    container:
      name: oxia-health
      image: {{ printf "%s:%s" $cluster.image.repository $cluster.image.tag | quote }}
      imagePullPolicy: {{ $cluster.image.pullPolicy }}
      command:
        - /bin/sh
        - -ec
      args:
        - |
          for ordinal in 0 1 2; do until oxia health --host={{ .root.Release.Name }}-$ordinal.{{ .root.Release.Name }}-svc --port=6649; do sleep 2; done; done
          for namespace in{{ range $cluster.namespaces }} {{ .name }}{{ end }}; do
            until oxia client range-scan --service-address={{ include "oxia-chaos.serviceAddress" .root }} --namespace=$namespace --request-timeout=10s --key-min=oxia-chaos-health-check --key-max=oxia-chaos-health-check0 >/dev/null; do sleep 2; done
          done
      resources:
        requests:
          cpu: 10m
          memory: 32Mi
        limits:
          memory: 64Mi
{{- end }}

{{/* Workflow template body that injects one pipeline stage's fault once. Takes a dict with root and stage. */}}
{{- define "oxia-chaos.stageFault" -}}
{{- $stage := .stage -}}
{{- $field := printf "chaosWorkflow.pipeline.stages[%s]" $stage.name -}}
{{- if eq $stage.fault "pod-kill" }}
templateType: PodChaos
deadline: 10s
podChaos:
  action: pod-kill
  mode: one
  selector:
    {{- include "oxia-chaos.serverSelector" .root | nindent 4 }}
{{- else if eq $stage.fault "container-kill" }}
{{- if not (has $stage.container (list "server" "coordinator")) }}
{{- fail (printf "%s.container must be server or coordinator, got %q" $field (toString $stage.container)) }}
{{- end }}
templateType: PodChaos
deadline: 10s
podChaos:
  action: container-kill
  containerNames:
    - {{ $stage.container | quote }}
  mode: one
  selector:
    {{- include "oxia-chaos.serverSelector" .root | nindent 4 }}
{{- else }}
{{- fail (printf "%s.fault must be pod-kill or container-kill, got %q" $field (toString $stage.fault)) }}
{{- end }}
{{- end }}

{{/* Chaos Mesh selector for the Oxia server pods of this release. */}}
{{- define "oxia-chaos.serverSelector" -}}
namespaces:
  - {{ .Release.Namespace | quote }}
labelSelectors:
  app.kubernetes.io/name: oxia-cluster
  app.kubernetes.io/component: server
  app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end }}
