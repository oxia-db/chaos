CLUSTER_NAME ?= oxia-chaos
KUBE_CONTEXT ?= kind-$(CLUSTER_NAME)
NAMESPACE ?= oxia-chaos
RELEASE_NAME ?= oxia-chaos
INFRA_RELEASE_NAME ?= $(RELEASE_NAME)-infra
KIND ?= kind
KUBECTL ?= kubectl
HELM ?= helm
DOCKER ?= docker
JAVA_IMAGE ?= oxia/chaos-java:latest
HELM_ARGS ?=
CHANNEL_CONFIG ?= ../config/oxia-channels.json
CHANNELS ?= stable
WORKLOAD_PROFILE ?= six-hour
REUSE_CHANNEL_PLAN ?= false
RUNNER_IMAGE_PULL_POLICY ?= IfNotPresent
PRELOAD_DEPENDENCY_IMAGES ?= true
WORK_DIR ?= .work
CHANNEL_PLAN ?= $(WORK_DIR)/oxia-channels.json
RUNNER_TIMEOUT_SECONDS ?= 25200
RUNNER_POLL_INTERVAL ?= 10
WORKFLOW_TIMEOUT_SECONDS ?= 21600
EXPECTED_CHAOS_INJECTIONS ?= 1
STATUS_OUTPUT_DIR ?= ../status-result
RUN_DATE ?= $(shell date -u +%F)
RUN_URL ?= https://github.com/oxia-db/chaos/actions
IMAGE_OUTCOME ?= skipped
VERIFY_OUTCOME ?= skipped
CLUSTER_OUTCOME ?= skipped
INSTALL_OUTCOME ?= skipped
WORKFLOW_OUTCOME ?= skipped
CHAOS_OUTCOME ?= skipped
OBSERVABILITY_ENABLED ?= false
OBSERVABILITY_SECRET_NAME ?= $(INFRA_RELEASE_NAME)-grafana-cloud
OBSERVABILITY_COLLECTOR_ENDPOINT ?= http://$(INFRA_RELEASE_NAME)-observability.$(NAMESPACE).svc.cluster.local:4317
GRAFANA_CLOUD_OTLP_ENDPOINT ?=
GRAFANA_CLOUD_OTLP_INSTANCE_ID ?=
GRAFANA_CLOUD_TOKEN ?=
GRAFANA_CLOUD_GRAFANA_URL ?=
GRAFANA_CLOUD_SERVICE_ACCOUNT_TOKEN ?=

.NOTPARALLEL:
.PHONY: up run down cluster namespaces dependencies channel-plan dependency-images runner-image observability-secret observability-annotations release release-infra release-clusters release-runners correctness correctness-test test status-results diagnostics status

up: runner-image release correctness status

cluster:
	@if $(KIND) get clusters | grep -qx '$(CLUSTER_NAME)'; then \
		echo "kind cluster $(CLUSTER_NAME) already exists"; \
	else \
		$(KIND) create cluster --name $(CLUSTER_NAME) --config kind/cluster.yaml; \
	fi

namespaces: cluster
	$(KUBECTL) --context $(KUBE_CONTEXT) apply --filename namespaces.yaml

dependencies:
	$(MAKE) -C .. chart-deps

channel-plan: dependencies
	@set -eu; \
	mkdir -p '$(WORK_DIR)'; \
	if [ '$(REUSE_CHANNEL_PLAN)' = true ] && [ -s '$(CHANNEL_PLAN)' ]; then \
		jq . '$(CHANNEL_PLAN)'; \
		exit 0; \
	fi; \
	plan='{}'; \
	selected=0; \
	for channel in $(CHANNELS); do \
		case "$$channel" in ''|*[!a-z0-9-]*) echo "Invalid channel ID: $$channel" >&2; exit 1 ;; esac; \
		entry=$$(jq --compact-output --exit-status --arg channel "$$channel" '.[$$channel]' '$(CHANNEL_CONFIG)'); \
		source_tag=$$(printf '%s' "$$entry" | jq --raw-output '.imageTag'); \
		server_version=$$(printf '%s' "$$entry" | jq --raw-output '.serverVersion'); \
		resolved_tag="$$source_tag"; \
		if [ "$$(printf '%s' "$$entry" | jq --raw-output '.resolveDigest')" = true ]; then \
			digest=$$($(DOCKER) buildx imagetools inspect "oxia/oxia:$$source_tag" | \
				awk '$$1 == "Digest:" { print $$2; exit }'); \
			case "$$digest" in sha256:*) ;; *) echo "Unable to resolve oxia/oxia:$$source_tag" >&2; exit 1 ;; esac; \
			resolved_tag="$$source_tag@$$digest"; \
		fi; \
		entry=$$(printf '%s' "$$entry" | jq --compact-output \
			--arg sourceTag "$$source_tag" \
			--arg imageTag "$$resolved_tag" \
			--arg serverVersion "$$server_version" \
			'. + {sourceTag: $$sourceTag, imageTag: $$imageTag, serverVersion: $$serverVersion}'); \
		plan=$$(jq --compact-output --null-input \
			--argjson plan "$$plan" --arg channel "$$channel" --argjson entry "$$entry" \
			'$$plan + {($$channel): $$entry}'); \
		selected=$$((selected + 1)); \
	done; \
	if [ "$$selected" -eq 0 ]; then echo "At least one channel is required" >&2; exit 1; fi; \
	tmp='$(CHANNEL_PLAN).tmp'; \
	printf '%s\n' "$$plan" > "$$tmp"; \
	mv "$$tmp" '$(CHANNEL_PLAN)'; \
	jq . '$(CHANNEL_PLAN)'

dependency-images: cluster channel-plan
	@set -eu; \
	if [ '$(PRELOAD_DEPENDENCY_IMAGES)' != true ]; then \
		echo "Skipping dependency image preload; kind nodes will pull pinned images directly"; \
		exit 0; \
	fi; \
	node=$$($(KIND) get nodes --name $(CLUSTER_NAME) | head -n 1); \
	case "$$($(DOCKER) exec "$$node" uname -m)" in \
		x86_64) platform=linux/amd64 ;; \
		aarch64|arm64) platform=linux/arm64 ;; \
		*) echo "Unsupported kind node architecture" >&2; exit 1 ;; \
	esac; \
	nodes=$$($(KIND) get nodes --name $(CLUSTER_NAME)); \
	images=$$({ \
		$(HELM) template $(INFRA_RELEASE_NAME) ../charts/oxia-chaos \
			--namespace $(NAMESPACE) \
			--set oxia-cluster.enabled=false \
			--set runnerWorkloadsEnabled=false \
			--set chaosWorkflow.enabled=false \
			--set chaos-mesh.enabled=true \
			--set-string channel=infra \
			--set-string testProfile=$(WORKLOAD_PROFILE) $(HELM_ARGS); \
		for channel in $$(jq --raw-output 'keys_unsorted[]' '$(CHANNEL_PLAN)'); do \
			image_tag=$$(jq --raw-output --arg channel "$$channel" '.[$$channel].imageTag' '$(CHANNEL_PLAN)'); \
			$(HELM) template "$(RELEASE_NAME)-$$channel" ../charts/oxia-chaos \
				--namespace $(NAMESPACE) \
				--set runnerWorkloadsEnabled=false \
				--set chaosWorkflow.enabled=false \
				--set chaos-mesh.enabled=false \
				--set-string channel="$$channel" \
				--set-string testProfile=$(WORKLOAD_PROFILE) \
				--set-string oxia-cluster.image.tag="$$image_tag" $(HELM_ARGS); \
		done; \
	} | awk '$$1 == "image:" { gsub(/"/, "", $$2); print $$2 }' | sort -u); \
	for image in $$images; do \
		if ! $(DOCKER) image inspect "$$image" >/dev/null 2>&1; then \
			$(DOCKER) pull --platform "$$platform" "$$image"; \
		else \
			echo "Using cached image $$image"; \
		fi; \
		for node in $$nodes; do \
			$(DOCKER) save "$$image" | \
				$(DOCKER) exec --privileged --interactive "$$node" \
					ctr --namespace=k8s.io images import \
					--platform="$$platform" --digests --snapshotter=overlayfs -; \
		done; \
	done

runner-image: cluster
	$(MAKE) -C .. java-image JAVA_IMAGE=$(JAVA_IMAGE)
	$(KIND) load docker-image $(JAVA_IMAGE) --name $(CLUSTER_NAME)

observability-secret: namespaces
	@set -eu; \
	if [ '$(OBSERVABILITY_ENABLED)' != true ]; then \
		echo "Grafana Cloud observability is disabled"; \
		exit 0; \
	fi; \
	if [ -z '$(GRAFANA_CLOUD_OTLP_ENDPOINT)' ] || [ -z '$(GRAFANA_CLOUD_OTLP_INSTANCE_ID)' ] || [ -z '$(GRAFANA_CLOUD_TOKEN)' ]; then \
		echo "Grafana Cloud OTLP endpoint, instance ID, and token are required" >&2; \
		exit 1; \
	fi; \
	{ \
		printf 'endpoint=%s\n' '$(GRAFANA_CLOUD_OTLP_ENDPOINT)'; \
		printf 'instance-id=%s\n' '$(GRAFANA_CLOUD_OTLP_INSTANCE_ID)'; \
		printf 'token=%s\n' '$(GRAFANA_CLOUD_TOKEN)'; \
	} | $(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) create secret generic '$(OBSERVABILITY_SECRET_NAME)' \
		--from-env-file=/dev/stdin --dry-run=client --output=yaml | \
		$(KUBECTL) --context $(KUBE_CONTEXT) apply --filename -

release-infra: namespaces dependency-images observability-secret
	$(HELM) upgrade --install $(INFRA_RELEASE_NAME) ../charts/oxia-chaos \
		--kube-context $(KUBE_CONTEXT) \
		--namespace $(NAMESPACE) \
		--set oxia-cluster.enabled=false \
		--set runnerWorkloadsEnabled=false \
		--set chaosWorkflow.enabled=false \
		--set chaos-mesh.enabled=true \
		--set observability.enabled=$(OBSERVABILITY_ENABLED) \
		--set-string observability.existingSecret=$(OBSERVABILITY_SECRET_NAME) \
		--set-string channel=infra \
		--set-string testProfile=$(WORKLOAD_PROFILE) \
		--wait --timeout 10m $(HELM_ARGS)

release-clusters: namespaces dependency-images
	@set -eu; \
	for channel in $$(jq --raw-output 'keys_unsorted[]' '$(CHANNEL_PLAN)'); do \
		image_tag=$$(jq --raw-output --arg channel "$$channel" '.[$$channel].imageTag' '$(CHANNEL_PLAN)'); \
		$(HELM) upgrade --install "$(RELEASE_NAME)-$$channel" ../charts/oxia-chaos \
			--kube-context $(KUBE_CONTEXT) \
			--namespace $(NAMESPACE) \
			--set runnerWorkloadsEnabled=false \
			--set chaosWorkflow.enabled=false \
			--set chaos-mesh.enabled=false \
			--set-string channel="$$channel" \
			--set-string testProfile=$(WORKLOAD_PROFILE) \
			--set-string oxia-cluster.image.tag="$$image_tag" \
			--wait --timeout 10m $(HELM_ARGS); \
	done

release-runners: release-infra release-clusters
	@set -eu; \
	for channel in $$(jq --raw-output 'keys_unsorted[]' '$(CHANNEL_PLAN)'); do \
		image_tag=$$(jq --raw-output --arg channel "$$channel" '.[$$channel].imageTag' '$(CHANNEL_PLAN)'); \
		$(HELM) upgrade --install "$(RELEASE_NAME)-$$channel" ../charts/oxia-chaos \
			--kube-context $(KUBE_CONTEXT) \
			--namespace $(NAMESPACE) \
			--set runnerWorkloadsEnabled=true \
			--set-string runnerImagePullPolicyOverride=$(RUNNER_IMAGE_PULL_POLICY) \
			--set runnerJob.telemetry.enabled=$(OBSERVABILITY_ENABLED) \
			--set-string runnerJob.telemetry.otlpEndpoint=$(OBSERVABILITY_COLLECTOR_ENDPOINT) \
			--set chaosWorkflow.enabled=false \
			--set chaos-mesh.enabled=false \
			--set-string channel="$$channel" \
			--set-string testProfile=$(WORKLOAD_PROFILE) \
			--set-string oxia-cluster.image.tag="$$image_tag" \
			--wait --timeout 5m $(HELM_ARGS); \
	done

release: release-runners

run: release correctness correctness-test test

correctness:
	@set -eu; \
	for channel in $$(jq --raw-output 'keys_unsorted[]' '$(CHANNEL_PLAN)'); do \
		release="$(RELEASE_NAME)-$$channel"; \
		$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) wait \
			--for=condition=Ready pod \
			--selector="app.kubernetes.io/instance=$$release,app.kubernetes.io/component=runner" \
			--timeout=3m; \
	done; \
	$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) delete workflows \
		--selector=app.kubernetes.io/component=correctness \
		--ignore-not-found=true; \
	for channel in $$(jq --raw-output 'keys_unsorted[]' '$(CHANNEL_PLAN)'); do \
		release="$(RELEASE_NAME)-$$channel"; \
		image_tag=$$(jq --raw-output --arg channel "$$channel" '.[$$channel].imageTag' '$(CHANNEL_PLAN)'); \
		for attempt in $$(seq 1 30); do \
			if $(HELM) template "$$release" ../charts/oxia-chaos \
				--namespace $(NAMESPACE) \
				--set runnerWorkloadsEnabled=false \
				--set chaosWorkflow.enabled=true \
				--set-string chaosWorkflow.profile=$(WORKLOAD_PROFILE) \
				--set chaos-mesh.enabled=false \
				--set-string channel="$$channel" \
				--set-string testProfile=$(WORKLOAD_PROFILE) \
				--set-string oxia-cluster.image.tag="$$image_tag" $(HELM_ARGS) \
				--show-only templates/correctness-workflow.yaml | \
				$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) apply --filename -; then \
				break; \
			fi; \
			if [ "$$attempt" -eq 30 ]; then echo "Failed to submit correctness workflow for $$channel" >&2; exit 1; fi; \
			sleep 2; \
		done; \
	done

correctness-test:
	@set -u; \
	result=0; \
	for channel in $$(jq --raw-output 'keys_unsorted[]' '$(CHANNEL_PLAN)'); do \
		release="$(RELEASE_NAME)-$$channel"; \
		selector="app.kubernetes.io/instance=$$release,app.kubernetes.io/component=correctness"; \
		workflow_name=''; \
		for attempt in $$(seq 1 30); do \
			workflow_name=$$($(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) \
				get workflows --selector="$$selector" \
				--output=jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | head -n 1); \
			if [ -n "$$workflow_name" ]; then break; fi; \
			sleep 2; \
		done; \
		if [ -z "$$workflow_name" ]; then echo "No correctness workflow found for $$channel" >&2; result=1; continue; fi; \
		if ! $(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) wait \
			--for=condition=Accomplished=True "workflow/$$workflow_name" \
			--timeout=$(WORKFLOW_TIMEOUT_SECONDS)s; then result=1; continue; fi; \
		failed_tasks=$$($(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) \
			get pods --selector="chaos-mesh.org/workflow=$$workflow_name" --output=json | \
			jq '[.items[] | select(.status.phase == "Failed")] | length'); \
		injected=$$($(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) \
			get workflownodes --selector="chaos-mesh.org/workflow=$$workflow_name" --output=json | \
			jq '[.items[] | select(any(.status.conditions[]?; .type == "ChaosInjected" and .status == "True"))] | length'); \
		if [ "$$failed_tasks" -gt 0 ] || [ "$$injected" -lt $(EXPECTED_CHAOS_INJECTIONS) ]; then \
			echo "Correctness workflow $$workflow_name failed validation: failedTasks=$$failed_tasks injections=$$injected" >&2; \
			result=1; \
		else \
			echo "Correctness workflow $$workflow_name completed with $$injected chaos injections"; \
		fi; \
	done; \
	exit $$result

observability-annotations:
	@set -eu; \
	if [ -z '$(GRAFANA_CLOUD_GRAFANA_URL)' ] || [ -z '$(GRAFANA_CLOUD_SERVICE_ACCOUNT_TOKEN)' ]; then \
		echo "Skipping Grafana chaos annotations; dashboard URL or service token is unavailable"; \
		exit 0; \
	fi; \
	for channel in $$(jq --raw-output 'keys_unsorted[]' '$(CHANNEL_PLAN)'); do \
		release="$(RELEASE_NAME)-$$channel"; \
		workflow_name=$$($(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) \
			get workflows --selector="app.kubernetes.io/instance=$$release,app.kubernetes.io/component=correctness" \
			--output=jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | head -n 1); \
		if [ -z "$$workflow_name" ]; then continue; fi; \
		$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) \
			get workflownodes --selector="chaos-mesh.org/workflow=$$workflow_name" --output=json | \
			GRAFANA_CLOUD_GRAFANA_URL='$(GRAFANA_CLOUD_GRAFANA_URL)' \
			GRAFANA_CLOUD_SERVICE_ACCOUNT_TOKEN='$(GRAFANA_CLOUD_SERVICE_ACCOUNT_TOKEN)' \
			python3 ../scripts/observability/publish_annotations.py --channel "$$channel" --run-url '$(RUN_URL)'; \
	done

test:
	@started_at=$$(date +%s); \
	channel_csv=$$(jq --raw-output 'keys_unsorted | join(",")' '$(CHANNEL_PLAN)'); \
	selector="app.kubernetes.io/part-of=oxia-chaos,app.kubernetes.io/component=runner,oxia.chaos/channel in ($$channel_csv)"; \
	result=0; \
	while true; do \
		statuses=$$($(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) get jobs \
			--selector="$$selector" \
			--output=jsonpath='{range .items[*]}{.status.succeeded},{.status.failed}{"\n"}{end}'); \
		if [ -n "$$statuses" ]; then \
			failed=$$(printf '%s\n' "$$statuses" | awk -F, '$$2 + 0 > 0 { count++ } END { print count + 0 }'); \
			pending=$$(printf '%s\n' "$$statuses" | awk -F, '$$1 + 0 < 1 && $$2 + 0 < 1 { count++ } END { print count + 0 }'); \
			if [ "$$failed" -gt 0 ]; then result=1; break; fi; \
			if [ "$$pending" -eq 0 ]; then break; fi; \
		fi; \
		if [ $$(( $$(date +%s) - started_at )) -ge $(RUNNER_TIMEOUT_SECONDS) ]; then result=124; break; fi; \
		sleep $(RUNNER_POLL_INTERVAL); \
	done; \
	$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) logs \
		--selector="$$selector" --all-containers=true --prefix=true || true; \
	exit $$result

status-results:
	python3 ../scripts/status/collect.py \
		--channel-config '$(CHANNEL_CONFIG)' \
		--channel-plan '$(CHANNEL_PLAN)' \
		--channels $(CHANNELS) \
		--chart ../charts/oxia-chaos \
		--workload-profile '$(WORKLOAD_PROFILE)' \
		--release-name '$(RELEASE_NAME)' \
		--namespace '$(NAMESPACE)' \
		--kube-context '$(KUBE_CONTEXT)' \
		--output-dir '$(STATUS_OUTPUT_DIR)' \
		--run-date '$(RUN_DATE)' \
		--run-url '$(RUN_URL)' \
		--image-outcome '$(IMAGE_OUTCOME)' \
		--verify-outcome '$(VERIFY_OUTCOME)' \
		--cluster-outcome '$(CLUSTER_OUTCOME)' \
		--install-outcome '$(INSTALL_OUTCOME)' \
		--workflow-outcome '$(WORKFLOW_OUTCOME)' \
		--chaos-outcome '$(CHAOS_OUTCOME)'

diagnostics:
	-@for pod in $$($(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) get pods \
		--selector=app.kubernetes.io/part-of=oxia-chaos,app.kubernetes.io/component=runner \
		--field-selector=status.phase=Running --output=name); do \
		$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) exec "$$pod" -- \
			/bin/sh -c 'kill -3 1'; \
	done
	-@sleep 2
	-$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) get \
		pods,jobs,statefulsets,workflows,workflownodes,podchaos,networkchaos,stresschaos \
		--output=wide
	-$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) get events \
		--sort-by=.lastTimestamp
	-$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) logs \
		--selector=app.kubernetes.io/part-of=oxia-chaos,app.kubernetes.io/component=runner \
		--all-containers=true --prefix=true --tail=500
	-$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) logs \
		--selector=chaos-mesh.org/workflow \
		--all-containers=true --prefix=true --tail=200
	-$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) logs \
		--selector=app.kubernetes.io/name=oxia-cluster,app.kubernetes.io/component=server \
		--all-containers=true --prefix=true --tail=200

status:
	$(KUBECTL) --context $(KUBE_CONTEXT) get pods --all-namespaces

down:
	$(KIND) delete cluster --name $(CLUSTER_NAME)
