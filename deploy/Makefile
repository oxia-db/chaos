CLUSTER_NAME ?= oxia-chaos
KUBE_CONTEXT ?= kind-$(CLUSTER_NAME)
NAMESPACE ?= oxia-chaos
RELEASE_NAME ?= oxia-chaos
KIND ?= kind
KUBECTL ?= kubectl
HELM ?= helm
DOCKER ?= docker
JAVA_IMAGE ?= oxia/chaos-java:latest
HELM_ARGS ?=
CORRECTNESS_PROFILE ?= five-cycle
CORRECTNESS_VICTIM_ORDINAL ?=
RUNNER_TIMEOUT_SECONDS ?= 25200
RUNNER_POLL_INTERVAL ?= 10
WORKFLOW_TIMEOUT_SECONDS ?= 21600
EXPECTED_CHAOS_INJECTIONS ?= 1

.NOTPARALLEL:
.PHONY: up down cluster namespaces dependencies dependency-images runner-image release correctness correctness-test test status

up: release correctness status

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

dependency-images: cluster dependencies
	@set -eu; \
	node=$$($(KIND) get nodes --name $(CLUSTER_NAME) | head -n 1); \
	case "$$($(DOCKER) exec "$$node" uname -m)" in \
		x86_64) platform=linux/amd64 ;; \
		aarch64|arm64) platform=linux/arm64 ;; \
		*) echo "Unsupported kind node architecture" >&2; exit 1 ;; \
	esac; \
	nodes=$$($(KIND) get nodes --name $(CLUSTER_NAME)); \
	images=$$($(HELM) template $(RELEASE_NAME) ../charts/oxia-chaos \
		--namespace $(NAMESPACE) \
		--set runners.java.enabled=false | \
		awk '$$1 == "image:" { gsub(/"/, "", $$2); print $$2 }' | sort -u); \
	for image in $$images; do \
		$(DOCKER) pull --platform "$$platform" "$$image"; \
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

release: namespaces dependency-images runner-image
	$(HELM) upgrade --install $(RELEASE_NAME) ../charts/oxia-chaos \
		--kube-context $(KUBE_CONTEXT) \
		--namespace $(NAMESPACE) \
		--set runners.java.image.pullPolicy=IfNotPresent \
		--wait $(HELM_ARGS)

correctness:
	$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) wait \
		--for=condition=Ready pod \
		--selector=app.kubernetes.io/instance=$(RELEASE_NAME),app.kubernetes.io/component=runner \
		--timeout=3m
	$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) delete workflows \
		--selector=app.kubernetes.io/instance=$(RELEASE_NAME),app.kubernetes.io/component=correctness \
		--ignore-not-found=true
	@set -eu; \
	victim_ordinal='$(CORRECTNESS_VICTIM_ORDINAL)'; \
	if [ -z "$$victim_ordinal" ]; then \
		victim_ordinal=$$(awk 'BEGIN { srand(); print int(rand() * 3) }'); \
	fi; \
	case "$$victim_ordinal" in 0|1|2) ;; *) echo "Invalid Oxia victim ordinal: $$victim_ordinal" >&2; exit 1 ;; esac; \
	echo "Selected Oxia victim pod $(RELEASE_NAME)-$$victim_ordinal"; \
	for attempt in $$(seq 1 30); do \
		if $(HELM) template $(RELEASE_NAME) ../charts/oxia-chaos \
			--namespace $(NAMESPACE) \
			--set runners.java.enabled=false \
			--set chaosWorkflow.enabled=true \
			--set-string chaosWorkflow.profile=$(CORRECTNESS_PROFILE) \
			--set chaosWorkflow.victimOrdinal=$$victim_ordinal \
			--show-only templates/correctness-workflow.yaml | \
			$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) apply --filename -; then \
			exit 0; \
		fi; \
		if [ "$$attempt" -eq 30 ]; then \
			echo "Failed to submit correctness workflow" >&2; \
			exit 1; \
		fi; \
		sleep 2; \
	done

correctness-test:
	@set -eu; \
	selector='app.kubernetes.io/instance=$(RELEASE_NAME),app.kubernetes.io/component=correctness'; \
	workflow_name=''; \
	for attempt in $$(seq 1 30); do \
		workflow_name=$$($(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) \
			get workflows --selector="$$selector" \
			--output=jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | head -n 1); \
		if [ -n "$$workflow_name" ]; then break; fi; \
		sleep 2; \
	done; \
	if [ -z "$$workflow_name" ]; then \
		echo "No correctness workflow found" >&2; \
		exit 1; \
	fi; \
	$(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) wait \
		--for=condition=Accomplished=True "workflow/$$workflow_name" \
		--timeout=$(WORKFLOW_TIMEOUT_SECONDS)s; \
	failed_tasks=$$($(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) \
		get pods --selector="chaos-mesh.org/workflow=$$workflow_name" --output=json | \
		jq '[.items[] | select(.status.phase == "Failed")] | length'); \
	if [ "$$failed_tasks" -gt 0 ]; then \
		echo "Correctness workflow has $$failed_tasks failed health tasks" >&2; \
		exit 1; \
	fi; \
	injected=$$($(KUBECTL) --context $(KUBE_CONTEXT) --namespace $(NAMESPACE) \
		get workflownodes --selector="chaos-mesh.org/workflow=$$workflow_name" --output=json | \
		jq '[.items[] | select(any(.status.conditions[]?; .type == "ChaosInjected" and .status == "True"))] | length'); \
	if [ "$$injected" -lt $(EXPECTED_CHAOS_INJECTIONS) ]; then \
		echo "Expected at least $(EXPECTED_CHAOS_INJECTIONS) chaos injections, observed $$injected" >&2; \
		exit 1; \
	fi; \
	echo "Correctness workflow $$workflow_name completed with $$injected chaos injections"

test:
	@started_at=$$(date +%s); \
	selector='app.kubernetes.io/instance=$(RELEASE_NAME),app.kubernetes.io/component=runner'; \
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

status:
	$(KUBECTL) --context $(KUBE_CONTEXT) get pods --all-namespaces

down:
	$(KIND) delete cluster --name $(CLUSTER_NAME)
