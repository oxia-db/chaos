CLUSTER_NAME ?= oxia-chaos
KUBE_CONTEXT ?= kind-$(CLUSTER_NAME)
KIND ?= kind
KUBECTL ?= kubectl
HELM ?= helm
JAVA_IMAGE ?= oxia/chaos-java:latest
HELM_ARGS ?=
RUNNER_TIMEOUT_SECONDS ?= 25200
RUNNER_POLL_INTERVAL ?= 10

.NOTPARALLEL:
.PHONY: up down cluster namespaces dependencies runner-image release test status

up: release status

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

runner-image: cluster
	$(MAKE) -C .. java-image JAVA_IMAGE=$(JAVA_IMAGE)
	$(KIND) load docker-image $(JAVA_IMAGE) --name $(CLUSTER_NAME)

release: namespaces dependencies runner-image
	$(HELM) upgrade --install oxia-chaos ../charts/oxia-chaos \
		--kube-context $(KUBE_CONTEXT) \
		--namespace oxia-chaos \
		--wait $(HELM_ARGS)

test:
	@started_at=$$(date +%s); \
	selector='app.kubernetes.io/instance=oxia-chaos,app.kubernetes.io/component=runner'; \
	result=0; \
	while true; do \
		statuses=$$($(KUBECTL) --context $(KUBE_CONTEXT) --namespace oxia-chaos get jobs \
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
	$(KUBECTL) --context $(KUBE_CONTEXT) --namespace oxia-chaos logs \
		--selector="$$selector" --all-containers=true --prefix=true || true; \
	exit $$result

status:
	$(KUBECTL) --context $(KUBE_CONTEXT) get pods --all-namespaces

down:
	$(KIND) delete cluster --name $(CLUSTER_NAME)
