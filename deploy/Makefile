CLUSTER_NAME ?= oxia-chaos
KUBE_CONTEXT ?= kind-$(CLUSTER_NAME)
KIND ?= kind
KUBECTL ?= kubectl
HELM ?= helm

.NOTPARALLEL:
.PHONY: up down cluster namespaces dependencies release test status

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

release: namespaces dependencies
	$(HELM) upgrade --install oxia-chaos ../charts/oxia-chaos \
		--kube-context $(KUBE_CONTEXT) \
		--namespace oxia-chaos \
		--values ../charts/oxia-chaos/values-java.yaml \
		--values values/kind.yaml \
		--wait

test:
	$(HELM) test oxia-chaos \
		--kube-context $(KUBE_CONTEXT) \
		--namespace oxia-chaos \
		--logs

status:
	$(KUBECTL) --context $(KUBE_CONTEXT) get pods --all-namespaces

down:
	$(KIND) delete cluster --name $(CLUSTER_NAME)
