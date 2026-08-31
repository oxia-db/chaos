JAVA_GRADLE := ./runner-java/gradlew -p runner-java
JAVA_IMAGE ?= oxia/chaos-java:latest
DOCKER ?= docker
HELM ?= helm
CHART := charts/oxia-chaos
OXIA_CHART_REPOSITORY := https://oxia-db.github.io/helm-charts/
CHAOS_MESH_CHART_REPOSITORY := https://charts.chaos-mesh.org

.PHONY: build test e2e-test check format clean java-build java-test java-e2e-test java-check java-format java-clean java-image run-java status-test observability-check observability-provision chart-deps chart-lint chart-template deploy-up deploy-down

build: java-build chart-lint

test: java-test

e2e-test: java-e2e-test

check: java-check status-test observability-check chart-lint

format: java-format

clean: java-clean

java-build:
	$(JAVA_GRADLE) build

java-test:
	$(JAVA_GRADLE) test

java-e2e-test:
	$(JAVA_GRADLE) e2eTest

java-check:
	$(JAVA_GRADLE) check

java-format:
	$(JAVA_GRADLE) spotlessApply

java-clean:
	$(JAVA_GRADLE) clean

java-image:
	$(DOCKER) build --tag $(JAVA_IMAGE) runner-java

run-java:
	$(JAVA_GRADLE) run --args='$(ARGS)'

status-test:
	python3 -m unittest discover -s scripts/status -p 'test_*.py'

observability-check:
	python3 -m unittest discover -s scripts/observability -p 'test_*.py'
	@for dashboard in observability/dashboards/*.json; do jq --exit-status . "$$dashboard" >/dev/null; done

observability-provision:
	python3 scripts/observability/provision.py

chart-deps:
	$(HELM) repo add oxia-chaos-oxia $(OXIA_CHART_REPOSITORY) --force-update
	$(HELM) repo add oxia-chaos-chaos-mesh $(CHAOS_MESH_CHART_REPOSITORY) --force-update
	$(HELM) dependency build --skip-refresh $(CHART)

chart-lint: chart-deps
	$(HELM) lint --strict $(CHART)

chart-template: chart-deps
	$(HELM) template oxia-chaos $(CHART) --namespace oxia-chaos

deploy-up:
	$(MAKE) -C deploy up

deploy-down:
	$(MAKE) -C deploy down
