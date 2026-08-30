JAVA_GRADLE := ./runner-java/gradlew -p runner-java
JAVA_IMAGE ?= oxia/chaos-java:latest
DOCKER ?= docker
HELM ?= helm
CHART := charts/oxia-chaos

.PHONY: build test e2e-test check format clean java-build java-test java-e2e-test java-check java-format java-clean java-image run-java chart-deps chart-lint chart-template deploy-up deploy-down

build: java-build chart-lint

test: java-test

e2e-test: java-e2e-test

check: java-check chart-lint

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

chart-deps:
	$(HELM) dependency build $(CHART)

chart-lint: chart-deps
	$(HELM) lint --strict $(CHART)

chart-template: chart-deps
	$(HELM) template oxia-chaos $(CHART) --namespace oxia-chaos

deploy-up:
	$(MAKE) -C deploy up

deploy-down:
	$(MAKE) -C deploy down
