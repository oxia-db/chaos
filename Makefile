JAVA_GRADLE := ./runner-java/gradlew -p runner-java
HELM ?= helm
CHART := charts/oxia-chaos
JAVA_VALUES := $(CHART)/values-java.yaml

.PHONY: build test check format clean java-build java-test java-check java-format java-clean run-java chart-deps chart-lint chart-template deploy-up deploy-down

build: java-build chart-lint

test: java-test

check: java-check chart-lint

format: java-format

clean: java-clean

java-build:
	$(JAVA_GRADLE) build

java-test:
	$(JAVA_GRADLE) test

java-check:
	$(JAVA_GRADLE) check

java-format:
	$(JAVA_GRADLE) spotlessApply

java-clean:
	$(JAVA_GRADLE) clean

run-java:
	$(JAVA_GRADLE) run --args='$(ARGS)'

chart-deps:
	$(HELM) dependency build $(CHART)

chart-lint: chart-deps
	$(HELM) lint --strict $(CHART) --values $(JAVA_VALUES)

chart-template: chart-deps
	$(HELM) template oxia-chaos $(CHART) --namespace oxia-chaos --values $(JAVA_VALUES)

deploy-up:
	$(MAKE) -C deploy up

deploy-down:
	$(MAKE) -C deploy down
