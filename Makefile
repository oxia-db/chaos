JAVA_GRADLE := ./runner-java/gradlew -p runner-java

.PHONY: build test check format clean java-build java-test java-check java-format java-clean run-java

build: java-build

test: java-test

check: java-check

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
