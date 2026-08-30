# Copyright © 2026 The Oxia Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM eclipse-temurin:21-jdk-noble AS build

WORKDIR /workspace
COPY gradle gradle
COPY gradlew gradlew
COPY settings.gradle.kts build.gradle.kts ./
COPY src src
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:21-jre-noble

WORKDIR /opt/oxia-chaos
COPY --from=build /workspace/build/install/oxia-chaos/ ./
USER 10001:10001
ENTRYPOINT ["/opt/oxia-chaos/bin/oxia-chaos"]
