/*
 * Copyright © 2026 The Oxia Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    application
    java
    id("com.diffplug.spotless") version "8.3.0"
}

group = "io.oxia.chaos"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    implementation(platform("io.opentelemetry:opentelemetry-bom:1.63.0"))
    implementation("io.github.oxia-db:oxia-client:0.9.4")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.27.7")
    testImplementation("org.mockito:mockito-junit-jupiter:5.14.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "io.oxia.chaos.runner.OxiaChaosRunner"
}

tasks.test {
    useJUnitPlatform()
}

spotless {
    java {
        googleJavaFormat()
        importOrder()
        removeUnusedImports()
    }
}
