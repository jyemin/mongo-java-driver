/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import ProjectExtensions.configureJarManifest
import ProjectExtensions.configureMavenPublication
import org.gradle.jvm.toolchain.JavaLanguageVersion
import project.DEFAULT_JAVA_VERSION

plugins {
    id("project.java")
    id("conventions.test-artifacts")
    id("conventions.test-artifacts-runtime-dependencies")
    id("conventions.test-include-optionals")
    id("conventions.testing-mockito")
    id("conventions.testing-junit")
    id("conventions.testing-spock-exclude-slow")
}

base.archivesName.set("mongodb-driver-sync")

dependencies {
    api(project(path = ":bson", configuration = "default"))
    api(project(path = ":driver-core", configuration = "default"))
    compileOnly(project(path = ":mongodb-crypt", configuration = "default"))
    implementation(project(path = ":rust-crud-bindings", configuration = "default"))

    testImplementation(project(path = ":bson", configuration = "testArtifacts"))
    testImplementation(project(path = ":driver-core", configuration = "testArtifacts"))

    optionalImplementation(platform(libs.micrometer.observation.bom))
    optionalImplementation(libs.micrometer.observation)

    // lambda testing
    testImplementation(libs.aws.lambda.core)

    // Tracing testing
    testImplementation(platform(libs.micrometer.tracing.integration.test.bom))
    testImplementation(libs.micrometer.tracing.integration.test) { exclude(group = "org.junit.jupiter") }
}

tasks.withType<Test> {
    // Require Java 23 for tests due to rust-crud-bindings dependency
    val testJavaVersion: Int = findProperty("javaVersion")?.toString()?.toInt() ?: 23
    javaLauncher.set(javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(testJavaVersion) })

    // Set the native library path for the Rust FFI library
    // Can be overridden with -PnativeLibPath=/path/to/lib
    val rustDriverDir = rootProject.file("../mongo-rust-driver")
    val nativeLibPath = findProperty("nativeLibPath")?.toString()
        ?: rustDriverDir.resolve("target/release").absolutePath
    environment("DYLD_LIBRARY_PATH", nativeLibPath)
    environment("LD_LIBRARY_PATH", nativeLibPath)

    // Needed for MicrometerProseTest to set env variable programmatically (calls
    // `field.setAccessible(true)`)
    if (testJavaVersion >= DEFAULT_JAVA_VERSION) {
        jvmArgs("--add-opens=java.base/java.util=ALL-UNNAMED")
    }
}

configureMavenPublication {
    pom {
        name.set("MongoDB Driver")
        description.set("The MongoDB Synchronous Driver")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "org.mongodb.driver.sync.client"
    attributes["Bundle-SymbolicName"] = "org.mongodb.driver-sync"
    attributes["Import-Package"] =
        listOf(
                "com.mongodb.crypt.capi.*;resolution:=optional",
                "com.mongodb.internal.crypt.capi.*;resolution:=optional",
                "*",
            )
            .joinToString(",")
}
