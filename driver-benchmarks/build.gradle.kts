/*
 * Copyright 2016-present MongoDB, Inc.
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

import org.gradle.jvm.toolchain.JavaLanguageVersion

plugins {
    id("application")
    id("java-library")
    id("project.base")
}

application {
    mainClass = "com.mongodb.benchmark.benchmarks.BenchmarkSuite"
}

sourceSets {
    main {
        java { setSrcDirs(listOf("src/main")) }
        resources { setSrcDirs(listOf("src/resources")) }
    }
}

dependencies {
    api(project(":driver-sync"))
    api(project(":mongodb-crypt"))

    implementation(platform(libs.netty.bom))
    implementation(libs.bundles.netty)

    implementation(libs.logback.classic)
    implementation(libs.jmh.core)
    annotationProcessor(libs.jmh.generator.annprocess)

}

// Configure all JavaExec tasks for native driver (Java 23 + native library path)
tasks.withType<JavaExec>().configureEach {
    javaLauncher.set(javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(23) })

    val rustDriverDir = rootProject.file("../mongo-rust-driver")
    val nativeLibPath = findProperty("nativeLibPath")?.toString()
        ?: rustDriverDir.resolve("target/release").absolutePath
    environment("DYLD_LIBRARY_PATH", nativeLibPath)
    environment("LD_LIBRARY_PATH", nativeLibPath)

    jvmArgs("--enable-native-access=ALL-UNNAMED")

    // Pass through benchmark properties
    systemProperty("org.mongodb.benchmarks.data", System.getProperty("org.mongodb.benchmarks.data") ?: "")
    systemProperty("org.mongodb.benchmarks.output", System.getProperty("org.mongodb.benchmarks.output") ?: "")
}

tasks.register<JavaExec>("jmh") {
    group = "benchmark"
    description = "Run JMH benchmarks."
    mainClass = "org.openjdk.jmh.Main"
    classpath = sourceSets.main.get().runtimeClasspath
}

tasks.register<JavaExec>("runNetty") {
    group = "application"
    description = "Run the Netty main class."
    mainClass.set("com.mongodb.benchmark.benchmarks.netty.BenchmarkNettyProviderSuite")
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.withType<Javadoc>().configureEach {
    enabled = false
}
