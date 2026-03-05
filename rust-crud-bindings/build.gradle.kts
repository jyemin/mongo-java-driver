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
plugins {
    id("project.java")
    id("conventions.test-artifacts")
}

// Main source set uses Java 17 (default) for interfaces and non-FFM code
// FFM source set uses Java 23 for FFM-specific implementations

// FFM source set - compiled with Java 23, includes jextract-generated code
sourceSets {
    create("ffm") {
        java {
            srcDir("src/ffm/java")
            srcDir("src/ffm-generated")
        }
    }
    // Test source set includes functional tests (Java 17 compatible)
    test {
        java {
            srcDir("src/test/java")
            srcDir("src/test/functional")
        }
    }
}

// Compile FFM source set with Java 23
tasks.named<JavaCompile>("compileFfmJava") {
    options.release.set(23)
    javaCompiler.set(javaToolchains.compilerFor { languageVersion.set(JavaLanguageVersion.of(23)) })
    // FFM code needs access to main source set classes (interfaces) and dependencies
    classpath = sourceSets.main.get().output + sourceSets.main.get().compileClasspath
    // Ensure main is compiled first
    dependsOn(tasks.named("compileJava"))
}

// Include FFM classes in the JAR
tasks.withType<Jar> { from(sourceSets["ffm"].output) }

dependencies {
    api(project(path = ":bson", configuration = "default"))
    api(project(path = ":driver-core", configuration = "default"))

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Path to the Rust driver header file
val rustDriverDir = rootProject.file("../mongo-rust-driver")
val headerFile = rustDriverDir.resolve("include/libmongodb.h")

// Task to generate FFM bindings using jextract
tasks.register<Exec>("generateFfmBindings") {
    description = "Generate Java FFM bindings from mongodb_ffi.h using jextract"
    group = "code generation"

    val outputDir = file("src/ffm-generated")
    val packageName = "com.mongodb.internal.rust.crud.ffi"

    // Find jextract - check common locations
    val jextractPath =
        providers
            .gradleProperty("jextract.path")
            .orElse(providers.environmentVariable("JEXTRACT_HOME").map { "$it/bin/jextract" })
            .orElse("jextract")
            .get()

    doFirst {
        if (!headerFile.exists()) {
            throw GradleException(
                "Header file not found: $headerFile\n" +
                    "Run cbindgen in the Rust driver first:\n" +
                    "  cd $rustDriverDir\n" +
                    "  ./generate-ffi-header.sh")
        }
        // Clean output directory
        outputDir.deleteRecursively()
        outputDir.mkdirs()
    }

    commandLine(jextractPath, "-t", packageName, "--output", outputDir.absolutePath, headerFile.absolutePath)
}

// Note: Run ./generate-ffm-bindings.sh manually to regenerate bindings from the header file
// The generateFfmBindings task is for manual invocation when jextract is properly configured
// tasks.compileJava { dependsOn("generateFfmBindings") }

// Test source set is compiled with Java 17 (no FFM dependencies)
// This allows other modules with Java 17 tests to depend on test artifacts

// FFM test source set - compiled with Java 23 for FFM-specific unit tests
sourceSets { create("ffmTest") { java { srcDir("src/ffmTest/java") } } }

// Compile FFM test source set with Java 23
tasks.named<JavaCompile>("compileFfmTestJava") {
    options.release.set(23)
    javaCompiler.set(javaToolchains.compilerFor { languageVersion.set(JavaLanguageVersion.of(23)) })
    classpath =
        sourceSets.main.get().output +
            sourceSets["ffm"].output +
            sourceSets.test.get().output +
            sourceSets["ffmTest"].compileClasspath
    dependsOn(tasks.named("compileFfmJava"), tasks.named("compileTestJava"))
}

// Main test task runs tests from regular test source set (Java 17)
// but runs on Java 23 JVM for FFM runtime
tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    javaLauncher.set(javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(23)) })
    classpath =
        sourceSets.test.get().output +
            sourceSets["ffm"].output +
            sourceSets.main.get().output +
            sourceSets.test.get().runtimeClasspath

    // Set the native library path for the Rust FFI library
    // Can be overridden with -PnativeLibPath=/path/to/lib
    val nativeLibPath =
        findProperty("nativeLibPath")?.toString() ?: rustDriverDir.resolve("target/release").absolutePath
    environment("DYLD_LIBRARY_PATH", nativeLibPath)
    environment("LD_LIBRARY_PATH", nativeLibPath)
    // Enable Rust backtraces for easier debugging of panics
    environment("RUST_BACKTRACE", "1")
}

// FFM test task for FFM-specific unit tests
val ffmTest by
    tasks.registering(Test::class) {
        description = "Runs FFM-specific tests"
        group = "verification"
        useJUnitPlatform()
        jvmArgs("--enable-native-access=ALL-UNNAMED")
        javaLauncher.set(javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(23)) })
        testClassesDirs = sourceSets["ffmTest"].output.classesDirs
        classpath =
            sourceSets["ffmTest"].output +
                sourceSets["ffm"].output +
                sourceSets.main.get().output +
                sourceSets.test.get().output +
                sourceSets["ffmTest"].runtimeClasspath

        val nativeLibPath =
            findProperty("nativeLibPath")?.toString() ?: rustDriverDir.resolve("target/release").absolutePath
        environment("DYLD_LIBRARY_PATH", nativeLibPath)
        environment("LD_LIBRARY_PATH", nativeLibPath)
    }

// Include ffmTest in the check task
tasks.check { dependsOn(ffmTest) }

// FFM test dependencies
dependencies {
    "ffmTestImplementation"(project(path = ":bson", configuration = "default"))
    "ffmTestImplementation"(project(path = ":driver-core", configuration = "default"))
    "ffmTestImplementation"("org.junit.jupiter:junit-jupiter-api:5.10.0")
    "ffmTestRuntimeOnly"("org.junit.jupiter:junit-jupiter-engine:5.10.0")
    "ffmTestRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
