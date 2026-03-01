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

plugins { id("project.java") }

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
}

// Path to the Rust driver header file
val rustDriverDir = rootProject.file("../mongo-rust-driver")
val headerFile = rustDriverDir.resolve("include/mongodb_ffi.h")

// Task to generate FFM bindings using jextract
tasks.register<Exec>("generateFfmBindings") {
    description = "Generate Java FFM bindings from mongodb_ffi.h using jextract"
    group = "code generation"

    val outputDir = file("src/ffm-generated")
    val packageName = "com.mongodb.internal.rust.crud.ffi"

    // Find jextract - check common locations
    val jextractPath = providers.gradleProperty("jextract.path")
        .orElse(providers.environmentVariable("JEXTRACT_HOME").map { "$it/bin/jextract" })
        .orElse("jextract")
        .get()

    doFirst {
        if (!headerFile.exists()) {
            throw GradleException(
                "Header file not found: $headerFile\n" +
                "Run cbindgen in the Rust driver first:\n" +
                "  cd $rustDriverDir\n" +
                "  cbindgen --config cbindgen.toml --crate mongodb --output include/mongodb_ffi.h"
            )
        }
        // Clean output directory
        outputDir.deleteRecursively()
        outputDir.mkdirs()
    }

    commandLine(
        jextractPath,
        "-t", packageName,
        "--output", outputDir.absolutePath,
        headerFile.absolutePath
    )
}

// Note: Run ./generate-ffm-bindings.sh manually to regenerate bindings from the header file
// The generateFfmBindings task is for manual invocation when jextract is properly configured
// tasks.compileJava { dependsOn("generateFfmBindings") }

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

