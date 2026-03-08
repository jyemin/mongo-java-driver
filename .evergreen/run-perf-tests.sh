#!/bin/bash

set -o xtrace
set -o errexit

RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

# Configuration for Rust driver
RUST_DRIVER_REPO="${RUST_DRIVER_REPO:-https://github.com/jyemin/mongo-rust-driver.git}"
RUST_DRIVER_BRANCH="${RUST_DRIVER_BRANCH:-ffi-drop}"
RUST_DRIVER_DIR="${PROJECT_DIRECTORY}/mongo-rust-driver"
JAVA23_DIR="${PROJECT_DIRECTORY}/java23"

#######################################
# Download test data
#######################################
rm -rf driver-performance-test-data
git clone https://github.com/mongodb-labs/driver-performance-test-data.git
cd driver-performance-test-data
tar xf extended_bson.tgz
tar xf parallel.tgz
tar xf single_and_multi_document.tgz
cd ..

#######################################
# Install Java 23 (required for FFM API)
#######################################
echo "=== Setting up Java 23 ==="

if [ ! -d "${JAVA23_DIR}" ]; then
    mkdir -p "${JAVA23_DIR}"
    cd "${JAVA23_DIR}"

    # Determine platform - using Eclipse Temurin (Adoptium)
    case "$(uname -s)" in
        Linux*)
            JAVA_URL="https://github.com/adoptium/temurin23-binaries/releases/download/jdk-23.0.2%2B7/OpenJDK23U-jdk_x64_linux_hotspot_23.0.2_7.tar.gz"
            ;;
        Darwin*)
            if [ "$(uname -m)" = "arm64" ]; then
                JAVA_URL="https://github.com/adoptium/temurin23-binaries/releases/download/jdk-23.0.2%2B7/OpenJDK23U-jdk_aarch64_mac_hotspot_23.0.2_7.tar.gz"
            else
                JAVA_URL="https://github.com/adoptium/temurin23-binaries/releases/download/jdk-23.0.2%2B7/OpenJDK23U-jdk_x64_mac_hotspot_23.0.2_7.tar.gz"
            fi
            ;;
        *)
            echo "Unsupported OS: $(uname -s)"
            exit 1
            ;;
    esac

    echo "Downloading Eclipse Temurin JDK 23 from ${JAVA_URL}..."
    curl -L -o jdk23.tar.gz "${JAVA_URL}"
    tar xzf jdk23.tar.gz --strip-components=1
    rm jdk23.tar.gz
    cd "${PROJECT_DIRECTORY}"
fi

export JAVA_HOME="${JAVA23_DIR}"
export PATH="${JAVA_HOME}/bin:${PATH}"
echo "Java 23 installed at: ${JAVA_HOME}"
java -version

# Export for Gradle toolchain auto-detection
echo "org.gradle.java.installations.paths=${JAVA23_DIR}" >> "${PROJECT_DIRECTORY}/gradle.properties"

#######################################
# Build Rust native library
#######################################
echo "=== Building Rust Native Library ==="
echo "Rust driver repo: ${RUST_DRIVER_REPO}"
echo "Rust driver branch: ${RUST_DRIVER_BRANCH}"

# Install Rust if not present
if ! command -v rustc &> /dev/null; then
    echo "Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
fi

echo "Rust version: $(rustc --version)"
echo "Cargo version: $(cargo --version)"

echo "Cloning Rust driver..."
git clone --depth 1 --branch "${RUST_DRIVER_BRANCH}" "${RUST_DRIVER_REPO}" "${RUST_DRIVER_DIR}"
cd "${RUST_DRIVER_DIR}"

echo "Building FFI library..."
if [ -f "./build-ffi.sh" ]; then
    ./build-ffi.sh
else
    cargo build --release -p mongodb --features ffi
fi

# Determine library path based on OS
case "$(uname -s)" in
    Linux*)
        NATIVE_LIB_PATH="${RUST_DRIVER_DIR}/target/release/libmongodb.so"
        ;;
    Darwin*)
        NATIVE_LIB_PATH="${RUST_DRIVER_DIR}/target/release/libmongodb.dylib"
        ;;
esac

if [ ! -f "${NATIVE_LIB_PATH}" ]; then
    echo "ERROR: Native library not found at ${NATIVE_LIB_PATH}"
    exit 1
fi

echo "Native library built successfully: ${NATIVE_LIB_PATH}"
echo "Library size: $(ls -lh "${NATIVE_LIB_PATH}" | awk '{print $5}')"

cd "${PROJECT_DIRECTORY}"

#######################################
# Run benchmarks
#######################################
echo "=== Running Benchmarks ==="

export TEST_PATH="${PROJECT_DIRECTORY}/driver-performance-test-data/"
export OUTPUT_FILE="${PROJECT_DIRECTORY}/results.json"

if [ "${PROVIDER}" = "Netty" ]; then
    TASK="driver-benchmarks:runNetty"
else
    TASK="driver-benchmarks:run"
fi

NATIVE_LIB_DIR="$(dirname "${NATIVE_LIB_PATH}")"
export LD_LIBRARY_PATH="${NATIVE_LIB_DIR}:${LD_LIBRARY_PATH:-}"
export DYLD_LIBRARY_PATH="${NATIVE_LIB_DIR}:${DYLD_LIBRARY_PATH:-}"

GRADLE_ARGS="-Dorg.mongodb.benchmarks.data=${TEST_PATH} -Dorg.mongodb.benchmarks.output=${OUTPUT_FILE} -PnativeLibPath=${NATIVE_LIB_DIR}"

start_time=$(date +%s)
./gradlew ${GRADLE_ARGS} ${TASK}
end_time=$(date +%s)
elapsed_secs=$((end_time-start_time))

echo "Benchmarks completed in ${elapsed_secs} seconds"

echo "=== Benchmark Results (${OUTPUT_FILE}) ==="
cat "${OUTPUT_FILE}"
