#!/bin/bash
#
# Generates Java FFM bindings from the Rust FFI C header using jextract.
#
# Prerequisites:
#   - jextract installed (https://jdk.java.net/jextract/)
#   - cbindgen has been run to generate libmongodb.h in mongo-rust-driver
#
# Usage:
#   ./generate-ffm-bindings.sh [jextract-path]
#
# If jextract-path is not provided, assumes 'jextract' is in PATH.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUST_DRIVER_DIR="${SCRIPT_DIR}/../../mongo-rust-driver"
HEADER_FILE="${RUST_DRIVER_DIR}/include/libmongodb.h"
OUTPUT_DIR="${SCRIPT_DIR}/src/ffm-generated"
PACKAGE="com.mongodb.internal.rust.crud.ffi"

JEXTRACT="${1:-jextract}"

# Check prerequisites
if ! command -v "$JEXTRACT" &> /dev/null; then
    echo "Error: jextract not found. Please provide path as argument or add to PATH."
    echo "Download from: https://jdk.java.net/jextract/"
    exit 1
fi

if [ ! -f "$HEADER_FILE" ]; then
    echo "Error: Header file not found: $HEADER_FILE"
    echo "Run 'generate-ffi-header.sh' in the Rust driver first:"
    echo "  cd $RUST_DRIVER_DIR"
    echo "  ./generate-ffi-header.sh"
    exit 1
fi

echo "Generating FFM bindings for CRUD API..."
echo "  Header: $HEADER_FILE"
echo "  Output: $OUTPUT_DIR"
echo "  Package: $PACKAGE"

# Clean output directory
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Create a preprocessed header that fixes issues:
# 1. The enum value "ChangeStream" conflicts with "struct ChangeStream"
# 2. Session is used but not forward-declared (opaque pointer type)
FIXED_HEADER="/tmp/mongodb_ffi_fixed.h"
sed 's/ChangeStream = 11/ChangeStreamError_Kind = 11/g' "$HEADER_FILE" | \
    sed '1a\
struct Session;
' > "$FIXED_HEADER"

# Run jextract - generate bindings for all types
# Note: jextract 25 renames "struct Error" to "Error_" because "Error" clashes
# with java.lang.Error. Our code uses Error_ to match.
"$JEXTRACT" \
    --header-class-name MongoDbFfi \
    -l mongodb \
    -t "$PACKAGE" \
    --output "$OUTPUT_DIR" \
    "$FIXED_HEADER"

# Remove system header cruft (Darwin/pthread/signal types that leak through)
# jextract's --include-* options don't handle transitive dependencies well,
# so we clean up after generation instead.
# Note: div_t, ldiv_t, lldiv_t are kept because MongoDbFfi references them.
OUTPUT_FFI_DIR="$OUTPUT_DIR/com/mongodb/internal/rust/crud/ffi"
echo "Removing system header cruft..."
find "$OUTPUT_FFI_DIR" -name "*.java" \( \
    -name "__*" -o \
    -name "_opaque_*" -o \
    -name "sig*" -o \
    -name "pthread_*" -o \
    -name "rusage*" -o \
    -name "rlimit*" -o \
    -name "stack_t*" -o \
    -name "timeval*" -o \
    -name "ucontext_t*" -o \
    -name "wait.java" -o \
    -name "proc_*" -o \
    -name "qsort*" -o \
    -name "bsearch*" -o \
    -name "heapsort*" -o \
    -name "mergesort*" -o \
    -name "psort*" -o \
    -name "atexit*" -o \
    -name "at_quick_exit*" \
    \) -delete
remaining=$(find "$OUTPUT_FFI_DIR" -name "*.java" | wc -l)
echo "  Kept $remaining types"

echo ""
echo "Generated files:"
find "$OUTPUT_DIR" -name "*.java" | sort | while read -r f; do
    lines=$(wc -l < "$f")
    printf "  %-50s %5d lines\n" "$(basename "$f")" "$lines"
done

total=$(find "$OUTPUT_DIR" -name "*.java" -exec cat {} \; | wc -l)
echo ""
echo "Total: $total lines of generated Java code"
echo ""
echo "To compile, run:"
echo "  ./gradlew :rust-crud-bindings:compileJava"

