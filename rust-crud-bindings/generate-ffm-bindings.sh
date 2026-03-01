#!/bin/bash
#
# Generates Java FFM bindings from the Rust FFI C header using jextract.
#
# Prerequisites:
#   - jextract installed (https://jdk.java.net/jextract/)
#   - cbindgen has been run to generate mongodb_ffi.h in mongo-rust-driver
#
# Usage:
#   ./generate-ffm-bindings.sh [jextract-path]
#
# If jextract-path is not provided, assumes 'jextract' is in PATH.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUST_DRIVER_DIR="${SCRIPT_DIR}/../../mongo-rust-driver"
HEADER_FILE="${RUST_DRIVER_DIR}/include/mongodb_ffi.h"
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
    echo "Run 'cbindgen' in the Rust driver first:"
    echo "  cd $RUST_DRIVER_DIR"
    echo "  cbindgen --config cbindgen.toml --crate mongodb --output include/mongodb_ffi.h"
    exit 1
fi

echo "Generating FFM bindings for CRUD API..."
echo "  Header: $HEADER_FILE"
echo "  Output: $OUTPUT_DIR"
echo "  Package: $PACKAGE"

# Clean output directory
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Create a preprocessed header that fixes the ChangeStream name conflict
# The enum value "ChangeStream" conflicts with "struct ChangeStream"
FIXED_HEADER="/tmp/mongodb_ffi_fixed.h"
sed 's/ChangeStream = 11/ChangeStreamError_Kind = 11/g' "$HEADER_FILE" > "$FIXED_HEADER"

# Run jextract - generate bindings for all MongoDB FFI types
# This generates full bindings for the CRUD-focused FFI API
"$JEXTRACT" \
    --header-class-name MongoDbFfi \
    -l mongodb_ffi \
    -t "$PACKAGE" \
    --output "$OUTPUT_DIR" \
    "$FIXED_HEADER"

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

