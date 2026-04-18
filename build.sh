#!/usr/bin/env sh

# Run format checks, build the valkey-flash module, and run unit + integration tests.

set -e

SCRIPT_DIR=$(pwd)
echo "Script Directory: $SCRIPT_DIR"

if [ "$1" = "clean" ]; then
  echo "Cleaning build artifacts"
  rm -rf target/
  rm -rf tests/build/
  rm -rf test-data/
  echo "Clean completed."
  exit 0
fi

echo "Running cargo fmt and clippy checks..."
cargo fmt --check
cargo clippy --profile release --all-targets -- -D warnings


echo "Running unit tests..."
cargo test --features enable-system-alloc

# Ensure SERVER_VERSION environment variable is set
if [ -z "$SERVER_VERSION" ]; then
    echo "ERROR: SERVER_VERSION environment variable is not set. Defaulting to unstable."
    export SERVER_VERSION="unstable"
fi

if [ "$SERVER_VERSION" != "unstable" ] && [ "$SERVER_VERSION" != "8.1" ] && [ "$SERVER_VERSION" != "9.0" ]; then
  echo "ERROR: Unsupported version - $SERVER_VERSION"
  exit 1
fi

echo "Running cargo build release..."
RUSTFLAGS="-D warnings" cargo build --all --all-targets --release


REPO_URL="https://github.com/valkey-io/valkey.git"
BINARY_PATH="tests/build/binaries/$SERVER_VERSION/valkey-server"
CACHED_VALKEY_PATH="tests/build/valkey"
if [ -f "$BINARY_PATH" ] && [ -x "$BINARY_PATH" ]; then
    echo "valkey-server binary '$BINARY_PATH' found."
else
    echo "valkey-server binary '$BINARY_PATH' not found."
    mkdir -p "tests/build/binaries/$SERVER_VERSION"
    rm -rf $CACHED_VALKEY_PATH
    cd tests/build
    git clone "$REPO_URL"
    cd valkey
    git checkout "$SERVER_VERSION"
    make distclean
    if [ -n "${ASAN_BUILD}" ]; then
        make -j SANITIZER=address
    else
        make -j
    fi
    cp src/valkey-server ../binaries/$SERVER_VERSION/
    # Copy Lua shared library if present (required by newer Valkey unstable builds)
    if [ -f src/modules/lua/libvalkeylua.so ]; then
        cp src/modules/lua/libvalkeylua.so ../binaries/$SERVER_VERSION/
    fi
    cd $SCRIPT_DIR
    rm -rf $CACHED_VALKEY_PATH
fi


TEST_FRAMEWORK_REPO="https://github.com/valkey-io/valkey-test-framework"
TEST_FRAMEWORK_DIR="tests/build/valkeytestframework"

if [ -d "$TEST_FRAMEWORK_DIR" ]; then
    echo "valkeytestframework found."
else
    echo "Cloning valkey-test-framework..."
    git clone "$TEST_FRAMEWORK_REPO"
    mkdir -p "$TEST_FRAMEWORK_DIR"
    mv "valkey-test-framework/src"/* "$TEST_FRAMEWORK_DIR/"
    rm -rf valkey-test-framework
fi

REQUIREMENTS_FILE="requirements.txt"
if [ -f "$SCRIPT_DIR/$REQUIREMENTS_FILE" ]; then
    if command -v pip > /dev/null 2>&1; then
        echo "Using pip to install packages..."
        pip install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
    elif command -v pip3 > /dev/null 2>&1; then
        echo "Using pip3 to install packages..."
        pip3 install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
    elif python3 -m pip --version > /dev/null 2>&1; then
        echo "Using python3 -m pip to install packages..."
        python3 -m pip install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
    else
        echo "Error: No pip available. Please install Python package installer."
        exit 1
    fi
fi

os_type=$(uname)
MODULE_EXT=".so"
if [ "$os_type" = "Darwin" ]; then
  MODULE_EXT=".dylib"
elif [ "$os_type" = "Linux" ]; then
  MODULE_EXT=".so"
else
  echo "Unsupported OS type: $os_type"
  exit 1
fi
export MODULE_PATH="$SCRIPT_DIR/target/release/libvalkey_flash$MODULE_EXT"

if [ -z "$(find "$SCRIPT_DIR/tests" -maxdepth 1 -name '*.py' 2>/dev/null)" ]; then
    echo "No Python integration tests found — skipping integration test run."
    echo "Build, Format Checks, and Unit tests succeeded"
    exit 0
fi

echo "Running the integration tests..."
if [ -n "${ASAN_BUILD}" ]; then
    if [ -n "$TEST_PATTERN" ]; then
        python3 -m pytest --capture=sys --cache-clear -v "$SCRIPT_DIR/tests/" -k $TEST_PATTERN 2>&1 | tee test_output.tmp
    else
        echo "TEST_PATTERN is not set. Running all integration tests."
        python3 -m pytest --capture=sys --cache-clear -v "$SCRIPT_DIR/tests/" 2>&1 | tee test_output.tmp
    fi

    if grep -q "LeakSanitizer: detected memory leaks" test_output.tmp; then
        RED='\033[0;31m'
        echo "${RED}Memory leaks detected in the following tests:"
        LEAKING_TESTS=$(grep -B 2 "LeakSanitizer: detected memory leaks" test_output.tmp | \
                        grep -v "LeakSanitizer" | \
                        grep ".*\.py::")

        LEAK_COUNT=$(echo "$LEAKING_TESTS" | wc -l)

        echo "$LEAKING_TESTS" | while read -r line; do
            echo "::error::Test with leak: $line"
        done

        printf "\n%s python integration tests have leaks detected in them\n" "$LEAK_COUNT"
        rm test_output.tmp
        exit 1
    fi
    rm test_output.tmp
else
    if [ -n "$TEST_PATTERN" ]; then
        python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/" -k $TEST_PATTERN
    else
        echo "TEST_PATTERN is not set. Running all integration tests."
        python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/"
    fi
fi

echo "Build, Format Checks, Unit tests, and Integration Tests succeeded"
