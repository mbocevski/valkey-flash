.PHONY: docker-build docker-test-single docker-test-cluster docker-test docker-clean coverage-integration

docker-build:
	docker build -t valkey-flash:dev -f docker/Dockerfile .

docker-test-single:
	USE_DOCKER=1 python -m pytest tests/test_docker_smoke.py -v -m docker_single

docker-test-cluster:
	USE_DOCKER=1 python -m pytest tests/test_docker_smoke.py -v -m docker_cluster

docker-test:
	USE_DOCKER=1 python -m pytest tests/test_docker_smoke.py -v

docker-clean:
	docker compose -f docker/compose.single.yml -p vf-single down -v --remove-orphans 2>/dev/null || true
	docker compose -f docker/compose.cluster.yml -p vf-cluster down -v --remove-orphans 2>/dev/null || true
	docker rmi valkey-flash:dev 2>/dev/null || true

# Combined unit + integration coverage report.
# Prerequisites: SERVER_VERSION env var (default: unstable), valkey-server binary in
# tests/build/binaries/$$SERVER_VERSION/, and Python test deps installed.
# Output: lcov.info in the repo root.
coverage-integration:
	cargo llvm-cov clean --workspace
	cargo llvm-cov --no-report --features enable-system-alloc
	LLVM_PROFILE_FILE="$$PWD/target/llvm-cov-target/flash-%p-%m.profraw" \
	  MODULE_PATH="$$PWD/target/debug/libvalkey_flash.so" \
	  SERVER_VERSION="$${SERVER_VERSION:-unstable}" \
	  python3 -m pytest tests/ --ignore=tests/test_docker_smoke.py -q --tb=no
	cargo llvm-cov report \
	  --lcov \
	  --output-path lcov.info \
	  --features enable-system-alloc \
	  --ignore-filename-regex '(src/wrapper/|/fuzz/|tests/build/)'
	@echo "Coverage report: lcov.info"
