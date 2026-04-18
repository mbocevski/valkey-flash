.PHONY: docker-build docker-test-single docker-test-cluster docker-test docker-clean

docker-build:
	docker build -t valkey-flash:dev -f docker/Dockerfile .

docker-test-single:
	USE_DOCKER=1 python -m pytest tests/test_docker_smoke.py -v -m docker_single

docker-test-cluster:
	USE_DOCKER=1 python -m pytest tests/test_docker_smoke.py -v -m docker_cluster

docker-test:
	USE_DOCKER=1 python -m pytest tests/test_docker_smoke.py -v

docker-clean:
	docker compose -f docker/compose.single.yml down -v --remove-orphans 2>/dev/null || true
	docker compose -f docker/compose.cluster.yml down -v --remove-orphans 2>/dev/null || true
	docker rmi valkey-flash:dev 2>/dev/null || true
