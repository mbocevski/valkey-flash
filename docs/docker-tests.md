# Running Docker-based tests locally

valkey-flash ships Docker infrastructure for two topologies:

| Topology | Compose file | Ports | Use |
|---|---|---|---|
| Single-node | `docker/compose.single.yml` | 6379 | Smoke, functional |
| 3-primary / 3-replica cluster | `docker/compose.cluster.yml` | 7001–7006 | Cluster-aware tests |

## Prerequisites

| Requirement | Minimum | Notes |
|---|---|---|
| Docker Engine | 20.10 | Compose v2 plugin bundled since 23.0; for older installs, `docker compose` must be on PATH |
| Docker Compose | v2 (`docker compose`) | v1 (`docker-compose`) is **not** supported |
| Linux kernel | 5.6 | Required for `io_uring`; the compose files use `security_opt: seccomp:unconfined` (see [seccomp note](#seccomp-and-io_uring)) |
| Python | 3.11+ | For the pytest fixtures (`pip install -r requirements.txt`) |

macOS with Docker Desktop works — the Linux VM it runs satisfies the kernel requirement.

## Build the image

The pytest fixtures rebuild the image automatically via `docker compose up --build`. To build explicitly:

```sh
docker build -t valkey-flash:dev -f docker/Dockerfile .
```

The multi-stage build compiles the Rust module inside the container; no local Rust toolchain is required.

## Single-node

### Start

```sh
docker compose -f docker/compose.single.yml up -d
```

Wait for healthy:

```sh
docker compose -f docker/compose.single.yml ps   # STATUS should show "(healthy)"
```

### Connect

```sh
# CLI
valkey-cli -p 6379

# Python
python -c "
import valkey
c = valkey.Valkey(port=6379)
print(c.execute_command('MODULE LIST'))
print(c.execute_command('FLASH.SET', 'k', 'hello'))
print(c.execute_command('FLASH.GET', 'k'))
"
```

### Tear down

```sh
docker compose -f docker/compose.single.yml down -v
```

`-v` removes the named volumes; omit it to keep data between restarts.

## Cluster (3-primary / 3-replica)

### Start

```sh
docker compose -f docker/compose.cluster.yml up -d
```

The `cluster-init` oneshot service slots the cluster automatically. Wait for it:

```sh
docker compose -f docker/compose.cluster.yml logs -f cluster-init
# exits when you see "All slots assigned"
```

### Connect

```sh
# CLI (any primary)
valkey-cli -p 7001 -c

# Python — must use cluster-aware client
python -c "
from valkey import ValkeyCluster
c = ValkeyCluster(host='localhost', port=7001)
print(c.execute_command('CLUSTER INFO'))
# Use hash tags to pin keys to one slot
print(c.execute_command('FLASH.SET', '{smoke}k', 'hello'))
print(c.execute_command('FLASH.GET', '{smoke}k'))
"
```

### Tear down

```sh
docker compose -f docker/compose.cluster.yml down -v
```

### cluster-init exits non-zero on re-up

If you `up` the cluster a second time without running `down -v` first, `cluster-init` exits non-zero because the cluster is already configured. This is expected — the service has `restart: "no"` to prevent a restart loop. The cluster itself keeps running. If you need a clean slate, always `down -v` first.

## Run the pytest suite against Docker

```sh
# Both topologies
USE_DOCKER=1 pytest tests/test_docker_smoke.py -v

# Single topology only
USE_DOCKER=1 pytest tests/test_docker_smoke.py -v -m docker_single
USE_DOCKER=1 pytest tests/test_docker_smoke.py -v -m docker_cluster
```

Without `USE_DOCKER=1`, the docker-marked tests are skipped automatically.

The fixtures (`docker_single`, `docker_cluster` in `tests/docker_fixtures.py`) bring up the compose stack, wait for health, yield a connected client, and tear everything down on exit — no manual `up`/`down` needed.

## Make targets

```sh
make docker-build          # build valkey-flash:dev image
make docker-test-single    # run docker_single smoke test
make docker-test-cluster   # run docker_cluster smoke test
make docker-test           # run both
make docker-clean          # down -v both stacks + remove image
```

## Debugging a failing container

```sh
# Tail compose logs for a topology
docker compose -f docker/compose.single.yml logs -f
docker compose -f docker/compose.cluster.yml logs -f

# Logs for a specific service
docker compose -f docker/compose.cluster.yml logs flash-primary-1

# List all containers (including stopped/exited)
docker compose -f docker/compose.cluster.yml ps -a

# Shell into a running node
docker exec -it vf-cluster-flash-primary-1-1 sh

# Inspect a stopped container
docker inspect vf-cluster-cluster-init-1
```

### Stuck containers

If a previous run left containers behind (e.g., a test was killed mid-flight):

```sh
docker compose -f docker/compose.single.yml -p vf-single down -v --remove-orphans
docker compose -f docker/compose.cluster.yml -p vf-cluster down -v --remove-orphans
```

The `vf-single` and `vf-cluster` compose project names keep the two stacks isolated; orphan cleanup only touches the matching project.

## seccomp and io_uring

The compose files set `security_opt: seccomp:unconfined` because the default Docker seccomp profile blocks several `io_uring` syscalls that valkey-flash requires. This is appropriate for local development and CI. A hardened custom seccomp profile that whitelists only the required io_uring operations is tracked as a follow-up task.
