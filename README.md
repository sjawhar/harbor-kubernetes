# harbor-kubernetes

`harbor-kubernetes` is a public [Harbor](https://github.com/harbor-framework/harbor)
environment extension. It runs each Harbor trial as one Kubernetes pod from the task's
`[environment].docker_image`, and carries Harbor's commands and file transfers over
Kubernetes exec.

The package has no dependency on a particular Harbor host. It works with a normal
`harbor run` from a machine that has Kubernetes credentials, as well as from a service
running inside a cluster.

## Install

Install the package into the environment that runs Harbor:

```bash
uv tool install harbor --with 'harbor-kubernetes @ git+https://github.com/sjawhar/harbor-kubernetes'
```

If you manage Harbor as a project dependency, install the `harbor` extra instead:

```bash
uv add 'harbor-kubernetes[harbor] @ git+https://github.com/sjawhar/harbor-kubernetes'
```

It is tested with Harbor 0.23.0 and 0.24.0 on Python 3.12 or newer.

## Job configuration

Configure the environment at the Harbor job level:

```json
{
  "environment": {
    "import_path": "harbor_kubernetes:KubernetesEnvironment",
    "kwargs": {
      "namespace": "harbor-trials",
      "kube_context": "my-cluster",
      "runtime_class_name": "gvisor",
      "pod_labels": {"app.kubernetes.io/part-of": "harbor"},
      "annotations": {"example.com/owner": "evaluation-team"},
      "image_pull_policy": "IfNotPresent",
      "pod_ready_timeout_sec": 900,
      "ephemeral_storage_request": "8Gi",
      "ephemeral_storage_limit": "16Gi"
    }
  }
}
```

`namespace` is required. `kube_context` is used when no in-cluster service-account
token exists. `runtime_class_name`, labels, annotations, image-pull policy, timeout,
and ephemeral-storage quantities are optional pod settings.

A public Harbor task creates no network-policy object. Set
`allow_public_network: false` to reject public task phases instead. Restricted
`no-network` and `allowlist` phases create one CiliumNetworkPolicy per trial. The
policy is created before the pod, is updated in place for phase switches, and is
deleted only after the pod is gone. Those modes require a cluster serving
`ciliumnetworkpolicies.cilium.io`; the package refuses a restricted trial before it
creates a pod when Cilium is absent. Allowlists accept only IP addresses and CIDRs.

For a no-network convergence check, set `cluster_dns_ip` when the cluster DNS Service
is not `kube-system/kube-dns`. Otherwise the environment reads the runner's resolver
inside a cluster and reads `kube-system/kube-dns`'s ClusterIP outside one. If it cannot
find an address, it refuses the trial before creating its pod and names
`cluster_dns_ip` in the error.

## Extension points

`KubernetesEnvironment` has public methods for a host integration that needs to add
its own behavior without making this package depend on that host:

- `start(force_build)`, `run_healthcheck(healthcheck)`, and `stop(delete)` implement
  lifecycle hooks; overriding lifecycle methods calls `super()` around host-specific
  behavior.
- `create_trial_directories()` creates Harbor's writable mount targets.
- `resolve_image_mount_reference(reference) -> str` can rewrite an image-volume
  reference for a registry mirror.
- `cluster_dns_probe_target() -> str` supplies the target for no-network convergence.
- `remote_command(...) -> str` builds the command passed through Kubernetes exec.
- `pod_manifest()` and `network_policy_manifest(network_policy)` expose the two
  Kubernetes manifests a host may extend.

The default pod has no service-account token, disables Kubernetes service-link
variables, runs the container as root while Harbor switches individual commands to
the task user, and mounts image volumes read-only.

## Real-cluster end-to-end tests

The real-cluster tests are opt-in and are not part of the unit-test command. They
run stock `harbor run` from the package virtual environment against the supplied
kubeconfig context. The context needs permission to create and delete a namespace
and must serve CiliumNetworkPolicy for the no-network trial.

Each session creates one `harbor-kubernetes-e2e-<random>` namespace, verifies that
`python:3.13-slim` can pull there before starting Harbor, and deletes the namespace
in teardown even if a trial fails. It waits for the namespace to be gone. The tests
do not create resources outside that namespace.

```bash
uv run pytest tests/e2e -s -vv --e2e --kube-context staging
uv run --with harbor==0.23.0 pytest tests/e2e -s -vv --e2e --kube-context staging
```

Both commands run a public task and a no-network task with the Oracle agent. The
public trial observes no CiliumNetworkPolicy. The no-network trial records its
policy while it runs, confirms the completed environment setup that includes the
package's convergence wait, and makes its `tests/test.sh` fail if TCP to the
package's cluster-DNS address on port 53 remains reachable. The test output prints
each trial's wall time.

## Development

```bash
uv sync --group dev
uv run ruff check .
uv run ruff format --check .
uv run basedpyright
uv run pytest -n 4 -q
uv run --with harbor==0.23.0 pytest -n 4 -q
```
