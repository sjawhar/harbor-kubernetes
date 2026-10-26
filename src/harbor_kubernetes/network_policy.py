"""Per-trial Cilium policy manifests for Kubernetes Harbor environments."""

from __future__ import annotations

from ipaddress import ip_address, ip_network
from typing import NotRequired, Required, TypedDict

from harbor.models.task.config import NetworkMode, NetworkPolicy

SESSION_LABEL = "harbor-kubernetes/session"
SESSION_ID_ANNOTATION = "harbor-kubernetes/session-id"
CILIUM_POLICY_GROUP = "cilium.io"
CILIUM_POLICY_VERSION = "v2"
CILIUM_POLICY_PLURAL = "ciliumnetworkpolicies"
CILIUM_POLICY_KIND = "CiliumNetworkPolicy"
PUBLIC_NETWORK_REFUSAL = (
    "network_mode='public' is refused: this environment was configured with "
    "allow_public_network=false, so a trial's pod reaches no more than an allowlist names"
)
_POD_NAMESPACE_LABEL = "io.kubernetes.pod.namespace"
_CLUSTER_DNS_ENDPOINTS = ("kube-dns", "node-local-dns")
_DNS_PORT = "53"


class Selector(TypedDict):
    """A Cilium endpoint selector."""

    matchLabels: dict[str, str]


class DnsPattern(TypedDict):
    """A DNS pattern accepted by Cilium."""

    matchPattern: str


class DnsRules(TypedDict):
    """The DNS rules held by a Cilium port rule."""

    dns: list[DnsPattern]


class Port(TypedDict):
    """A Cilium port declaration."""

    port: str
    protocol: str


class PortRule(TypedDict):
    """Ports and DNS rules in an egress rule."""

    ports: list[Port]
    rules: DnsRules


class CiliumEgressRule(TypedDict, total=False):
    """One Cilium egress or egress-deny rule."""

    toEndpoints: list[Selector]
    toPorts: list[PortRule]
    toEntities: list[str]
    toCIDR: list[str]


class IcmpField(TypedDict):
    """A Cilium ICMP selector field."""

    type: int
    family: str


class IcmpRule(TypedDict):
    """A Cilium ICMP selector."""

    fields: list[IcmpField]


class CiliumIngressRule(TypedDict, total=False):
    """One Cilium ingress rule."""

    fromEndpoints: list[Selector]
    icmps: list[IcmpRule]


class CiliumPolicySpec(TypedDict, total=False):
    """The Cilium policy spec written for one trial."""

    description: Required[str]
    endpointSelector: Required[Selector]
    ingress: Required[list[CiliumIngressRule]]
    egress: list[CiliumEgressRule]
    egressDeny: list[CiliumEgressRule]


class OwnerReference(TypedDict):
    """A pod owner reference for a trial policy."""

    apiVersion: str
    kind: str
    name: str
    uid: str


class PolicyMetadata(TypedDict):
    """Metadata for a trial Cilium policy."""

    name: str
    namespace: str
    labels: dict[str, str]
    ownerReferences: NotRequired[list[OwnerReference]]
    resourceVersion: NotRequired[str]


class CiliumNetworkPolicyManifest(TypedDict):
    """A CiliumNetworkPolicy custom-resource manifest."""

    apiVersion: str
    kind: str
    metadata: PolicyMetadata
    spec: CiliumPolicySpec


def validated_pod_labels(pod_labels: dict[str, str] | None) -> dict[str, str]:
    """Copy caller labels while reserving the provider's pod identity label."""
    labels = dict(pod_labels or {})
    if SESSION_LABEL in labels:
        raise ValueError(
            f"pod_labels may not set {SESSION_LABEL!r}: the provider owns it, and the "
            "trial's network policy selects the pod by it."
        )
    return labels


def trial_selector(namespace: str, pod_name: str) -> Selector:
    """Select exactly one trial pod."""
    return {
        "matchLabels": {
            _POD_NAMESPACE_LABEL: namespace,
            SESSION_LABEL: pod_name,
        }
    }


def _sandbox_egress_rules(
    namespace: str, selector: Selector, reach: CiliumEgressRule | None
) -> list[CiliumEgressRule]:
    patterns: list[DnsPattern] = [{"matchPattern": f"*.{namespace}.svc.cluster.local"}]
    rules: list[CiliumEgressRule] = [
        {
            "toEndpoints": [
                {
                    "matchLabels": {
                        _POD_NAMESPACE_LABEL: "kube-system",
                        "k8s-app": app,
                    }
                }
                for app in _CLUSTER_DNS_ENDPOINTS
            ],
            "toPorts": [
                {
                    "ports": [{"port": _DNS_PORT, "protocol": "ANY"}],
                    "rules": {"dns": patterns},
                }
            ],
        },
        {"toEndpoints": [selector]},
    ]
    if reach is not None:
        rules.append(reach)
    return rules


def _sandbox_ingress_rules(selector: Selector) -> list[CiliumIngressRule]:
    echo: IcmpRule = {
        "fields": [{"type": 8, "family": "IPv4"}, {"type": 128, "family": "IPv6"}]
    }
    return [
        {"fromEndpoints": [selector]},
        {"fromEndpoints": [selector], "icmps": [echo]},
    ]


def build_network_policy_manifest(
    *,
    namespace: str,
    pod_name: str,
    policy_name: str,
    pod_labels: dict[str, str],
    network_policy: NetworkPolicy,
    pod_owner: OwnerReference | None,
) -> CiliumNetworkPolicyManifest:
    """Build the Cilium policy that enforces one non-public Harbor network mode."""
    selector = trial_selector(namespace, pod_name)
    spec: CiliumPolicySpec
    match network_policy.network_mode:
        case NetworkMode.NO_NETWORK:
            spec = {
                "description": "Harbor trial, no-network: deny all ingress and egress.",
                "endpointSelector": selector,
                "ingress": [{}],
                "egressDeny": [{"toEntities": ["all"]}],
            }
        case NetworkMode.ALLOWLIST:
            cidrs: list[str] = []
            for entry in network_policy.allowed_hosts:
                if "/" in entry:
                    cidrs.append(str(ip_network(entry, strict=True)))
                else:
                    address = ip_address(entry)
                    cidrs.append(f"{address}/{32 if address.version == 4 else 128}")
            spec = {
                "description": (
                    "Harbor trial, allowlist: ingress from the trial's own pod; "
                    "egress to cluster DNS for this namespace, the trial's own pod, "
                    "and the allowlisted CIDRs."
                    if cidrs
                    else "Harbor trial, granted nothing: ingress from the trial's own "
                    "pod; egress to cluster DNS for this namespace and the trial's own pod."
                ),
                "endpointSelector": selector,
                "ingress": _sandbox_ingress_rules(selector),
                "egress": _sandbox_egress_rules(
                    namespace, selector, {"toCIDR": cidrs} if cidrs else None
                ),
            }
        case NetworkMode.PUBLIC:
            raise ValueError("a public trial has no network policy to build")
    metadata: PolicyMetadata = {
        "name": policy_name,
        "namespace": namespace,
        "labels": {**pod_labels, SESSION_LABEL: pod_name},
    }
    if pod_owner is not None:
        metadata["ownerReferences"] = [pod_owner]
    return {
        "apiVersion": f"{CILIUM_POLICY_GROUP}/{CILIUM_POLICY_VERSION}",
        "kind": CILIUM_POLICY_KIND,
        "metadata": metadata,
        "spec": spec,
    }
