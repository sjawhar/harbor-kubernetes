# Harbor Kubernetes test porting ledger

Source: agent-c `meta/trajectory_labs/tests/harbor/test_harbor_kubernetes_env.py`
at `415274e0e0e`. Each source test is classified exactly once.

- **PORTED: 30** — Kubernetes behavior now covered by this package's unit tests.
- **AGENT-C: 24** — behavior tied to the five agent-c additions, `job_environment`,
  agent-c registry choices, or agent-c's Hawk/Inspect integration remains in agent-c.

| Source test | Classification | Destination or reason |
| --- | --- | --- |
| `test_start_boots_one_root_pod_carrying_the_labels_and_runtime_class` | PORTED | `tests/test_environment.py::test_start_creates_a_root_pod_with_safety_controls` |
| `test_a_pod_left_by_an_earlier_attempt_at_the_same_trial_is_replaced` | PORTED | `test_replaces_only_a_pod_owned_by_this_session` |
| `test_a_pod_another_session_folds_onto_is_never_deleted` | PORTED | `test_never_replaces_a_pod_owned_by_another_session` |
| `test_an_image_mount_is_the_pods_read_only_image_volume` | PORTED | `test_image_mount_is_read_only_and_subclass_can_rewrite_reference` |
| `test_the_job_environment_block_mounts_the_claude_cli_image_volume` | AGENT-C | `job_environment` and the Claude CLI mount are agent-c policy. |
| `test_the_claude_image_volume_resolves_to_the_clusters_ecr_mirror` | AGENT-C | agent-c selects its own registry mirror; package exposes `resolve_image_mount_reference`. |
| `test_a_no_network_baseline_is_in_force_before_the_pod_exists` | PORTED | `test_no_network_policy_exists_before_the_pod_and_waits_for_enforcement` |
| `test_switching_to_no_network_returns_once_the_cni_has_closed_egress` | PORTED | `test_policy_switch_replaces_in_place_and_waits_for_no_network` |
| `test_a_trial_that_asks_for_the_world_is_refused_before_anything_exists` | PORTED | `tests/test_public_and_dns.py` verifies public by default and refusal with `allow_public_network=False`. |
| `test_switching_to_public_is_refused_and_leaves_the_policy_alone` | PORTED | public policy handling is covered by `test_public_network_is_allowed_without_creating_a_cilium_policy`. |
| `test_the_chart_fixtures_are_of_the_chart_inspect_k8s_sandbox_installs` | AGENT-C | validates an agent-c Inspect chart fixture, not a package dependency. |
| `test_each_policy_is_an_inspect_sandboxs_own_for_the_same_intent` | AGENT-C | retains agent-c's comparison with its Inspect chart. |
| `test_the_trial_pod_links_no_service_into_its_environment_as_an_inspect_pod` | PORTED | `test_start_creates_a_root_pod_with_safety_controls` verifies `enable_service_links=False`. |
| `test_a_policy_another_writer_updated_is_still_replaced_in_place` | PORTED | `test_policy_switch_replaces_in_place_and_waits_for_no_network` increments the live resource version. |
| `test_an_allowlist_reaches_ipv6_addresses_and_ranges_as_cidrs` | PORTED | `test_policy_cidrs_and_invalid_entries_are_handled_without_weakening` |
| `test_an_allowlist_entry_a_cidr_policy_cannot_hold_is_refused` | PORTED | `test_policy_cidrs_and_invalid_entries_are_handled_without_weakening` |
| `test_a_no_network_policy_the_cni_never_enforces_fails_closed` | PORTED | `test_cni_failure_fails_closed` |
| `test_the_egress_probe_targets_an_explicit_cluster_dns_ip_not_the_pods_resolver` | PORTED | `test_egress_probe_targets_a_real_dns_address` |
| `test_the_convergence_target_is_the_runners_non_loopback_nameserver` | PORTED | `test_in_cluster_dns_target_skips_loopback`; off-cluster behavior is in `test_public_and_dns.py`. |
| `test_exec_keeps_stdout_stderr_and_status_apart_with_cwd_and_env` | PORTED | `test_exec_preserves_streams_cwd_env_and_timeout` |
| `test_a_timed_out_exec_is_killed_in_the_pod_and_reports_124` | PORTED | `test_exec_preserves_streams_cwd_env_and_timeout` |
| `test_a_uid_with_no_account_never_runs_the_command` | PORTED | `test_unknown_uid_cannot_become_root` |
| `test_a_file_round_trips_byte_exact_with_its_mode` | PORTED | `test_file_and_directory_transfer_are_byte_exact` |
| `test_downloading_a_missing_file_raises_file_not_found` | PORTED | `test_missing_download_paths_keep_their_distinct_errors` |
| `test_a_directory_round_trips_and_a_missing_one_raises` | PORTED | `test_file_and_directory_transfer_are_byte_exact` and `test_missing_download_paths_keep_their_distinct_errors` |
| `test_an_upload_creates_its_directories_0755_whatever_the_execs_umask` | PORTED | `test_uploads_create_private_modes_under_a_permissive_umask` |
| `test_stop_deletes_the_pod_before_lifting_its_network_policy` | PORTED | `test_stop_deletes_pod_before_policy_and_respects_delete_false` |
| `test_the_verdict_directory_is_created_root_s_alone_the_others_open` | AGENT-C | the verifier root-only seal is one of agent-c's five steps. |
| `test_stop_without_delete_leaves_the_pod_running` | PORTED | `test_stop_deletes_pod_before_policy_and_respects_delete_false` |
| `test_an_image_pull_failure_fails_start_without_waiting_out_the_timeout` | PORTED | `test_start_errors_for_image_pull_failure_and_pod_gone` |
| `test_exec_into_a_deleted_pod_raises_pod_gone` | PORTED | `test_start_errors_for_image_pull_failure_and_pod_gone` |
| `test_a_deleted_pod_never_reads_as_a_missing_file` | AGENT-C | its adapter assertion covers `HarborSandboxEnvironment`, which stays in agent-c. |
| `test_a_compose_task_is_refused` | PORTED | `test_rejects_compose_tasks_missing_images_and_reserved_labels` |
| `test_a_task_without_a_prebuilt_image_is_refused` | PORTED | `test_rejects_compose_tasks_missing_images_and_reserved_labels` |
| `test_pod_labels_may_not_take_the_providers_session_label` | PORTED | `test_rejects_compose_tasks_missing_images_and_reserved_labels` |
| `test_the_job_environment_block_builds_this_provider_through_harbors_factory` | AGENT-C | validates agent-c's `job_environment()` block and import path. |
| `test_the_trial_pod_carries_the_job_annotations_beside_its_session` | AGENT-C | validates Hawk annotations as agent-c's job configuration. |
| `test_preflight_accepts_a_kubeconfig_and_refuses_no_credentials` | PORTED | `test_preflight_accepts_a_kubeconfig_and_refuses_no_credentials` |
| `test_the_egress_preflight_runs_as_whatever_user_harbor_runs_the_agent_as` | AGENT-C | egress preflight is one of agent-c's five steps. |
| `test_the_egress_preflight_runs_once_after_setup_into_the_trials_sample_metadata` | AGENT-C | egress preflight and host metadata are agent-c steps. |
| `test_a_failed_egress_preflight_fails_the_trial_with_its_marker_recorded` | AGENT-C | egress preflight and host metadata are agent-c steps. |
| `test_a_failed_setup_fails_the_trial_before_the_egress_preflight` | AGENT-C | ordering is for agent-c's egress preflight. |
| `test_stop_records_the_verifiers_relaunch_breadcrumb_beside_the_preflight_marker` | AGENT-C | provenance and relaunch metadata are agent-c steps. |
| `test_stop_adds_nothing_when_chrome_was_never_relaunched` | AGENT-C | relaunch metadata is an agent-c step. |
| `test_stop_deletes_the_pod_and_its_policy_when_recording_the_breadcrumb_fails` | AGENT-C | relaunch metadata is an agent-c step. |
| `test_stop_surfaces_both_failures_and_keeps_the_policy_of_a_pod_it_could_not_delete` | AGENT-C | relaunch metadata is an agent-c step. |
| `test_the_installed_content_package_is_the_trials_sample_metadata_from_start` | AGENT-C | content provenance is an agent-c step. |
| `test_a_trial_whose_pod_never_starts_still_names_its_content` | AGENT-C | content provenance is an agent-c step. |
| `test_a_task_under_no_installed_package_records_no_content` | AGENT-C | content provenance is an agent-c step. |
| `test_a_rewardkit_graded_arm_on_an_image_without_the_venv_is_refused_before_setup` | AGENT-C | RewardKit check is an agent-c step. |
| `test_a_rewardkit_graded_arm_on_an_image_with_the_venv_runs_and_checks_as_root` | AGENT-C | RewardKit check is an agent-c step. |
| `test_an_arm_rewardkit_does_not_grade_runs_on_an_image_without_the_venv` | AGENT-C | RewardKit check is an agent-c step. |
| `test_a_grading_mode_task_build_never_writes_fails_the_trial_before_setup` | AGENT-C | RewardKit check is an agent-c step. |
| `test_a_check_that_cannot_answer_fails_the_trial_without_calling_the_venv_missing` | AGENT-C | RewardKit check is an agent-c step. |

## New behavior red/green record

Before the Task 1 changes, the unchanged moved environment was run with:

```console
uv run pytest tests/test_public_and_dns.py -q
```

It failed all four new tests: public policy was refused in both public-network tests,
an off-cluster run raised the old resolver-only error, and a missing Cilium custom resource
propagated Kubernetes `ApiException(404)`. The output ended with `4 failed`.

After the changes, the same command reported `4 passed`. The tests cover public runs without
a Cilium policy, the restrictive override, `kube-system/kube-dns` and `cluster_dns_ip` lookup,
and a Cilium-404 refusal before pod creation.
