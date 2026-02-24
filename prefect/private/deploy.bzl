"""Bazel macro for Prefect flow deploy targets."""

load("@aspect_rules_py//py:defs.bzl", "py_binary")

_RUNNER_LIB = Label("//prefect/private:_prefect_runner_lib")

def _make_prefect_target(name, src_file, args, deps, kwargs):
    """Creates a one-line wrapper + py_binary that dispatches to prefect_runner.

    Each target gets a tiny generated main script that imports and calls the
    shared prefect_runner.main().  All per-target config is passed via CLI args
    (the py_binary `args` attribute), so zero application logic is generated.

    Args:
        name: Target name for the py_binary
        src_file: User's Python source file (flow module)
        args: CLI arguments forwarded to prefect_runner
        deps: Dependencies for the py_binary
        kwargs: Extra kwargs forwarded to py_binary
    """
    script_name = name + "_main.py"
    native.genrule(
        name = name + "_gen",
        outs = [script_name],
        cmd = "echo 'from prefect_runner import main; main()' > $@",
    )
    py_binary(
        name = name,
        srcs = [script_name, src_file],
        main = script_name,
        args = args,
        deps = deps + [_RUNNER_LIB, "@pip//prefect"],
        imports = ["."],
        **kwargs
    )

def prefect_deploy(
        name,
        flow_file,
        entrypoint,
        image_repository,
        work_pool_name = None,
        concurrency_limit = None,
        cron = None,
        cpu = None,
        memory = None,
        timeout = None,
        base_image = None,
        deployment_name = "main",
        deps = [],
        **kwargs):
    """Create a deploy py_binary for a Prefect flow.

    Args:
      name: Target name (typically "deploy").
      flow_file: Python file containing the flow definition.
      entrypoint: Prefect flow entrypoint path (e.g. "path/flow.py:func").
      image_repository: Docker image repository (e.g. "us-central1-docker.pkg.dev/my-project/prefect").
      work_pool_name: Prefect work pool name.
      concurrency_limit: Optional concurrency limit for the deployment.
      cron: Optional cron schedule string.
      cpu: Optional Cloud Run CPU setting.
      memory: Optional Cloud Run memory setting.
      timeout: Optional timeout in seconds.
      base_image: Optional base Docker image (default: prefecthq/prefect-client:3.6.18-python3.13).
      deployment_name: Deployment name (default: main).
      deps: Additional dependencies.
      **kwargs: Extra args forwarded to py_binary.
    """

    # Auto-derive the flow py_library from the package directory name.
    package_path = native.package_name()
    if package_path:
        package_depth = len(package_path.split("/"))
        flow_lib = ":" + package_path.split("/")[-1]
        image_name = package_path.split("/")[-1]
    else:
        # Root package — use the flow_file stem as a fallback.
        package_depth = 0
        image_name = flow_file.rsplit(".", 1)[0]
        flow_lib = ":" + image_name

    args = [
        "--entrypoint",
        entrypoint,
        "--image-name",
        image_name,
        "--image-repository",
        image_repository,
        "--deployment-name",
        deployment_name,
        "--package-depth",
        str(package_depth),
    ]

    if work_pool_name:
        args.extend(["--work-pool-name", work_pool_name])

    if concurrency_limit != None:
        args.extend(["--concurrency-limit", str(concurrency_limit)])

    if cron:
        for token in [part for part in cron.split(" ") if part]:
            args.extend(["--cron", token])

    if cpu != None:
        args.extend(["--cpu", str(cpu)])

    if memory != None:
        args.extend(["--memory", str(memory)])

    if timeout != None:
        args.extend(["--timeout", str(timeout)])

    if base_image != None:
        args.extend(["--base-image", base_image])

    all_deps = deps + [
        flow_lib,
    ]

    _make_prefect_target(name, flow_file, args, all_deps, kwargs)
