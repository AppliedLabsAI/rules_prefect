"""Shared entry point for prefect_deploy targets.

Replaces per-target Click CLI (cli.py) with a single static file that accepts
CLI args.  Each Bazel macro generates a one-line wrapper that calls main()
here; all real logic lives in lintable, testable Python.

GCP Cloud Run focus: the deploy helpers use GCP_PROJECT_ID / GCP_REGION env
vars to resolve image repositories. Non-GCP users can ignore those env vars
and pass fully-qualified image_repository values directly.
"""

from __future__ import annotations

import argparse
import asyncio
import atexit
import glob
import importlib.metadata
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final
from urllib.parse import urlparse, urlunparse

if TYPE_CHECKING:
    from prefect.docker import DockerImage
    from prefect.runner.storage import LocalStorage

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BASE_IMAGE: Final[str] = "prefecthq/prefect-client:3.6.18-python3.13"
ENVIRONMENT: Final[str] = os.getenv("ENVIRONMENT") or os.getenv("environment") or ""  # noqa: SIM112
FLOW_IMAGE_TAG_DEFAULT: Final[str] = "latest"
BAKED_SRC_PATH: Final[str] = "/opt/prefect/src"

JobVariableValue = str | int | list[str] | dict[str, str]
JobVariables = dict[str, JobVariableValue]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class DeployFlowArgs:
    entrypoint: str
    image_name: str
    image_repository: str
    dockerfile: str
    base_image: str = DEFAULT_BASE_IMAGE
    buildx_push: bool = False
    concurrency_limit: int | None = None
    deployment_name: str = "main"
    work_pool_name: str | None = None
    cron: str | None = None
    cpu: str | None = None
    memory: str | None = None
    timeout: int | None = None


@dataclass
class DeploymentSettings:
    image: DockerImage | str
    build: bool
    push: bool
    job_variables: JobVariables | None


# ---------------------------------------------------------------------------
# Flow source / image / deployment resolution
# ---------------------------------------------------------------------------


async def resolve_flow_source() -> tuple[LocalStorage, Path | None]:
    """Return the storage source and local repo root (if applicable)."""
    from prefect.runner.storage import LocalStorage

    if ENVIRONMENT == "local":
        repo_root = Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")).resolve()
        return LocalStorage(path=str(repo_root)), repo_root

    return LocalStorage(path=BAKED_SRC_PATH), None


def _resolve_image_repository(image_repository: str) -> str:
    """Override image repository using GCP_PROJECT_ID env var if set."""
    gcp_project_id = os.environ.get("GCP_PROJECT_ID")
    if gcp_project_id:
        region = os.environ.get("GCP_REGION", "us-central1")
        return f"{region}-docker.pkg.dev/{gcp_project_id}/prefect"
    return image_repository


def resolve_flow_image(
    image_repository: str, flow_name: str, tag: str = FLOW_IMAGE_TAG_DEFAULT
) -> str:
    return f"{_resolve_image_repository(image_repository)}/{flow_name}:{tag}"


def _local_prefect_api_url_for_docker() -> str:
    """Return a docker-worker friendly Prefect API URL for local runs."""
    raw = os.getenv("PREFECT_API_URL", "http://127.0.0.1:4200/api")
    parsed = urlparse(raw)
    hostname = parsed.hostname
    if hostname not in {"127.0.0.1", "localhost"}:
        return raw.rstrip("/")

    netloc = "host.docker.internal"
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"
    if parsed.username:
        auth = parsed.username
        if parsed.password:
            auth = f"{auth}:{parsed.password}"
        netloc = f"{auth}@{netloc}"

    rewritten = parsed._replace(netloc=netloc)
    return urlunparse(rewritten).rstrip("/")


def _apply_local_repo_mount(
    job_variables: JobVariables | None,
    local_repo_root: Path | None,
) -> JobVariables | None:
    """Add a local repo volume mount for Docker workers."""
    if local_repo_root is None:
        return job_variables

    base_variables = job_variables or {}
    volumes = list(base_variables.get("volumes", []))
    volumes.append(f"{local_repo_root}:{local_repo_root}")
    return {**base_variables, "volumes": volumes}


def resolve_deployment_settings(
    args: DeployFlowArgs,
    dockerfile_path: Path,
    local_repo_root: Path | None,
) -> DeploymentSettings:
    """Build deployment settings for managed or custom work pools."""
    from prefect.docker import DockerImage

    is_local = ENVIRONMENT == "local"
    job_variables: JobVariables

    image = DockerImage(
        name=resolve_flow_image(args.image_repository, args.image_name),
        dockerfile=dockerfile_path.name,
        context=dockerfile_path.parent,
    )
    job_variables = {}

    if is_local:
        job_variables["image_pull_policy"] = "IfNotPresent"
        job_variables["environment"] = "local"
        job_variables["env"] = {
            "PREFECT_API_URL": _local_prefect_api_url_for_docker(),
            "PREFECT_EVENTS_ENABLED": "false",
        }
    else:
        job_variables["environment"] = ENVIRONMENT

    if args.cpu is not None:
        job_variables["cpu"] = args.cpu
    if args.memory is not None:
        job_variables["memory"] = args.memory
    if args.timeout is not None:
        job_variables["timeout"] = args.timeout

    return DeploymentSettings(
        image=image,
        build=True,
        push=not is_local,
        job_variables=_apply_local_repo_mount(job_variables, local_repo_root),
    )


def _build_and_push_image(
    image_ref: str, dockerfile_path: Path, platform: str = "linux/amd64"
) -> None:
    """Build and push a Docker image using Docker Buildx.

    Useful on Apple Silicon where Prefect's built-in Docker build produces
    arm64 images that are incompatible with amd64 work pools (e.g. Cloud Run).
    """
    subprocess.run(
        [
            "docker",
            "buildx",
            "build",
            "--platform",
            platform,
            "--file",
            str(dockerfile_path),
            "--tag",
            image_ref,
            "--pull",
            "--push",
            str(dockerfile_path.parent),
        ],
        check=True,
    )


async def deploy_flow(args: DeployFlowArgs) -> None:
    """Deploy a Prefect flow using the configured environment settings."""
    from prefect import flow
    from prefect.client.schemas.objects import ConcurrencyLimitConfig
    from prefect.deployments.runner import RunnerDeployment
    from prefect.deployments.runner import adeploy as prefect_adeploy  # type: ignore[attr-defined]
    from prefect.docker import DockerImage
    from prefect.runner.storage import LocalStorage

    dockerfile_path = Path(args.dockerfile).resolve()
    source, local_repo_root = await resolve_flow_source()

    settings = resolve_deployment_settings(args, dockerfile_path, local_repo_root)

    if (
        args.buildx_push
        and isinstance(settings.image, DockerImage)
        and settings.build
        and settings.push
    ):
        _build_and_push_image(
            image_ref=settings.image.reference,
            dockerfile_path=dockerfile_path,
        )
        settings = DeploymentSettings(
            image=settings.image.reference,
            build=False,
            push=False,
            job_variables=settings.job_variables,
        )

    concurrency_limit = None
    if args.concurrency_limit is not None:
        concurrency_limit = ConcurrencyLimitConfig(
            limit=args.concurrency_limit,
            grace_period_seconds=300,
        )

    if ENVIRONMENT != "local":
        staged_source = LocalStorage(path=str(dockerfile_path.parent / "src"))
        deployment = await RunnerDeployment.afrom_storage(
            storage=staged_source,
            entrypoint=args.entrypoint,
            name=args.deployment_name,
            work_pool_name=args.work_pool_name,
            job_variables=settings.job_variables,
            concurrency_limit=concurrency_limit,
            cron=args.cron,
        )
        deployment.storage = source
        await prefect_adeploy(
            deployment,
            work_pool_name=args.work_pool_name,
            image=settings.image,
            build=settings.build,
            push=settings.push,
        )
    else:
        target_flow = await flow.from_source(
            source=source,
            entrypoint=args.entrypoint,
        )
        await target_flow.deploy(
            name=args.deployment_name,
            image=settings.image,
            build=settings.build,
            push=settings.push,
            job_variables=settings.job_variables,
            work_pool_name=args.work_pool_name,
            concurrency_limit=concurrency_limit,
            cron=args.cron,
        )


# ---------------------------------------------------------------------------
# Runfile staging
# ---------------------------------------------------------------------------


def _stage_runfiles(package_depth: int) -> str:
    """Stage Bazel runfiles into a temp directory with dereferenced symlinks.

    Bazel runfiles on macOS are symlink trees; Docker COPY needs real files.
    This function:
      1. Walks up package_depth dirs from sys.argv[0] to find runfiles root
      2. Copies runfiles into a temp dir with dereferenced symlinks
      3. Skips _solib_* native library directories
      4. Flattens .venv.pth proto-generated code into the staging root

    Returns the staging directory path.
    """
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    runfiles_main = script_dir
    for _ in range(package_depth):
        runfiles_main = os.path.dirname(runfiles_main)

    staging_dir = os.path.realpath(tempfile.mkdtemp(prefix="prefect_src_"))
    atexit.register(shutil.rmtree, staging_dir, True)

    for entry in os.listdir(runfiles_main):
        if entry.startswith("_solib_"):
            continue
        src = os.path.join(runfiles_main, entry)
        dst = os.path.join(staging_dir, entry)
        if os.path.isdir(src):
            shutil.copytree(src, dst, symlinks=False)
        else:
            shutil.copy2(src, dst)

    # Flatten .venv.pth entries (proto-generated code) into the staging root.
    runfiles_root = os.path.dirname(runfiles_main)
    site_pkg_dirs = glob.glob(
        os.path.join(runfiles_root, ".*.venv", "lib", "python*", "site-packages")
    )
    for sp_dir in site_pkg_dirs:
        for pth_file in glob.glob(os.path.join(sp_dir, "*.venv.pth")):
            with open(pth_file) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    resolved = os.path.normpath(os.path.join(sp_dir, line))
                    if resolved.startswith(runfiles_main + os.sep) and os.path.isdir(
                        resolved
                    ):
                        rel_path = os.path.relpath(resolved, runfiles_main)
                        for item in os.listdir(resolved):
                            src_path = os.path.join(resolved, item)
                            dst_path = os.path.join(staging_dir, item)
                            if os.path.isdir(src_path):
                                shutil.copytree(
                                    src_path,
                                    dst_path,
                                    symlinks=False,
                                    dirs_exist_ok=True,
                                )
                            elif not os.path.exists(dst_path):
                                shutil.copy2(src_path, dst_path)
                        staged_orig = os.path.join(staging_dir, rel_path)
                        if os.path.basename(resolved).endswith("_pb") and os.path.isdir(
                            staged_orig
                        ):
                            shutil.rmtree(staged_orig)

    return staging_dir


def _resolve_extra_pip_packages(base_packages: list[str]) -> list[str]:
    """Auto-detect extra pip packages from the Bazel environment.

    The base Prefect Docker image includes *base_packages* and their transitive
    deps.  Any additional pip packages present in the Bazel venv are returned
    so they can be installed in the Docker image at deploy time.
    """
    base: set[str] = set()
    queue = list(base_packages)
    while queue:
        name = queue.pop()
        key = name.lower().replace("-", "_").replace(".", "_")
        if key in base:
            continue
        base.add(key)
        try:
            reqs = importlib.metadata.requires(name) or []
        except importlib.metadata.PackageNotFoundError:
            continue
        for r in reqs:
            if "; extra" in r:
                continue
            pkg = r.split(";")[0].strip()
            for c in "><=!~[ ":
                pkg = pkg.split(c)[0]
            if pkg:
                queue.append(pkg)

    extras: list[str] = []
    seen: set[str] = set()
    for dist in importlib.metadata.distributions():
        dist_name = dist.metadata["Name"]
        key = dist_name.lower().replace("-", "_").replace(".", "_")
        if key not in base and key not in seen:
            seen.add(key)
            extras.append(dist_name + "==" + dist.version)
    return extras


def _render_dockerfile(
    base_image: str,
    dependencies: list[str],
    src_dir_name: str | None = None,
) -> str:
    """Generate Dockerfile content for a Prefect flow image."""
    lines = [f"ARG BASE_IMAGE={base_image}", "FROM ${BASE_IMAGE}"]
    if dependencies:
        lines.append("")
        lines.append(
            "RUN set -f; python -m pip install --no-cache-dir --upgrade-strategy only-if-needed \\"
        )
        for index, dependency in enumerate(dependencies):
            suffix = " \\" if index < len(dependencies) - 1 else ""
            lines.append(f"    '{dependency}'{suffix}")
    if src_dir_name:
        lines.append("")
        lines.append(f"COPY {src_dir_name}/ /opt/prefect/src/")
        lines.append("WORKDIR /opt/prefect/src")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


async def _deploy(args: argparse.Namespace) -> None:
    """Generate Dockerfile with auto-detected deps, then deploy the flow."""
    extra_deps = _resolve_extra_pip_packages(["prefect"])
    if extra_deps:
        print("Extra pip packages detected from Bazel deps: " + ", ".join(extra_deps))  # noqa: T201

    bake_source = ENVIRONMENT not in ("", "local") and args.package_depth > 0
    staging_dir: str | None = None
    if bake_source:
        staging_dir = _stage_runfiles(args.package_depth)

    base_image = args.base_image

    dockerfile_content = _render_dockerfile(
        base_image,
        extra_deps,
        src_dir_name="src" if bake_source else None,
    )

    with tempfile.TemporaryDirectory(prefix="prefect_deploy_") as tmpdir:
        if bake_source and staging_dir:
            shutil.copytree(staging_dir, str(Path(tmpdir) / "src"))

        dockerfile_path = Path(tmpdir) / "Dockerfile"
        dockerfile_path.write_text(dockerfile_content, encoding="utf-8")

        cron_value = " ".join(args.cron) if args.cron else None

        deploy_args = DeployFlowArgs(
            entrypoint=args.entrypoint,
            image_name=args.image_name,
            image_repository=args.image_repository,
            dockerfile=str(dockerfile_path),
            base_image=base_image,
            buildx_push=args.buildx_push,
            concurrency_limit=args.concurrency_limit,
            deployment_name=args.deployment_name,
            work_pool_name=args.work_pool_name,
            cron=cron_value,
            cpu=args.cpu,
            memory=args.memory,
            timeout=args.timeout,
        )
        await deploy_flow(deploy_args)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prefect flow deploy runner")
    parser.add_argument("--entrypoint", required=True)
    parser.add_argument("--image-name", required=True)
    parser.add_argument("--image-repository", required=True)
    parser.add_argument(
        "--base-image",
        default=DEFAULT_BASE_IMAGE,
        help="Base Docker image for the flow (default: %(default)s)",
    )
    parser.add_argument(
        "--buildx-push",
        action="store_true",
        default=False,
        help="Use docker buildx to build and push a linux/amd64 image (useful on Apple Silicon)",
    )
    parser.add_argument("--concurrency-limit", type=int, default=None)
    parser.add_argument("--deployment-name", default="main")
    parser.add_argument("--work-pool-name", default=None)
    parser.add_argument("--cron", action="append", default=[])
    parser.add_argument("--cpu", default=None)
    parser.add_argument("--memory", default=None)
    parser.add_argument("--timeout", type=int, default=None)
    parser.add_argument("--package-depth", type=int, default=0)
    args = parser.parse_args()
    asyncio.run(_deploy(args))


if __name__ == "__main__":
    main()
