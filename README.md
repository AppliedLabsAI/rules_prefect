# rules_prefect

Bazel rules for deploying [Prefect](https://www.prefect.io/) flows as Docker images to work pools (e.g. GCP Cloud Run).

## Setup

Add to your `MODULE.bazel`:

```starlark
bazel_dep(name = "rules_prefect", version = "0.1.0")
```

You must also have `rules_python` with a pip hub that includes `prefect`:

```starlark
bazel_dep(name = "rules_python", version = "1.8.4")
bazel_dep(name = "aspect_rules_py", version = "1.8.4")

pip = use_extension("@rules_python//python/extensions:pip.bzl", "pip")
pip.parse(
    hub_name = "pip",
    python_version = "3.13",
    requirements_lock = "//:requirements.txt",
)
use_repo(pip, "pip")
```

Your `requirements.txt` must include `prefect>=3.6`.

## Usage

```starlark
load("@rules_prefect//prefect:defs.bzl", "prefect_deploy")

prefect_deploy(
    name = "deploy",
    flow_file = "flow.py",
    entrypoint = "flow.py:my_flow",
    image_repository = "us-central1-docker.pkg.dev/my-project/prefect",
    work_pool_name = "my-cloud-run-pool",
)
```

Then deploy with:

```bash
bazel run //:deploy
```

### `prefect_deploy` Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `name` | Yes | — | Bazel target name (typically `"deploy"`) |
| `flow_file` | Yes | — | Python source file containing the flow |
| `entrypoint` | Yes | — | Prefect entrypoint (`"file.py:function"`) |
| `image_repository` | Yes | — | Docker image repository URL |
| `work_pool_name` | No | `None` | Prefect work pool name |
| `deployment_name` | No | `"main"` | Deployment name |
| `base_image` | No | `"prefecthq/prefect-client:3.6.18-python3.13"` | Base Docker image |
| `concurrency_limit` | No | `None` | Max concurrent flow runs |
| `cron` | No | `None` | Cron schedule string |
| `cpu` | No | `None` | Cloud Run CPU setting |
| `memory` | No | `None` | Cloud Run memory setting |
| `timeout` | No | `None` | Timeout in seconds |
| `deps` | No | `[]` | Additional Bazel dependencies |

## GCP Cloud Run Configuration

Set these environment variables for GCP integration:

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | Overrides `image_repository` with `{region}-docker.pkg.dev/{project}/prefect` |
| `GCP_REGION` | GCP region (default: `us-central1`) |
| `ENVIRONMENT` | Set to `local` for local development, or your env name for remote deploys |

## Local Development

For local development without pushing images:

```bash
ENVIRONMENT=local bazel run //my/flow:deploy
```

This uses `BUILD_WORKSPACE_DIRECTORY` to mount your local source tree into the Docker container instead of baking it into the image.

## Apple Silicon

On Apple Silicon Macs, Prefect's built-in Docker build produces arm64 images. Use `--buildx_push` for amd64 work pools:

```bash
bazel run //my/flow:deploy -- --buildx-push
```

## License

Apache 2.0
