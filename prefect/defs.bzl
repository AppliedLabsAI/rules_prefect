"""Public API for rules_prefect.

Usage:
    load("@rules_prefect//prefect:defs.bzl", "prefect_deploy")
"""

load("//prefect/private:deploy.bzl", _prefect_deploy = "prefect_deploy")

prefect_deploy = _prefect_deploy
