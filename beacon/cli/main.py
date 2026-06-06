"""Root Click group for the ``beacon`` CLI.

Exit code contract (stable across releases):

* ``0`` — success.
* ``1`` — runtime / operational failure (DAG plan failed, no logs
  found, deployment not found, etc.). Caller should inspect ``stderr``.
* ``2`` — invocation error (bad flags, missing required input,
  malformed cron, unknown subcommand). This is the standard Click
  ``UsageError`` exit code; downstream automation can treat it as
  "fix your command line and retry".

Every command must respect this contract: never ``sys.exit(0)`` on
failure, never use exit codes ≥ 3.
"""

import click

from .commands import (
    api_cmd,
    config_cmd,
    deploy_cmd,
    deployment_cmd,
    list_cmd,
    logs_cmd,
    plan_cmd,
    run_cmd,
    scheduler_cmd,
    sync_cmd,
    test_cmd,
    trigger_cmd,
)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(package_name="beacon", prog_name="beacon")
def cli() -> None:
    """Beacon — an everyday workflow orchestrator."""


cli.add_command(plan_cmd.plan)
cli.add_command(test_cmd.test)
cli.add_command(run_cmd.run)
cli.add_command(deploy_cmd.deploy)
cli.add_command(deployment_cmd.deployment_cmd)
cli.add_command(sync_cmd.sync)
cli.add_command(trigger_cmd.trigger)
cli.add_command(scheduler_cmd.scheduler)
cli.add_command(api_cmd.api)
cli.add_command(logs_cmd.logs)
cli.add_command(list_cmd.list_cmd)
cli.add_command(config_cmd.config)
