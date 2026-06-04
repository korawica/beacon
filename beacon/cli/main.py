"""Root Click group for the ``beacon`` CLI."""

import click

from .commands import (
    config_cmd,
    deploy_cmd,
    deployment_cmd,
    dryrun_cmd,
    list_cmd,
    logs_cmd,
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


cli.add_command(dryrun_cmd.dryrun)
cli.add_command(test_cmd.test)
cli.add_command(run_cmd.run)
cli.add_command(deploy_cmd.deploy)
cli.add_command(deployment_cmd.deployment_cmd)
cli.add_command(sync_cmd.sync)
cli.add_command(trigger_cmd.trigger)
cli.add_command(scheduler_cmd.scheduler)
cli.add_command(logs_cmd.logs)
cli.add_command(list_cmd.list_cmd)
cli.add_command(config_cmd.config)
