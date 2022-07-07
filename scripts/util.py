import subprocess
import time

import click


def error(*args, **kwargs):
    click.secho(fg="red", *args, **kwargs)
    raise RuntimeError()


def sleep(duration: float, reason: str = ""):
    msg = f"Waiting for {duration} seconds"
    if reason:
        msg += f" ({reason})"
    click.echo(msg)
    time.sleep(duration)


def run(
    cmd: str,
    *,
    echo: bool = True,
    retries: int = 0,
    time_between_retries: float = 30,
    **kwargs,
) -> subprocess.CompletedProcess:
    if echo:
        click.secho(f"> {cmd}", fg="cyan")
    try:
        return subprocess.run(cmd, shell=True, check=True, **kwargs)
    except subprocess.CalledProcessError as e:
        if retries == 0:
            raise e
        click.secho(f"> {e.cmd} failed with code {e.returncode}", fg="yellow")
        sleep(time_between_retries, f"{retries} times left")
        return run(
            cmd,
            retries=retries - 1,
            time_between_retries=time_between_retries,
            **kwargs,
        )


def run_output(cmd: str, **kwargs) -> str:
    return (
        run(cmd, stdout=subprocess.PIPE, echo=False, **kwargs)
        .stdout.decode("ascii")
        .strip()
    )
