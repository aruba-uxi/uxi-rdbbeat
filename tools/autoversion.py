# pyright: reportAny=false
import http.client
import json
import logging
import os
import pathlib
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from typing import Any, override
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("autoversion")

AUTOVERSION_PR = os.getenv("AUTOVERSION_PR", "PR VAR NOT SET")
AUTOVERSION_REPO = os.getenv("AUTOVERSION_REPO", "REPO VAR NOT SET")
AUTOVERSION_TOML = os.getenv("AUTOVERSION_TOML", "pyproject.toml")
AUTOVERSION_SLACK_URL = os.getenv("AUTOVERSION_SLACK_URL", "SLACK URL NOT SET")


@dataclass
class VersionedAsset:
    name: str
    old_version: str
    new_version: str

    @override
    def __str__(self) -> str:
        """Custom str implementation."""
        return f"{self.name}: {self.old_version} -> {self.new_version}"


def _notify(slack_url: str, pr: str, source_repo: str, releases: list[VersionedAsset]) -> None:
    release_txt = "\n".join([str(release) for release in releases])

    url = slack_url
    payload = {
        "pr": pr,
        "source-repo": source_repo,
        "releases": release_txt,
    }

    parsed = urlparse(url)
    conn = http.client.HTTPSConnection(parsed.netloc)
    headers = {"Content-Type": "application/json"}
    conn.request("POST", parsed.path, body=json.dumps(payload), headers=headers)
    response = conn.getresponse()
    print(response.status, response.reason)  # noqa: T201
    print(response.read().decode())  # noqa: T201
    conn.close()


def autoversion(  # noqa: PLR0913
    name: str,
    version_file: str,
    version_prefix: str,
    watch_paths: list[str],
    yaml_path: str | None = None,
    yaml_key: str | None = None,
) -> VersionedAsset | None:
    """Bump version using convco, commit/tag if changed, and return the new version string or None."""
    msg = f"Running autoversion with version_file={version_file}, version_prefix={version_prefix}, watch_paths={watch_paths}, yaml_path={yaml_path}, yaml_key={yaml_key}"
    log.info(msg)

    current_version = "0.0.0"
    with pathlib.Path(version_file).open() as vf:
        current_version = vf.read().strip()
    # Build convco command
    cmd = ["convco", "version", "--bump", "--prefix", version_prefix]
    for path in watch_paths:
        cmd.extend(["-P", path])
    with pathlib.Path(version_file).open("w") as vf:
        _ = subprocess.run(cmd, check=True, stdout=vf)  # noqa: S603

    # Read version value
    with pathlib.Path(version_file).open() as vf:
        version_value = vf.read().strip()

    # Check if there are changes to commit
    result = subprocess.run(["git", "diff", "--quiet", "HEAD"], check=False)  # noqa: S603, S607
    if result.returncode == 0:
        # No changes
        log.info("No changes detected.")
        return None
    # There are changes
    commit_msg = f"chore: release {version_prefix}{version_value} [skip ci]"
    log.info(commit_msg)
    tag = f"{version_prefix}{version_value}"
    _ = subprocess.run(["git", "commit", "-am", commit_msg], check=True)  # noqa: S603, S607
    _ = subprocess.run(["git", "tag", "-f", "-a", tag, "-m", f"release version: {tag}"], check=True)  # noqa: S603, S607

    if not yaml_path or not yaml_key:
        log.info("No YAML path or key provided, skipping YAML update.")
        return VersionedAsset(name=name, old_version=current_version, new_version=version_value)

    _update_yaml_key(yaml_path, yaml_key, version_value)
    result = subprocess.run(["git", "diff", "--quiet", "HEAD"], check=False)  # noqa: S603, S607
    if result.returncode == 0:
        log.info("No changes detected.")
        # No changes
        return None
    log.info("Changes detected. Committing.")
    commit_msg = f"fix: update {name} to {version_value} [skip ci]"
    log.info(commit_msg)
    _ = subprocess.run(["git", "commit", "-am", commit_msg], check=True)  # noqa: S603, S607
    return VersionedAsset(name=name, old_version=current_version, new_version=version_value)


def _update_yaml_key(yaml_path: str, key: str, value: str) -> None:
    """Update yaml key with yq."""
    _ = subprocess.run(["yq", "-i", f'{key} = "{value}"', yaml_path], check=True)  # noqa: S603, S607


def _validate_config(conf: dict[str, Any], section: str) -> bool:  # pyright: ignore[reportExplicitAny]
    required_fields = ["version_file", "version_prefix", "watch_paths"]
    for field in required_fields:
        if field not in conf:
            msg = f"Missing required field '{field}' in section '{section}'"
            log.error(msg)
            return False
    if not isinstance(conf["watch_paths"], list) or not all(
        isinstance(p, str)
        for p in conf["watch_paths"]  # pyright: ignore[reportUnknownVariableType]
    ):
        msg = f"'watch_paths' must be a list of strings in section '{section}'"
        log.error(msg)
        return False
    return True


def main() -> None:
    """Entrypoint to autoversion process."""
    with pathlib.Path(AUTOVERSION_TOML).open("rb") as config_file:
        config = tomllib.load(config_file)
    tools = config.get("tool", {})
    autoversion_config = tools.get("autoversion", {})
    for section_key, section_value in autoversion_config.items():
        if not _validate_config(section_value, section_key):
            sys.exit(1)

    bumps: list[VersionedAsset] = []
    for name, section_value in autoversion_config.items():
        versioned_asset = autoversion(
            name=name,
            version_file=section_value["version_file"],
            version_prefix=section_value["version_prefix"],
            watch_paths=section_value["watch_paths"],
            yaml_path=section_value.get("yaml_path"),
            yaml_key=section_value.get("yaml_key"),
        )
        if versioned_asset:
            bumps.append(versioned_asset)
            msg = f"Versioned asset: {versioned_asset}"
            log.info(msg)

    if len(bumps) == 0:
        log.info("No version bumps detected.")
        return
    _notify(AUTOVERSION_SLACK_URL, AUTOVERSION_PR, AUTOVERSION_REPO, bumps)


if __name__ == "__main__":
    main()
