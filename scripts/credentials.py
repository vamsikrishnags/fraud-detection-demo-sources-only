"""Load (or create from the example template) credentials.env."""

import shutil
from pathlib import Path

from dotenv import dotenv_values


def load_or_create_credentials_file(root: Path) -> tuple[Path, dict[str, str]]:
    creds_file = root / "credentials.env"
    example_file = root / "credentials.env.example"
    if creds_file.exists():
        return creds_file, dotenv_values(creds_file)
    if example_file.exists():
        shutil.copy(example_file, creds_file)
    else:
        creds_file.touch()
    creds_file.chmod(0o600)
    print(f"\nCreated {creds_file}.")
    return creds_file, {}
