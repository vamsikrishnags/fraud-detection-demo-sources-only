#!/usr/bin/env python3
"""
Destroy the datagen deployment (everything in its Confluent Cloud environment).

Usage:
  uv run destroy              # interactive confirmation
  uv run destroy --testing    # non-interactive
  uv run destroy --force      # clean local files even if terraform destroy fails
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

from dotenv import dotenv_values

from .terraform import get_project_root, terraform_dir
from .terraform_runner import run_terraform_destroy


def cleanup_terraform_artifacts(env_path: Path) -> None:
    """Remove state, generated tfvars and provider caches after a successful destroy."""
    try:
        for path in [*env_path.glob("*.tfstate*"), *env_path.glob("*.tfvars"), *env_path.glob("*.tfvars.backup")]:
            path.unlink()
        if (env_path / ".terraform").exists():
            shutil.rmtree(env_path / ".terraform")
        lock_file = env_path / ".terraform.lock.hcl"
        if lock_file.exists():
            lock_file.unlink()
    except OSError:
        pass  # leftover local files are harmless


def main(argv=None):
    parser = argparse.ArgumentParser(description="Destroy the datagen deployment")
    parser.add_argument("--testing", action="store_true", help="non-interactive")
    parser.add_argument("--force", action="store_true", help="clean local state even if destroy fails")
    args = parser.parse_args(argv)

    root = get_project_root()
    env_path = terraform_dir(root)
    if not (env_path / "terraform.tfstate").exists():
        print("⊘ Nothing to destroy: no terraform state found (never deployed)")
        return
    creds_file = root / "credentials.env"
    if not creds_file.exists():
        print("Error: credentials.env not found.")
        sys.exit(1)
    if not args.testing:
        print("⚠️  This permanently destroys the datagen Confluent Cloud resources (cluster, topics, Flink pool, keys).")
        print("   A newly created environment is deleted with everything Bob built in it; an existing one (DATAGEN_ENVIRONMENT_ID) is kept,")
        print("   but its cluster is deleted along with anything Bob built on it.")
        if input("\nAre you sure? (y/n): ").strip().lower() != "y":
            print("Destroy cancelled.")
            sys.exit(0)
    for key, value in dotenv_values(str(creds_file)).items():
        if value:
            os.environ[key] = value

    if run_terraform_destroy(env_path):
        cleanup_terraform_artifacts(env_path)
    elif args.force:
        print("  ⚠ Destroy failed but --force set: cleaning local state")
        cleanup_terraform_artifacts(env_path)
    else:
        print("\n✗ Destroy failed. Fix the error and rerun, or use --force to clean local state anyway.")
        sys.exit(1)

    for name in ("bob-config.json", "config.py"):
        generated = root / name
        if generated.exists():
            generated.unlink()
            print(f"✓ Removed {name}")
    print("\n✓ Destroy completed!")


if __name__ == "__main__":
    main()
