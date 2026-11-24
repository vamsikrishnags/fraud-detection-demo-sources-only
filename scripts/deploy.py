#!/usr/bin/env python3
"""
Deploy the datagen half of the fraud-detection demo to Confluent Cloud.

Creates the environment, Kafka cluster, Schema Registry, a Flink compute pool,
scoped API keys, and the three INPUT topics (as Flink tables, so their Avro
schemas are registered). Nothing LLM-related is deployed: IBM Bob builds the
windowing, agent, alerts and dashboard from bob-config.json.

Usage:
  uv run deploy               # prompts once for the Confluent Cloud key/secret, saves credentials.env
  uv run deploy --datagen     # same thing (this project only does the datagen part)
  uv run deploy --automated   # non-interactive, from credentials.env
"""

import argparse
import json
import os
import sys

from dotenv import dotenv_values, set_key

from scripts.credentials import load_or_create_credentials_file
from scripts.terraform import get_project_root, terraform_dir
from scripts.terraform_runner import run_terraform
from scripts.tfvars import write_tfvars_for_deployment
from scripts.ui import prompt_with_default

KEY = "TF_VAR_confluent_cloud_api_key"
SECRET = "TF_VAR_confluent_cloud_api_secret"


def pretty_print_config(path) -> None:
    """Terraform's jsonencode is one line; make it readable for a human (and Bob)."""
    data = json.loads(path.read_text())
    path.write_text(json.dumps(data, indent=2) + "\n")
    path.chmod(0o600)


ENV_ID = "DATAGEN_ENVIRONMENT_ID"
SA_ID = "DATAGEN_SERVICE_ACCOUNT_ID"


def main(argv=None):
    parser = argparse.ArgumentParser(description="Deploy the datagen half of the demo (topics + schemas only)")
    parser.add_argument("--datagen", action="store_true", help="accepted for clarity; this is the only mode")
    parser.add_argument("--automated", action="store_true", help="non-interactive: use credentials.env as-is")
    args = parser.parse_args(argv)

    print("=== Datagen for Bob — Deploy ===\n")
    root = get_project_root()
    print(f"Project root: {root}")
    creds_file, creds = load_or_create_credentials_file(root)

    if not args.automated:
        creds[KEY] = prompt_with_default("Confluent Cloud API Key", creds.get(KEY, ""))
        creds[SECRET] = prompt_with_default("Confluent Cloud API Secret", creds.get(SECRET, ""), secret=True)
        set_key(str(creds_file), KEY, creds[KEY])
        set_key(str(creds_file), SECRET, creds[SECRET])
        print("\nOptional: deploy into an EXISTING environment (leave blank to create a new one).")
        creds[ENV_ID] = prompt_with_default("Existing environment ID (env-xxxxx)", creds.get(ENV_ID, ""))
        set_key(str(creds_file), ENV_ID, creds[ENV_ID])
        if creds[ENV_ID]:
            creds[SA_ID] = prompt_with_default("Service account ID that owns the keys (sa-xxxxx)", creds.get(SA_ID, ""))
            set_key(str(creds_file), SA_ID, creds[SA_ID])
        if input("\nDeploy topics + schemas to us-east-1 now? (y/n): ").strip().lower() != "y":
            print("Deployment cancelled.")
            sys.exit(0)
        creds = dict(dotenv_values(str(creds_file)))

    print("\n=== Starting Deployment ===")
    if not write_tfvars_for_deployment(root, creds):
        print("Error: missing Confluent Cloud API key/secret in credentials.env.")
        sys.exit(1)
    for key, value in creds.items():
        if value:
            os.environ[key] = value
    if not run_terraform(terraform_dir(root)):
        print("\nDeployment failed. Fix the error above and rerun `uv run deploy`.")
        sys.exit(1)

    config = root / "bob-config.json"
    if config.exists():
        pretty_print_config(config)
    print("\n✓ Deployed: topics transactions, user_logins, account_changes (+ Avro schemas).")
    print(f"✓ Config for Bob: {root / 'config.py'}")
    print("\nNext:")
    print("  uv run producer                 # continuous normal traffic, a fraud scenario every ~4 min")
    print("  uv run producer --inject geo    # on demand: geo | takeover | velocity")
    print("  Then give Bob config.py and BOB.md.")


if __name__ == "__main__":
    main()
