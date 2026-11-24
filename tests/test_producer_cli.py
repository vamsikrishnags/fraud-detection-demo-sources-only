"""The producer's CLI and config loading must be safe without a deployment."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from producer import generate_events as ge

ROOT = Path(__file__).resolve().parents[1]


def run_producer(*args):
    return subprocess.run(
        [sys.executable, "-m", "producer.generate_events", *args],
        cwd=ROOT, capture_output=True, text=True, timeout=20,
    )


@pytest.mark.parametrize("flag", ["--help"])
def test_help_without_deployment(flag):
    result = run_producer(flag)
    assert result.returncode == 0, result.stderr
    assert "--fraud-every" in result.stdout and "--inject" in result.stdout


def test_negative_fraud_every_rejected():
    assert run_producer("--fraud-every", "-1").returncode == 2


def test_inject_rejects_unknown_scenario():
    assert run_producer("--inject", "nope").returncode == 2


def test_load_config_reads_bob_config(tmp_path):
    cfg = tmp_path / "bob-config.json"
    cfg.write_text(json.dumps({
        "kafka": {"bootstrap_servers": "b:9092", "api_key": "k", "api_secret": "s"},
        "schema_registry": {"url": "https://sr", "api_key": "sk", "api_secret": "ss"},
    }))
    assert ge.load_config(str(cfg)) == {
        "bootstrap": "b:9092", "kafka_key": "k", "kafka_secret": "s",
        "sr_url": "https://sr", "sr_key": "sk", "sr_secret": "ss",
    }


def test_load_config_missing_file_is_empty(tmp_path):
    assert ge.load_config(str(tmp_path / "nope.json")) == {}


def test_every_scenario_is_exposed():
    assert set(ge.FRAUD_SCENARIOS) == {"geo", "takeover", "velocity"}
