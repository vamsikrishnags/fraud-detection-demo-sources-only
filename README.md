# Datagen for Bob

Deploys **only the data-generation half** of the Confluent Cloud fraud-detection demo:
the environment, Kafka cluster, Schema Registry, Flink compute pool, scoped API keys
and three **input topics** (`transactions`, `user_logins`, `account_changes`) with
their Avro schemas. No Flink pipeline, no LLM, no dashboard: **IBM Bob builds those**
from `bob-config.json` and [BOB.md](BOB.md).

This is a standalone copy; it shares no state with the full `demo-confluent-fraud-agent` repo.

## Credentials (do this first)

Put your Confluent credentials in `credentials.env` (copy `credentials.env.example`, or let `uv run deploy` prompt and save them):

```
TF_VAR_confluent_cloud_api_key=...
TF_VAR_confluent_cloud_api_secret=...
DATAGEN_ENVIRONMENT_ID=env-xxxxx          # optional: reuse this environment
DATAGEN_SERVICE_ACCOUNT_ID=sa-xxxxx       # required with the line above
```

- **Existing environment:** set both IDs. The key only needs `EnvironmentAdmin` on that environment. It must have Stream Governance (Schema Registry) enabled. Deploy creates a new cluster, Flink pool, keys and topics inside it; the environment itself is never created or deleted.
- **New environment:** leave the two IDs blank. This needs an org-level Cloud resource management key.

## Run it

Prerequisites: [uv](https://docs.astral.sh/uv/), [Terraform](https://developer.hashicorp.com/terraform/install),
a Confluent Cloud account and a *Cloud resource management* API key + secret.

```bash
uv run deploy                     # prompts for the key/secret once; creates everything (~5 min)
uv run producer                   # continuous normal traffic + one fraud scenario every ~4 min
```

`uv run deploy` writes **`config.py`** (Kafka, Schema Registry, Flink settings and `topic_list`). Give Bob that file and `BOB.md`. (`bob-config.json` is also written; the producer reads it.)

## Controlling LLM cost

The producer decides how much suspicious activity exists; Bob's Flink SQL decides how
much of it reaches the LLM (BOB.md makes a cheap SQL gate mandatory).

| Command | Effect |
|---|---|
| `uv run producer` | normal traffic, fraud scenario every 10 cycles (~4 min) |
| `uv run producer --fraud-every 0` | normal traffic only: no fraud signal at all |
| `uv run producer --fraud-every 3` | busier demo |
| `uv run producer --inject geo` | one fraud scenario now, then exit (`geo`, `takeover`, `velocity`; `--user user-007`) |

Stopping the producer stops all new input. Stopping Bob's LLM statement stops LLM spend
while data keeps flowing.

## Cleanup

```bash
uv run destroy                    # deletes the whole environment, including whatever Bob built in it
```

## What's in `config.py` / `bob-config.json`

Kafka bootstrap/REST endpoint + key, Schema Registry URL + key, Flink REST endpoint,
compute pool, catalog/database, service account + key (scoped keys, one per service,
because Confluent Cloud keys are per-resource), the input topics and columns,
the CSP-hosted Claude model settings, cost guardrails and discovery hints.
It contains secrets and is gitignored.
