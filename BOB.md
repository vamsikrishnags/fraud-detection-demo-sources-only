# Real-Time Fraud Detection with Confluent Cloud and IBM Bob

Lab guide: prompts to give Bob and the output to expect.

## Scenario

Transactions, logins and account changes stream into Confluent Cloud. Bob builds a
Flink SQL pipeline that groups each user's activity into sessions, gates the
suspicious ones with cheap SQL, scores only those with a Confluent-hosted Claude
agent, and shows the alerts on a live dashboard.

## What is already deployed

| Item | Detail |
|---|---|
| Connection settings | `config.py` (Kafka, Schema Registry, Flink, model id, `topic_list`) |
| Input topics | `transactions`, `user_logins`, `account_changes` (Avro schemas registered) |
| Traffic | `uv run producer` (normal activity, a fraud scenario about every 4 min) |

| Topic | Columns |
|---|---|
| `transactions` | user_id, transaction_id, amount, merchant, merchant_category, location, timestamp |
| `user_logins` | user_id, ip_address, device_id, location, timestamp |
| `account_changes` | user_id, field_changed, old_value, new_value, timestamp |

`timestamp` is epoch ms. Each table also has a computed `event_time` with a 5 s watermark.

> `config.py` contains secrets. Never print the keys, commit the file or paste it anywhere.

## Rules Bob must follow (paste these first)

**Prompt 0: ground rules**

```text
Read config.py in this folder for all connection details. Never print API keys or secrets.
Rules for everything you build:
1. Every continuous Flink statement starts with SET 'sql.tables.scan.startup.mode' = 'latest-offset';
2. The LLM must never run per event. Cheap deterministic SQL decides what reaches it.
3. Use exactly one LLM-backed continuous statement.
4. Check SHOW TABLES / SHOW MODELS / SHOW AGENTS before any CREATE (names are case-sensitive).
5. Tell me the name of every statement you start and how to stop it.
Acknowledge these rules and wait for the next prompt.
```

**Expected output:** Bob restates the five rules and does nothing else.

---

## Step 1: Discover what exists

**Prompt 1**

```text
Using config.py, list the Kafka topics, the Schema Registry subjects and the Flink
statements. Confirm that transactions, user_logins and account_changes exist with
the columns in BOB.md. Do not create anything.
```

**Expected output**
- Three topics found: `transactions`, `user_logins`, `account_changes`.
- Three subjects found: `transactions-value`, `user_logins-value`, `account_changes-value`.
- Three completed `create-table-*` statements.
- A short table of each topic's columns that matches BOB.md.

---

## Step 2: Per-user activity profiles

**Prompt 2**

```text
Union the three streams into one per-user view. Use an event-time SESSION window
(3 second gap, partitioned by user_id) to build one activity profile per user session:
logins, transactions, account changes, distinct locations, total and max amount.
Materialize it as a table called activity_profiles. Set
'sql.tables.scan.idle-timeout' = '5 s' on this statement. Show me the SQL, then run it.
```

**Expected output**
- A `activity_profiles` table is created and its statement is RUNNING.
- `SELECT * FROM activity_profiles` returns rows within about a minute, roughly one per user burst.
- The table shows up in Stream Lineage between the three inputs and the new topic.

---

## Step 3: The cost gate

**Prompt 3**

```text
Create a view called suspicious_profiles on top of activity_profiles that keeps only
profiles with at least one of: a transaction amount >= 500, more than one distinct
location, any account change, or 4 or more transactions. Put the thresholds in this one
view so I can tune them. Then show how many profiles pass the gate versus the total.
```

**Expected output**
- View `suspicious_profiles` exists.
- Under normal traffic the gate passes (almost) nothing; a count query shows 0 or very few rows.
- The thresholds appear in a single `WHERE` clause.

---

## Step 4: Model and agent

**Prompt 4**

```text
Create a model on Confluent-hosted Claude using LLM_MODEL_ID from config.py
(provider 'confluent', task 'text_generation', max_tokens 1024, no AWS keys).
Then create a Streaming Agent that scores a profile from 0 to 100 for fraud and answers
with one JSON object: {user_id, risk_score, reasoning}. Fraud patterns to look for:
geo-impossible travel (login in one city, purchase in another seconds later), account
takeover (email or password change followed by a large purchase), and velocity (many
rapid purchases). Decide the score first and make the final message the JSON object only.
Set max_consecutive_failures and handle_exception = 'continue'.
```

**Expected output**
- `SHOW MODELS` lists the new model and `SHOW AGENTS` lists the agent.
- A test call on one sample profile returns valid JSON with a `risk_score` between 0 and 100.

---

## Step 5: Alerts

**Prompt 5**

```text
Run the agent with AI_RUN_AGENT on suspicious_profiles only, and write the parsed result
to a table called fraud_alerts (user_id, risk_score, reasoning, plus the profile fields).
Parse the agent text defensively: extract the JSON object, use JSON_VALUE and COALESCE
defaults. Tell me the statement name and the command to stop it.
```

**Expected output**
- One RUNNING statement, with its name printed, plus the stop command (PATCH `spec.stopped=true` or the Flink UI).
- A `fraud_alerts` topic with a registered `fraud_alerts-value` schema.
- No rows yet if the producer is idle.

---

## Step 6: Prove the cost gate works

**Prompt 6**

```text
Prove the LLM is not called on normal traffic. I will run the producer with
--fraud-every 0. Wait one minute, then show the number of agent calls and the row count
of fraud_alerts. After that I will run a fraud scenario and you show the new alerts.
```

Run in a terminal:

```bash
uv run producer --fraud-every 0          # normal traffic only
uv run producer --inject takeover        # then one fraud scenario (geo | takeover | velocity)
```

**Expected output**
- After the idle minute: **0 agent calls** and 0 new alerts.
- After the injection: one or a few alerts for `user-007` with a high `risk_score` (about 70+) and readable `reasoning`.

---

## Step 7: Dashboard

**Prompt 7**

```text
Build a small local Streamlit app that shows live input counts for the three topics and a
table of fraud_alerts, newest first. Read the alerts schema from Schema Registry
(GET /subjects/fraud_alerts-value/versions/latest), not from memory, and use
AvroDeserializer. Consume with a unique group.id and auto.offset.reset=latest.
Give me the command to start it.
```

**Expected output**
- `streamlit run app.py` opens a page with three live counters that increase while the producer runs.
- Alerts appear in the table within seconds of a `--inject` run.

---

## Wrap-up

**Prompt 8**

```text
List everything you created (tables, views, model, agent, statements, files), how to stop
each running statement and how to delete each object.
```

**Expected output:** a complete inventory with stop and delete commands, so the presenter
can stop LLM spend instantly while data keeps flowing.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `activity_profiles` stays empty | Add `SET 'sql.tables.scan.idle-timeout' = '5 s';` to that statement. |
| Agent statement stops | Harden the prompt (final message is the JSON only) and set `handle_exception = 'continue'`. |
| `CREATE` fails | The name already exists; check `SHOW ...` first. |
| Changed SQL has no effect | A running statement cannot be edited: stop it and recreate. |
| Flink REST returns 401 | Use `FLINK_API_KEY` / `FLINK_API_SECRET` from `config.py` with basic auth. |
