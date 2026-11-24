# The single file handed to Bob. Everything Bob needs to discover the deployment
# (topics, schemas, Flink SQL) and to build the rest of the demo: endpoints, scoped
# API keys, the pre-created input tables, the LLM to use and the cost guardrails.
# Contains secrets: written 0600 and gitignored. Re-run `terraform apply` to refresh.
locals {
  org_id = data.confluent_organization.main.id

  input_columns = {
    transactions = [
      { name = "user_id", type = "STRING" },
      { name = "transaction_id", type = "STRING" },
      { name = "amount", type = "DOUBLE" },
      { name = "merchant", type = "STRING" },
      { name = "merchant_category", type = "STRING" },
      { name = "location", type = "STRING" },
      { name = "timestamp", type = "BIGINT (epoch ms)" },
    ]
    user_logins = [
      { name = "user_id", type = "STRING" },
      { name = "ip_address", type = "STRING" },
      { name = "device_id", type = "STRING" },
      { name = "location", type = "STRING" },
      { name = "timestamp", type = "BIGINT (epoch ms)" },
    ]
    account_changes = [
      { name = "user_id", type = "STRING" },
      { name = "field_changed", type = "STRING" },
      { name = "old_value", type = "STRING" },
      { name = "new_value", type = "STRING" },
      { name = "timestamp", type = "BIGINT (epoch ms)" },
    ]
  }

  bob_config = {
    purpose = "Input streams for a fraud-detection demo are deployed and being generated. Build the rest (windowing, LLM agent, alerts, dashboard) on top. See BOB.md."

    confluent = {
      organization_id  = local.org_id
      environment_id   = local.env_id
      environment_name = local.env_name
      cloud            = local.cloud_provider
      region           = local.aws_region
    }

    kafka = {
      cluster_id        = confluent_kafka_cluster.standard.id
      cluster_name      = confluent_kafka_cluster.standard.display_name
      bootstrap_servers = replace(confluent_kafka_cluster.standard.bootstrap_endpoint, "SASL_SSL://", "")
      rest_endpoint     = confluent_kafka_cluster.standard.rest_endpoint
      api_key           = confluent_api_key.kafka.id
      api_secret        = confluent_api_key.kafka.secret
      security_protocol = "SASL_SSL"
      sasl_mechanism    = "PLAIN"
    }

    schema_registry = {
      url        = data.confluent_schema_registry_cluster.sr.rest_endpoint
      api_key    = confluent_api_key.schema_registry.id
      api_secret = confluent_api_key.schema_registry.secret
    }

    flink = {
      rest_endpoint      = data.confluent_flink_region.main.rest_endpoint
      compute_pool_id    = confluent_flink_compute_pool.main.id
      max_cfu            = confluent_flink_compute_pool.main.max_cfu
      catalog            = local.catalog
      database           = local.database
      service_account_id = local.sa_id
      api_key            = confluent_api_key.flink.id
      api_secret         = confluent_api_key.flink.secret
      statements_url     = "${data.confluent_flink_region.main.rest_endpoint}/sql/v1/organizations/${local.org_id}/environments/${local.env_id}/statements"
      properties_for_every_statement = {
        "sql.current-catalog"  = local.catalog
        "sql.current-database" = local.database
      }
    }

    topics = {
      note = "Each topic is a Flink table created for you (do NOT recreate). Avro value schemas are registered by Flink under subject <topic>-value; keys are user_id (string)."
      input = {
        for name, cols in local.input_columns : name => {
          topic          = name
          schema_subject = "${name}-value"
          key            = "user_id"
          columns        = cols
          computed       = "event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3), WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND"
        }
      }
      output = "None yet. Bob creates the alerts table/topic with its own Flink statement; read its schema back from Schema Registry (subject <topic>-value) for the dashboard."
    }

    llm = {
      provider = "confluent"
      model_id = var.csp_hosted_model
      create_model_with = {
        provider                      = "confluent"
        task                          = "text_generation"
        "confluent.model"             = var.csp_hosted_model
        "confluent.params.max_tokens" = "1024"
      }
      note = "CSP Hosted Flink Model Inference: Confluent-hosted Claude, no AWS keys or connection. model id is case-sensitive."
    }

    cost_guardrails = {
      why = "Input events stream continuously; every LLM call costs money. The LLM must only see a small, gated subset of activity."
      rules = [
        "Never call the LLM per event. Window per user first (event-time SESSION window, 3s gap) and call it at most once per window.",
        "Gate the LLM behind cheap deterministic Flink SQL: only windows with a suspicious signal (e.g. amount >= 500, more than one distinct location in the window, any account_changes row, or >= 4 transactions) may reach the agent. Everything else is dropped or scored 0 without an LLM call.",
        "Start every continuous statement from latest offset: SET 'sql.tables.scan.startup.mode' = 'latest-offset'. Never replay topic history through the model.",
        "Keep model output small (max_tokens above) and keep the agent prompt short.",
        "Make the gate easy to tighten or loosen from one place (a single WHERE / a view) and print the statement name so the presenter can STOP it.",
        "Do not create more than one LLM-backed continuous statement.",
      ]
      kill_switch = "Stop the LLM statement from the Flink UI or REST (PATCH the statement with spec.stopped=true). Data keeps flowing; only LLM spend stops."
    }

    discovery = {
      list_topics       = "GET {kafka.rest_endpoint}/kafka/v3/clusters/{kafka.cluster_id}/topics  (basic auth kafka.api_key:kafka.api_secret)"
      list_subjects     = "GET {schema_registry.url}/subjects  (basic auth schema_registry keys); GET /subjects/<topic>-value/versions/latest"
      list_statements   = "GET {flink.statements_url}  (basic auth flink keys)"
      run_flink_sql     = "POST {flink.statements_url} with {name, spec:{statement, properties, compute_pool_id: {flink.compute_pool_id}, principal: {flink.service_account_id}}}"
      consume_in_python = "confluent-kafka Consumer (SASL_SSL/PLAIN) + AvroDeserializer(SchemaRegistryClient) — the writer schema is fetched automatically by schema id."
    }
  }
}

resource "local_file" "bob_config" {
  filename        = "${path.module}/../bob-config.json"
  file_permission = "0600"
  content         = jsonencode(local.bob_config)

  depends_on = [
    module.tbl_transactions,
    module.tbl_user_logins,
    module.tbl_account_changes,
    confluent_kafka_acl.topic,
    confluent_kafka_acl.group_read,
  ]
}

# Flat, importable settings file for Bob (same style as a hand-written config.py).
# Contains secrets: written 0600 and gitignored.
resource "local_file" "config_py" {
  filename        = "${path.module}/../config.py"
  file_permission = "0600"
  content         = <<-EOT
    # config.py: generated by `uv run deploy`. Contains secrets; do not commit.

    # --- Kafka ---
    BOOTSTRAP_SERVERS      = ${jsonencode(replace(confluent_kafka_cluster.standard.bootstrap_endpoint, "SASL_SSL://", ""))}
    SASL_USERNAME          = ${jsonencode(confluent_api_key.kafka.id)}
    SASL_PASSWORD          = ${jsonencode(confluent_api_key.kafka.secret)}

    # --- Schema Registry ---
    SCHEMA_REGISTRY_URL    = ${jsonencode(data.confluent_schema_registry_cluster.sr.rest_endpoint)}
    SCHEMA_REGISTRY_KEY    = ${jsonencode(confluent_api_key.schema_registry.id)}
    SCHEMA_REGISTRY_SECRET = ${jsonencode(confluent_api_key.schema_registry.secret)}

    # --- Flink ---
    FLINK_REST_ENDPOINT    = ${jsonencode(data.confluent_flink_region.main.rest_endpoint)}
    FLINK_API_KEY          = ${jsonencode(confluent_api_key.flink.id)}
    FLINK_API_SECRET       = ${jsonencode(confluent_api_key.flink.secret)}
    FLINK_ORGANIZATION_ID  = ${jsonencode(local.org_id)}
    FLINK_ENVIRONMENT_ID   = ${jsonencode(local.env_id)}
    FLINK_COMPUTE_POOL_ID  = ${jsonencode(confluent_flink_compute_pool.main.id)}
    FLINK_PRINCIPAL_ID     = ${jsonencode(local.sa_id)}
    FLINK_CATALOG          = ${jsonencode(local.catalog)}
    FLINK_DATABASE         = ${jsonencode(local.database)}

    # --- LLM (Confluent-hosted model, used in CREATE MODEL 'confluent.model') ---
    LLM_MODEL_ID           = ${jsonencode(var.csp_hosted_model)}

    topic_list = ["transactions", "user_logins", "account_changes"]
  EOT

  depends_on = [
    module.tbl_transactions,
    module.tbl_user_logins,
    module.tbl_account_changes,
    confluent_kafka_acl.topic,
    confluent_kafka_acl.group_read,
  ]
}
