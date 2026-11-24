resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  # --- Fixed configuration (not user-facing) ------------------------------
  cloud_provider = "AWS"
  aws_region     = "us-east-1"
  prefix         = "fraud-detection"


  # Existing-environment mode: set var.environment_id (+ var.service_account_id).
  use_existing_env = var.environment_id != ""

  env_id   = local.use_existing_env ? var.environment_id : confluent_environment.main[0].id
  env_name = local.use_existing_env ? data.confluent_environment.existing[0].display_name : confluent_environment.main[0].display_name
  env_crn  = local.use_existing_env ? data.confluent_environment.existing[0].resource_name : confluent_environment.main[0].resource_name

  # Principal that owns the generated API keys and runs Flink statements.
  sa_id = local.use_existing_env ? var.service_account_id : confluent_service_account.app_manager[0].id

  # Catalog/database that Flink statements run against.
  catalog  = local.env_name
  database = confluent_kafka_cluster.standard.display_name
}

# Reuse an existing environment (only the env-scoped key is needed).
data "confluent_environment" "existing" {
  count = local.use_existing_env ? 1 : 0
  id    = var.environment_id
}

resource "confluent_environment" "main" {
  count        = local.use_existing_env ? 0 : 1
  display_name = "${local.prefix}-env-${random_id.suffix.hex}"

  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "standard" {
  display_name = "${local.prefix}-cluster-${random_id.suffix.hex}"
  availability = "SINGLE_ZONE"
  cloud        = local.cloud_provider
  region       = local.aws_region
  standard {}
  environment {
    id = local.env_id
  }
}

data "confluent_schema_registry_cluster" "sr" {
  environment {
    id = local.env_id
  }
  depends_on = [confluent_kafka_cluster.standard]
}

data "confluent_organization" "main" {}

data "confluent_flink_region" "main" {
  cloud  = local.cloud_provider
  region = local.aws_region
}

# ---------------------------------------------------------------------------
# Service account + API keys. A single "app-manager" account (EnvironmentAdmin)
# owns the Kafka, Schema Registry, and Flink keys used by Terraform and the
# local producer and Bob.
# ---------------------------------------------------------------------------

resource "confluent_service_account" "app_manager" {
  count        = local.use_existing_env ? 0 : 1
  display_name = "${local.prefix}-sa-${random_id.suffix.hex}"
  description  = "Service account for the fraud detection pipeline"
}

resource "confluent_role_binding" "app_manager_env_admin" {
  count       = local.use_existing_env ? 0 : 1
  principal   = "User:${confluent_service_account.app_manager[0].id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = local.env_crn
}

resource "confluent_api_key" "kafka" {
  display_name = "${local.prefix}-kafka-key-${random_id.suffix.hex}"
  description  = "Kafka API key for the fraud detection pipeline"
  owner {
    id          = local.sa_id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }
  managed_resource {
    id          = confluent_kafka_cluster.standard.id
    api_version = confluent_kafka_cluster.standard.api_version
    kind        = confluent_kafka_cluster.standard.kind
    environment {
      id = local.env_id
    }
  }
  depends_on = [confluent_role_binding.app_manager_env_admin]
}

resource "confluent_api_key" "schema_registry" {
  display_name = "${local.prefix}-sr-key-${random_id.suffix.hex}"
  description  = "Schema Registry API key for the fraud detection pipeline"
  owner {
    id          = local.sa_id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment {
      id = local.env_id
    }
  }
  depends_on = [confluent_role_binding.app_manager_env_admin]
}

resource "confluent_api_key" "flink" {
  display_name = "${local.prefix}-flink-key-${random_id.suffix.hex}"
  description  = "Flink API key for statement submission"
  owner {
    id          = local.sa_id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }
  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind
    environment {
      id = local.env_id
    }
  }
  depends_on = [confluent_role_binding.app_manager_env_admin]
}

resource "confluent_flink_compute_pool" "main" {
  display_name = "${local.prefix}-pool-${random_id.suffix.hex}"
  cloud        = local.cloud_provider
  region       = local.aws_region
  max_cfu      = 5
  environment {
    id = local.env_id
  }
}

# ---------------------------------------------------------------------------
# Data-plane ACLs so the Kafka API key can produce/consume on all topics
# (the producer writes the 3 input topics; Bob's consumers/dashboard read any topic).
# ---------------------------------------------------------------------------

locals {
  acl_topic_ops = {
    write    = "WRITE"
    read     = "READ"
    create   = "CREATE"
    describe = "DESCRIBE"
  }
}

resource "confluent_kafka_acl" "topic" {
  for_each = local.acl_topic_ops
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${local.sa_id}"
  host          = "*"
  operation     = each.value
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}

resource "confluent_kafka_acl" "group_read" {
  kafka_cluster {
    id = confluent_kafka_cluster.standard.id
  }
  resource_type = "GROUP"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${local.sa_id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.standard.rest_endpoint
  credentials {
    key    = confluent_api_key.kafka.id
    secret = confluent_api_key.kafka.secret
  }
}
