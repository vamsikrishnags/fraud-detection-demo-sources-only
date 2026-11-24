# Shared args for every Flink statement (passed to the flink-statement module).
locals {
  flink_common = {
    organization_id  = data.confluent_organization.main.id
    environment_id   = local.env_id
    compute_pool_id  = confluent_flink_compute_pool.main.id
    principal_id     = local.sa_id
    rest_endpoint    = data.confluent_flink_region.main.rest_endpoint
    flink_api_key    = confluent_api_key.flink.id
    flink_api_secret = confluent_api_key.flink.secret
    catalog          = local.catalog
    database         = local.database
  }
}

# ----------------------------- Source tables -------------------------------
# Created by Flink so the topics + Avro value schemas exist before the producer
# runs and before the detection statement is deployed. `timestamp` is epoch ms;
# event_time is a computed time attribute used for the session window.

module "tbl_transactions" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-transactions-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `transactions` (
      `user_id` STRING NOT NULL,
      `transaction_id` STRING NOT NULL,
      `amount` DOUBLE NOT NULL,
      `merchant` STRING NOT NULL,
      `merchant_category` STRING NOT NULL,
      `location` STRING NOT NULL,
      `timestamp` BIGINT NOT NULL,
      `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    );
  EOT
}

module "tbl_user_logins" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-user-logins-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `user_logins` (
      `user_id` STRING NOT NULL,
      `ip_address` STRING NOT NULL,
      `device_id` STRING NOT NULL,
      `location` STRING NOT NULL,
      `timestamp` BIGINT NOT NULL,
      `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    );
  EOT
}

module "tbl_account_changes" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-account-changes-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `account_changes` (
      `user_id` STRING NOT NULL,
      `field_changed` STRING NOT NULL,
      `old_value` STRING NOT NULL,
      `new_value` STRING NOT NULL,
      `timestamp` BIGINT NOT NULL,
      `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    );
  EOT
}

