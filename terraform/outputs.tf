output "bob_config_path" {
  value       = local_file.bob_config.filename
  description = "The single config file to hand to Bob (contains secrets; gitignored)."
}

output "confluent_environment_id" {
  value = local.env_id
}

output "confluent_environment_display_name" {
  value = local.env_name
}

output "flink_catalog" {
  value = local.env_name
}

output "flink_database" {
  value = confluent_kafka_cluster.standard.display_name
}

output "flink_compute_pool_id" {
  value = confluent_flink_compute_pool.main.id
}
