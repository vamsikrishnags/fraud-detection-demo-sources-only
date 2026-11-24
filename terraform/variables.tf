# The only values a user must provide: the Confluent Cloud API key/secret.
# `uv run deploy` writes them to terraform.tfvars from credentials.env.
# Region, names and sizing are fixed in main.tf locals.

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (Cloud resource management)."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret."
  type        = string
  sensitive   = true
}

# Not used by Terraform to create anything: it is written into bob-config.json so
# Bob builds CREATE MODEL against Confluent-hosted Claude (no AWS keys).
variable "csp_hosted_model" {
  description = "Model id Bob should use in CREATE MODEL ('confluent.model'). Case-sensitive."
  type        = string
  default     = "anthropic.claude-sonnet-4-6"
}

# Optional: deploy into an EXISTING environment instead of creating one.
# Needs an existing service account (it owns the generated keys); the Cloud API
# key above must be able to manage resources in that environment (EnvironmentAdmin).
variable "environment_id" {
  description = "Existing Confluent environment id (env-xxxxx). Empty = create a new environment."
  type        = string
  default     = ""
}

variable "service_account_id" {
  description = "Existing service account id (sa-xxxxx) to own keys; required with environment_id."
  type        = string
  default     = ""
}
