variable "confluent_api_key" {
  type = string
}

variable "confluent_api_secret" {
  type      = string
  sensitive = true
}

variable "nrod_username" {
  type = string
}

variable "nrod_password" {
  type = string
  sensitive = true
}

variable "confluent_region" {
  type = string
}

variable "confluent_schema_region" {
  type = string
}

variable "confluent_environment" {
  type = string
}

variable "confluent_cluster" {
  type = string
}

variable "confluent_env_id" {
  type    = string
  default = ""
}

variable "confluent_custom_connection_endpoints" {
  type = string
  default = ""
}

variable "cif_total_http_url" {
  type = string
}
