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

variable "confluent_environment" {
  type = string
}

variable "confluent_cluster" {
  type = string
}

variable "bootcamp-app-manager" {
  type = string
  default = "bootcamp-app-manager"
}

variable "flink_pool_name" {
  type = string
  default = "rails_pool"
}

variable "confluent_custom_connection_endpoints" {
  type = string
  default = ""
}

variable "cif_total_http_url" {
  type = string
}

variable "cif_update_mon_http_url" {
  type = string
}

variable "cif_update_tue_http_url" {
  type = string
}

variable "cif_update_wed_http_url" {
  type = string
}

variable "cif_update_thu_http_url" {
  type = string
}

variable "cif_update_fri_http_url" {
  type = string
}

variable "cif_update_sat_http_url" {
  type = string
}

variable "cif_update_sun_http_url" {
  type = string
}
