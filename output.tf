output "bootcamp-cluster" {
  value = confluent_kafka_cluster.bootcamp.bootstrap_endpoint
}

output "schema-registry" {
  value = data.confluent_schema_registry_cluster.essentials.rest_endpoint
}

output "flink-compute-pool" {
  value = confluent_flink_compute_pool.main.resource_name
}

output "app-manager-api-key" {
  value = confluent_api_key.app-manager-kafka-api-key.id
}

output "app-manager-api-secret" {
  value = confluent_api_key.app-manager-kafka-api-key.secret
  sensitive = true
}

output "app-tableflow-api-key" {
  value = confluent_api_key.app-tableflow-api-key.id
}

output "app-tableflow-api-secret" {
  value = confluent_api_key.app-tableflow-api-key.secret
  sensitive = true
}

# The join(", ") is a hack, there should only be one table_format selected or this URL will not be valid

output "tableflow-rest-endpoint" {
  value = "https://tableflow.${confluent_kafka_cluster.bootcamp.region}.aws.confluent.cloud/${join(", ",confluent_tableflow_topic.movement.table_formats)}/catalog/organizations/${data.confluent_organization.bootcamp.id}/environments/${confluent_environment.rails_environment.id}"
}

output "schema-key" {
  value = local.schema_api_key
}

output "schema-secret" {
  value = local.schema_secret
  sensitive = true
}

output "flink-key" {
  value = local.flink_api_key
}

output "flink-secret" {
  value = local.flink_api_secret
  sensitive = true
}

data "confluent_ip_addresses" "connect" {
  filter {
    clouds        = ["AWS"]
    regions       = [var.confluent_region]
    services      = ["CONNECT"]
    address_types = ["EGRESS"]
  }
}

output "ip_addresses" {
  value = data.confluent_ip_addresses.connect.ip_addresses.*.ip_prefix
}
