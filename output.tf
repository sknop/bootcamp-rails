output "bootcamp-cluster" {
  value = confluent_kafka_cluster.bootcamp.bootstrap_endpoint
}

output "schema-registry" {
  value = confluent_schema_registry_cluster.essentials.rest_endpoint
}

output "ksql-server" {
  value = confluent_ksql_cluster.bootcamp.rest_endpoint
}

output "app-manager-api-key" {
  value = confluent_api_key.app-manager-kafka-api-key.id
}

output "app-manager-api-secret" {
  value = confluent_api_key.app-manager-kafka-api-key.secret
  sensitive = true
}

output "Schema-Key" {
  value = local.schema_api_key
}

output "Schema-Secret" {
  value = local.schema_secret
  sensitive = true
}
