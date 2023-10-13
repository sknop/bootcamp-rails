output "bootcamp-cluster" {
  value = confluent_kafka_cluster.bootcamp
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
