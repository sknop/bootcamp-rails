output "bootcamp-cluster" {
  value = confluent_kafka_cluster.bootcamp-cluster
}

output "API-Key" {
  value = local.api_key
}

output "API-Secret" {
  value = local.secret
  sensitive = true
}

output "Schema-Key" {
  value = local.schema_api_key
}

output "Schema-Secret" {
  value = local.schema_secret
  sensitive = true
}
