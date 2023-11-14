variable "templatefile" {
  default = "ccloud.tftpl"
}

variable "ccloud-properties" {
  default = "ccloud.properties"
}

resource "local_file" "ccloud-properties" {
  content = templatefile("${path.module}/${var.templatefile}", {
    bootstrap_server = confluent_kafka_cluster.bootcamp.bootstrap_endpoint
    api_key = confluent_api_key.app-manager-kafka-api-key.id
    api_secret = confluent_api_key.app-manager-kafka-api-key.secret
    schema_registry_url = confluent_schema_registry_cluster.essentials.rest_endpoint
    schema_api_key = confluent_api_key.bootcamp-schema-registry-api-key.id
    schema_api_secret = confluent_api_key.bootcamp-schema-registry-api-key.secret
  })
  filename = var.ccloud-properties
  file_permission = "0644"
}
