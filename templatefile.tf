variable "ccloud-templatefile" {
  default = "ccloud.tftpl"
}

variable "kcat-templatefile" {
  default = "kcat.tftpl"
}

variable "ccloud-properties" {
  default = "ccloud.properties"
}

variable "kcat-properties" {
  default = "kcat.properties"
}

variable "env-templatefile" {
  default = "environment.tftpl"
}

variable "env-properties" {
  default = "env.sh"
}

resource "local_file" "ccloud-properties" {
  content = templatefile("${path.module}/${var.ccloud-templatefile}", {
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

resource "local_file" "kcat-properties" {
  content = templatefile("${path.module}/${var.kcat-templatefile}", {
    bootstrap_server = confluent_kafka_cluster.bootcamp.bootstrap_endpoint
    api_key = confluent_api_key.app-manager-kafka-api-key.id
    api_secret = confluent_api_key.app-manager-kafka-api-key.secret
    schema_registry_url = replace(confluent_schema_registry_cluster.essentials.rest_endpoint, "https://", "")
    schema_api_key = confluent_api_key.bootcamp-schema-registry-api-key.id
    schema_api_secret = confluent_api_key.bootcamp-schema-registry-api-key.secret
  })
  filename = var.kcat-properties
  file_permission = "0644"
}

resource "local_file" "environment" {
  content = templatefile("${path.module}/${var.env-templatefile}", {
    schema_registry_url = replace(confluent_schema_registry_cluster.essentials.rest_endpoint, "https://", "")
    schema_api_key = confluent_api_key.bootcamp-schema-registry-api-key.id
    schema_api_secret = confluent_api_key.bootcamp-schema-registry-api-key.secret
  })
  filename = var.env-properties
  file_permission = "0644"
}
