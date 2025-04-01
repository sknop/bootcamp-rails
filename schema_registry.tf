
resource "confluent_service_account" "bootcamp-schema-registry-manager" {
  display_name = "bootcamp_schema_registry_manager"
  description  = "Service account to manage schemas under 'stream_bootcamp' environment on Confluent Cloud"
}

resource "confluent_role_binding" "schema-registry-resource-owner" {
  principal   = "User:${confluent_service_account.bootcamp-schema-registry-manager.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.essentials.resource_name}/subject=*"
}

data "confluent_schema_registry_cluster" "essentials" {
  environment {
    id = confluent_environment.rails_environment.id
  }

  depends_on = [
    confluent_kafka_cluster.bootcamp
  ]
}

resource "confluent_api_key" "bootcamp-schema-registry-api-key" {
  display_name = "bootcamp-schema-registry-api-key"
  description = "Schema Registry API Key used by the bootcamp students"
  owner {
    id            = confluent_service_account.bootcamp-schema-registry-manager.id
    api_version   = confluent_service_account.bootcamp-schema-registry-manager.api_version
    kind          = confluent_service_account.bootcamp-schema-registry-manager.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.essentials.id
    api_version = data.confluent_schema_registry_cluster.essentials.api_version
    kind        = data.confluent_schema_registry_cluster.essentials.kind

    environment {
      id = confluent_environment.rails_environment.id
    }
  }

}


locals {
  schema_api_key = confluent_api_key.bootcamp-schema-registry-api-key.id
  schema_secret = confluent_api_key.bootcamp-schema-registry-api-key.secret
}

resource "local_file" "schema-api-key" {
  filename = "${path.module}/schema-apikey.json"
  content = "{\n\t\"api_key\": \"${local.schema_api_key}\",\n\t\"secret\": \"${local.schema_secret}\"\n}"
  file_permission = "0600"
}
