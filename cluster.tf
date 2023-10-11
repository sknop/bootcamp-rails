
resource "confluent_kafka_cluster" "bootcamp-cluster" {
  display_name = "bootcamp-cluster"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.confluent_region
  standard {}

  environment {
    id = local.env_id
  }
}

resource "confluent_service_account" "bootcamp-env-manager" {
  display_name = "bootcamp-env-manager"
  description  = "Service account to manage resources under 'stream_bootcamp' environment on Confluent Cloud"
}

resource "confluent_role_binding" "app-manager-env-admin" {
  principal   = "User:${confluent_service_account.bootcamp-env-manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.stream_bootcamp.resource_name
}

resource "confluent_role_binding" "app-manager-cluster-admin" {
  principal   = "User:${confluent_service_account.bootcamp-env-manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern =  confluent_kafka_cluster.bootcamp-cluster.rbac_crn
}

resource "confluent_api_key" "env-manager-cluster-api-key" {
  display_name = "env-manager-cluster-api-key"
  description  = "Cloud API Key that is owned by 'env-manager' service account"
  owner {
    id          = confluent_service_account.bootcamp-env-manager.id
    api_version = confluent_service_account.bootcamp-env-manager.api_version
    kind        = confluent_service_account.bootcamp-env-manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.bootcamp-cluster.id
    api_version = confluent_kafka_cluster.bootcamp-cluster.api_version
    kind        = confluent_kafka_cluster.bootcamp-cluster.kind

    environment {
      id = confluent_environment.stream_bootcamp.id
    }
  }

  # The goal is to ensure that confluent_role_binding.app-manager-env-admin is created before
  # confluent_api_key.env-manager-cloud-api-key is used.

  depends_on = [
    confluent_role_binding.app-manager-env-admin,
    confluent_role_binding.app-manager-cluster-admin
  ]

}

locals {
  api_key = confluent_api_key.env-manager-cluster-api-key.id
  secret = confluent_api_key.env-manager-cluster-api-key.secret
}

resource "local_file" "api-key" {
  filename = "${path.module}/apikey.json"
  content = "{\n\t\"api_key\": \"${local.api_key}\",\n\t\"secret\": \"${local.secret}\"\n}"
  file_permission = "0664"
}
