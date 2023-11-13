
resource "confluent_kafka_cluster" "bootcamp" {
  display_name = var.confluent_cluster
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.confluent_region
  standard {}

  environment {
    id = confluent_environment.stream_bootcamp.id
  }
}

resource "confluent_service_account" "app-manager" {
  display_name = "app-manager"
  description  = "Service account to manage 'bootcamp' Kafka cluster"
}

resource "confluent_role_binding" "app-manager-kafka-cluster-admin" {
  principal   = "User:${confluent_service_account.app-manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.bootcamp.rbac_crn
}

resource "confluent_api_key" "app-manager-kafka-api-key" {
  display_name = "app-manager-kafka-api-key"
  description  = "Kafka API Key that is owned by 'app-manager' service account"
  owner {
    id          = confluent_service_account.app-manager.id
    api_version = confluent_service_account.app-manager.api_version
    kind        = confluent_service_account.app-manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.bootcamp.id
    api_version = confluent_kafka_cluster.bootcamp.api_version
    kind        = confluent_kafka_cluster.bootcamp.kind

    environment {
      id = confluent_environment.stream_bootcamp.id
    }
  }
  # The goal is to ensure that confluent_role_binding.app-manager-kafka-cluster-admin is created before
  # confluent_api_key.app-manager-kafka-api-key is used to create instances of
  # confluent_kafka_topic, confluent_kafka_acl resources.

  # 'depends_on' meta-argument is specified in confluent_api_key.app-manager-kafka-api-key to avoid having
  # multiple copies of this definition in the configuration which would happen if we specify it in
  # confluent_kafka_topic, confluent_kafka_acl resources instead.
  depends_on = [
    confluent_role_binding.app-manager-kafka-cluster-admin
  ]
}

resource "local_file" "app-manager-api-key" {
  filename = "${path.module}/app-manager-apikey.json"
  content = "{\n\t\"api_key\": \"${confluent_api_key.app-manager-kafka-api-key.id}\",\n\t\"secret\": \"${confluent_api_key.app-manager-kafka-api-key.secret}\"\n}"
  file_permission = "0664"
}

