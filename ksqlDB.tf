
resource "confluent_service_account" "app-ksql" {
  display_name = "app-ksql"
  description  = "Service account to manage 'example' ksqlDB cluster"
}

resource "confluent_role_binding" "app-ksql-kafka-cluster-admin" {
  principal   = "User:${confluent_service_account.app-ksql.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.bootcamp.rbac_crn
}

resource "confluent_role_binding" "app-ksql-schema-registry-resource-owner" {
  principal   = "User:${confluent_service_account.app-ksql.id}"
  role_name   = "ResourceOwner"
  crn_pattern = format("%s/%s", data.confluent_schema_registry_cluster.essentials.resource_name, "subject=*")
}

resource "confluent_ksql_cluster" "bootcamp" {
  display_name = "bootcamp"
  csu          = 2
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  credential_identity {
    id = confluent_service_account.app-ksql.id
  }
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  depends_on = [
    confluent_role_binding.app-ksql-kafka-cluster-admin,
    confluent_role_binding.app-ksql-schema-registry-resource-owner,
    data.confluent_schema_registry_cluster.essentials
  ]
}

resource "confluent_api_key" "ksqldb-api-key" {
  display_name = "ksqldb-api-key"
  description  = "KsqlDB API Key that is owned by 'app-manager' service account"
  owner {
    id = confluent_service_account.app-ksql.id
    api_version = confluent_service_account.app-ksql.api_version
    kind        = confluent_service_account.app-ksql.kind
  }

  managed_resource {
    api_version = confluent_ksql_cluster.bootcamp.api_version
    id          = confluent_ksql_cluster.bootcamp.id
    kind        = confluent_ksql_cluster.bootcamp.kind

    environment {
      id = confluent_environment.stream_bootcamp.id
    }
  }
}


resource "local_file" "app-ksql-api-key" {
  filename = "${path.module}/app-ksql-apikey.json"
  content = "{\n\t\"api_key\": \"${confluent_api_key.ksqldb-api-key.id}\",\n\t\"secret\": \"${confluent_api_key.ksqldb-api-key.secret}\"\n}"
  file_permission = "0600"
}

# No need for trimming, will do this in the application. ksql CLI command needs the full URL
locals {
  # ksql_endpoint = trimsuffix(trimprefix(confluent_ksql_cluster.bootcamp.rest_endpoint, "https://"), ":443")
  ksql_endpoint = confluent_ksql_cluster.bootcamp.rest_endpoint
}

resource "local_file" "ksql-property-file" {
  filename = "${path.module}/ksql.properties"
  content = "api.key = ${confluent_api_key.ksqldb-api-key.id}\napi.secret = ${confluent_api_key.ksqldb-api-key.secret}\nksqldb.endpoint = ${local.ksql_endpoint}\n"
  file_permission = "0600"
}
