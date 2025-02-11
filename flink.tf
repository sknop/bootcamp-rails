resource "confluent_flink_compute_pool" "main" {
  display_name     = var.flink_pool_name
  cloud            = "AWS"
  region           = var.confluent_region
  max_cfu          = 20
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
}

data "confluent_flink_region" "rails_pool_region" {
  cloud = confluent_flink_compute_pool.main.cloud
  region = confluent_flink_compute_pool.main.region
}

resource "confluent_service_account" "app-flink" {
  display_name = "app-flink"
  description  = "Service account to manage Flink compute statements"
}

resource "confluent_role_binding" "app-flink" {
  principal   = "User:${confluent_service_account.app-flink.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.stream_bootcamp.resource_name
}

# TODO
# This is a hack to get things working, need to narrow permissions down to bare minimum later

resource "confluent_role_binding" "app-flink-kafka-cluster-admin" {
  principal   = "User:${confluent_service_account.app-flink.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.bootcamp.rbac_crn
}

resource "confluent_api_key" "flink-api-key" {
  display_name = "env-manager-flink-api-key"
  description = "Flink API Key owned by 'app-flink' service account"
  owner {
    id = confluent_service_account.app-flink.id
    api_version = confluent_service_account.app-flink.api_version
    kind = confluent_service_account.app-flink.kind
  }

  managed_resource {
    api_version = data.confluent_flink_region.rails_pool_region.api_version
    id          = data.confluent_flink_region.rails_pool_region.id
    kind        = data.confluent_flink_region.rails_pool_region.kind

    environment {
      id = confluent_environment.stream_bootcamp.id
    }
  }

  depends_on = [
    confluent_role_binding.app-flink
  ]
}

locals {
  flink_api_key = confluent_api_key.flink-api-key.id
  flink_api_secret = confluent_api_key.flink-api-key.secret
}

data "confluent_organization" "bootcamp" {
}

resource "confluent_flink_statement" "flink_locations" {
  statement = file("flink/01_locations.sql")

  properties = {
    "sql.current-catalog"  = confluent_environment.stream_bootcamp.display_name
    "sql.current-database" = confluent_kafka_cluster.bootcamp.display_name
  }

  rest_endpoint = data.confluent_flink_region.rails_pool_region.rest_endpoint

  organization {
    id = data.confluent_organization.bootcamp.id
  }
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app-flink.id
  }
  credentials {
    key    = confluent_api_key.flink-api-key.id
    secret = confluent_api_key.flink-api-key.secret
  }

  depends_on = [
    confluent_connector.NETWORKRAIL_CIF_TOTAL,
    confluent_connector.NETWORKRAIL_TRAIN_MVT_ALL_TOC,
    confluent_kafka_topic.LOCATIONS_RAW,
    confluent_kafka_topic.CANX_REASON_CODE,
    confluent_kafka_topic.CIF_FULL_DAILY,
    confluent_kafka_topic.NETWORKRAIL_TRAIN_MVT,
    confluent_kafka_topic.TOC_CODES,
    confluent_role_binding.app-flink-kafka-cluster-admin
  ]
}

resource "confluent_flink_statement" "flink_locations_by_stanox" {
  statement = file("flink/01A_locations_by_stanox.sql")

  properties = {
    "sql.current-catalog"  = confluent_environment.stream_bootcamp.display_name
    "sql.current-database" = confluent_kafka_cluster.bootcamp.display_name
  }

  rest_endpoint = data.confluent_flink_region.rails_pool_region.rest_endpoint

  organization {
    id = data.confluent_organization.bootcamp.id
  }
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app-flink.id
  }
  credentials {
    key    = confluent_api_key.flink-api-key.id
    secret = confluent_api_key.flink-api-key.secret
  }

  depends_on = [
    confluent_flink_statement.flink_locations
  ]
}

resource "confluent_flink_statement" "flink_schedule" {
  statement = file("flink/02_schedule.sql")

  properties = {
    "sql.current-catalog"  = confluent_environment.stream_bootcamp.display_name
    "sql.current-database" = confluent_kafka_cluster.bootcamp.display_name
  }

  rest_endpoint = data.confluent_flink_region.rails_pool_region.rest_endpoint

  organization {
    id = data.confluent_organization.bootcamp.id
  }
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app-flink.id
  }
  credentials {
    key    = confluent_api_key.flink-api-key.id
    secret = confluent_api_key.flink-api-key.secret
  }

  depends_on = [
    confluent_flink_statement.flink_locations_by_stanox
  ]
}

resource "confluent_flink_statement" "flink_tiploc_code" {
  statement = file("flink/02A_tiploc_code.sql")

  properties = {
    "sql.current-catalog"  = confluent_environment.stream_bootcamp.display_name
    "sql.current-database" = confluent_kafka_cluster.bootcamp.display_name
  }

  rest_endpoint = data.confluent_flink_region.rails_pool_region.rest_endpoint

  organization {
    id = data.confluent_organization.bootcamp.id
  }
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app-flink.id
  }
  credentials {
    key    = confluent_api_key.flink-api-key.id
    secret = confluent_api_key.flink-api-key.secret
  }

  depends_on = [
    confluent_connector.NETWORKRAIL_CIF_TOTAL,
    confluent_kafka_topic.CIF_FULL_DAILY,
  ]
}

resource "confluent_flink_statement" "flink_activations" {
  statement = file("flink/03_activations.sql")

  properties = {
    "sql.current-catalog"  = confluent_environment.stream_bootcamp.display_name
    "sql.current-database" = confluent_kafka_cluster.bootcamp.display_name
  }

  rest_endpoint = data.confluent_flink_region.rails_pool_region.rest_endpoint

  organization {
    id = data.confluent_organization.bootcamp.id
  }
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app-flink.id
  }
  credentials {
    key    = confluent_api_key.flink-api-key.id
    secret = confluent_api_key.flink-api-key.secret
  }

  depends_on = [
    confluent_flink_statement.flink_schedule
  ]
}


resource "confluent_flink_statement" "flink_movements" {
  statement = file("flink/04_movements.sql")

  properties = {
    "sql.current-catalog"  = confluent_environment.stream_bootcamp.display_name
    "sql.current-database" = confluent_kafka_cluster.bootcamp.display_name
  }

  rest_endpoint = data.confluent_flink_region.rails_pool_region.rest_endpoint

  organization {
    id = data.confluent_organization.bootcamp.id
  }
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app-flink.id
  }
  credentials {
    key    = confluent_api_key.flink-api-key.id
    secret = confluent_api_key.flink-api-key.secret
  }

  depends_on = [
    confluent_flink_statement.flink_activations,
    confluent_flink_statement.flink_locations_by_stanox
  ]
}

resource "confluent_flink_statement" "flink_cancellations" {
  statement = file("flink/05_cancellations.sql")

  properties = {
    "sql.current-catalog"  = confluent_environment.stream_bootcamp.display_name
    "sql.current-database" = confluent_kafka_cluster.bootcamp.display_name
  }

  rest_endpoint = data.confluent_flink_region.rails_pool_region.rest_endpoint

  organization {
    id = data.confluent_organization.bootcamp.id
  }
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app-flink.id
  }
  credentials {
    key    = confluent_api_key.flink-api-key.id
    secret = confluent_api_key.flink-api-key.secret
  }

  depends_on = [
    confluent_flink_statement.flink_activations,
    confluent_flink_statement.flink_locations_by_stanox
  ]
}

resource "confluent_flink_statement" "flink_reinstatements" {
  statement = file("flink/06_reinstatements.sql")

  properties = {
    "sql.current-catalog"  = confluent_environment.stream_bootcamp.display_name
    "sql.current-database" = confluent_kafka_cluster.bootcamp.display_name
  }

  rest_endpoint = data.confluent_flink_region.rails_pool_region.rest_endpoint

  organization {
    id = data.confluent_organization.bootcamp.id
  }
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app-flink.id
  }
  credentials {
    key    = confluent_api_key.flink-api-key.id
    secret = confluent_api_key.flink-api-key.secret
  }

  depends_on = [
    confluent_flink_statement.flink_activations,
    confluent_flink_statement.flink_locations_by_stanox
  ]
}

resource "confluent_flink_statement" "flink_train_describers" {
  statement = file("flink/07_train_describers.sql")

  properties = {
    "sql.current-catalog"  = confluent_environment.stream_bootcamp.display_name
    "sql.current-database" = confluent_kafka_cluster.bootcamp.display_name
  }

  rest_endpoint = data.confluent_flink_region.rails_pool_region.rest_endpoint

  organization {
    id = data.confluent_organization.bootcamp.id
  }
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app-flink.id
  }
  credentials {
    key    = confluent_api_key.flink-api-key.id
    secret = confluent_api_key.flink-api-key.secret
  }
  depends_on = [
    confluent_connector.TD_ALL_SIG_AREA,
    confluent_kafka_topic.TD_ALL_SIG_AREA
  ]
}