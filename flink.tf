resource "confluent_flink_compute_pool" "main" {
  display_name     = "rails_pool"
  cloud            = "AWS"
  region           = var.confluent_region
  max_cfu          = 10
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
}
data "confluent_organization" "bootcamp" {
}

resource "confluent_flink_statement" "flink_locations" {
  statement = <<SQL
    CREATE TABLE FLINK_LOCATIONS (
  	  `tiploc` STRING,
	  `name` STRING,
	  `description` STRING,
	  `location_id` STRING,
	  `crs` STRING,
	  `nlc` STRING ,
	  `stanox` STRING,
	  `notes` STRING,
	  PRIMARY KEY (`tiploc`) NOT ENFORCED)
    WITH (
	  'changelog.mode' = 'upsert',
	  'kafka.cleanup-policy' = 'compact',
	  'kafka.retention.time' = '0'
    )
    AS
      SELECT `tiploc`, `name`, `description`, `location_id`, `crs`, `nlc`, LPAD(`stanox`,5,'00000'), `notes`
      FROM LOCATIONS_RAW
      WHERE `tiploc` <> '';
  SQL
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
}
