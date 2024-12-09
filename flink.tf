resource "confluent_flink_compute_pool" "main" {
  display_name     = "rails_pool"
  cloud            = "AWS"
  region           = var.confluent_region
  max_cfu          = 10
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
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
      SELECT `tiploc`, `name`, `description`, `location_id`, `crs`, `nlc`, `stanox`, `notes`
      FROM LOCATIONS_RAW
      WHERE `tiploc` <> '';
  SQL
  properties = {
    "sql.current-catalog" = confluent_environment.stream_bootcamp.display_name
    "sql.current-database" = confluent_kafka_cluster.bootcamp.display_name
  }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
}
