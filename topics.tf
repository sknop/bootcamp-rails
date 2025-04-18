
resource "confluent_kafka_topic" "NETWORKRAIL_TRAIN_MVT" {
  kafka_cluster  {
    id = confluent_kafka_cluster.bootcamp.id
  }

  topic_name       = "NETWORKRAIL_TRAIN_MVT"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.bootcamp.rest_endpoint
  config = {
  }
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_topic" "CIF_FULL_DAILY" {
  kafka_cluster  {
    id = confluent_kafka_cluster.bootcamp.id
  }

  topic_name       = "CIF_FULL_DAILY"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.bootcamp.rest_endpoint

  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_topic" "TD_ALL_SIG_AREA" {
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }

  topic_name        = "TD_ALL_SIG_AREA"
  partitions_count  = 1
  rest_endpoint     = confluent_kafka_cluster.bootcamp.rest_endpoint

  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_topic" "LOCATIONS_RAW" {
  kafka_cluster  {
    id = confluent_kafka_cluster.bootcamp.id
  }

  topic_name       = "LOCATIONS_RAW"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.bootcamp.rest_endpoint

  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }

  config = {
    "cleanup.policy" = "compact"
    "retention.ms" = "-1"
    "retention.bytes" = "-1"
  }
}

resource "confluent_kafka_topic" "CANX_REASON_CODE" {
  kafka_cluster  {
    id = confluent_kafka_cluster.bootcamp.id
  }

  topic_name       = "CANX_REASON_CODE"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.bootcamp.rest_endpoint

  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }

  config = {
    "cleanup.policy" = "compact"
    "retention.ms" = "-1"
    "retention.bytes" = "-1"
  }
}

resource "confluent_kafka_topic" "TOC_CODES" {
  kafka_cluster  {
    id = confluent_kafka_cluster.bootcamp.id
  }

  topic_name       = "TOC_CODES"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.bootcamp.rest_endpoint

  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }

  config = {
    "cleanup.policy" = "compact"
    "retention.ms" = "-1"
    "retention.bytes" = "-1"
  }
}
