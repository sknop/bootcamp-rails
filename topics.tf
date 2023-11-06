
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
