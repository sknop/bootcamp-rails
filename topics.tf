
resource "confluent_kafka_topic" "NETWORKRAIL_TRAIN_MVT" {
  kafka_cluster  {
    id = confluent_kafka_cluster.bootcamp-cluster.id
  }

  topic_name       = "NETWORKRAIL_TRAIN_MVT"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.bootcamp-cluster.rest_endpoint
  config = {
  }
  credentials {
    key    = local.api_key
    secret = local.secret
  }
}

resource "confluent_kafka_topic" "CIF_FULL_DAILY" {
  kafka_cluster  {
    id = confluent_kafka_cluster.bootcamp-cluster.id
  }

  topic_name       = "CIF_FULL_DAILY"
  partitions_count = 1
  rest_endpoint    = confluent_kafka_cluster.bootcamp-cluster.rest_endpoint
  config = {
  }
  credentials {
    key    = local.api_key
    secret = local.secret
  }
}
