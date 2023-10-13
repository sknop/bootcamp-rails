
resource "confluent_service_account" "app-connector" {
  display_name = "app-connector"
  description  = "Service account of ActiveMQ connector writing to 'NETWORKRAIL_TRAIN_MVT' topic of 'bootcamp' Kafka cluster"
}

resource "confluent_kafka_acl" "app-connector-describe-on-cluster" {
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  resource_type = "CLUSTER"
  resource_name = "kafka-cluster"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-connector.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.bootcamp.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-connector-write-on-target-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.NETWORKRAIL_TRAIN_MVT.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-connector.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.bootcamp.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}


resource "confluent_connector" "NETWORKRAIL_TRAIN_MVT_ALL_TOC" {
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  config_sensitive = {
    "activemq.username" = var.nrod_username
    "activemq.password" = var.nrod_password
  }
  config_nonsensitive = {
    "connector.class" = "ActiveMQSource"
    "name" = "NETWORKRAIL_TRAIN_MVT_ALL_TOC"
    "kafka.auth.mode" = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app-connector.id
    "activemq.url" = "tcp://datafeeds.networkrail.co.uk:61619"
    "jms.destination.type" = "topic"
    "jms.destination.name" = "TRAIN_MVT_ALL_TOC"
    "output.data.format" = "JSON"
    "kafka.topic" = confluent_kafka_topic.NETWORKRAIL_TRAIN_MVT.topic_name
    "transforms" = "extractText"
    "transforms.extractText.type": "org.apache.kafka.connect.transforms.ExtractField$Value"
    "transforms.extractText.field": "text"
    "tasks.max" = "1"
  }
  depends_on = [
    confluent_kafka_acl.app-connector-describe-on-cluster,
    confluent_kafka_acl.app-connector-write-on-target-topic,
  ]
}
