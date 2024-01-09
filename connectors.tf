
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

resource "confluent_kafka_acl" "app-connector-write-on-NETWORKRAIL_TRAIN_MVT-topic" {
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

resource "confluent_kafka_acl" "app-connector-write-on-TD_ALL_SIG_AREA-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.TD_ALL_SIG_AREA.topic_name
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
    "activemq.password" = var.nrod_password
  }
  config_nonsensitive = {
    "connector.class" = "ActiveMQSource"
    "activemq.username" = var.nrod_username
    "name" = "NETWORKRAIL_TRAIN_MVT_ALL_TOC"
    "kafka.auth.mode" = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app-connector.id
    "activemq.url" = "tcp://datafeeds.networkrail.co.uk:61619"
    "jms.destination.type" = "topic"
    "jms.destination.name" = "TRAIN_MVT_ALL_TOC"
    "output.data.format" = "AVRO"
    "kafka.topic" = confluent_kafka_topic.NETWORKRAIL_TRAIN_MVT.topic_name
    "tasks.max" = "1"
  }
  depends_on = [
    confluent_kafka_acl.app-connector-describe-on-cluster,
    confluent_kafka_acl.app-connector-write-on-NETWORKRAIL_TRAIN_MVT-topic
  ]
}

resource "confluent_connector" "TD_ALL_SIG_AREA" {
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  config_sensitive = {
    "activemq.password" = var.nrod_password
  }
  config_nonsensitive = {
    "connector.class" = "ActiveMQSource"
    "activemq.username" = var.nrod_username
    "name" = "TD_ALL_SIG_AREA"
    "kafka.auth.mode" = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app-connector.id
    "activemq.url" = "tcp://datafeeds.networkrail.co.uk:61619"
    "jms.destination.type" = "topic"
    "jms.destination.name" = "TD_ALL_SIG_AREA"
    "output.data.format" = "AVRO"
    "kafka.topic" = confluent_kafka_topic.TD_ALL_SIG_AREA.topic_name
    "tasks.max" = "1"
  }
  depends_on = [
    confluent_kafka_acl.app-connector-describe-on-cluster,
    confluent_kafka_acl.app-connector-write-on-TD_ALL_SIG_AREA-topic,
  ]
}

resource "confluent_connector" "NETWORKRAIL_CIF_TOTAL" {
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  config_sensitive = {
    "http.user" = var.nrod_username
    "http.password" = var.nrod_password
  }
  config_nonsensitive = {
    "name" = "NETWORKRAIL_CIF_TOTAL"
    "connector.class" = "io.confluent.bootcamp.connect.http.HttpCompressedSourceConnector"
    "kafka.auth.mode" = "KAFKA_API_KEY"
    "kafka.api.key" = confluent_api_key.app-manager-kafka-api-key.id
    "kafka.api.secret" = confluent_api_key.app-manager-kafka-api-key.secret
    "kafka.service.account.id" = confluent_service_account.app-manager.id # Does not support Granular API Key access yet
    "tasks.max" = "1"
    "confluent.custom.plugin.id" = confluent_custom_connector_plugin.http-compressed-source.id
    "confluent.connector.type" = "CUSTOM"
    "confluent.custom.connection.endpoints" = var.confluent_custom_connection_endpoints
    "value.converter" = "org.apache.kafka.connect.storage.StringConverter"
    "http.url" = var.cif_total_http_url
    "topic" = confluent_kafka_topic.CIF_FULL_DAILY.topic_name
    "page.size.lines" = "5000"
    "task.pause.ms" = "300000" # 5 min
  }
  depends_on = [
    confluent_service_account.app-manager
  ]
}
