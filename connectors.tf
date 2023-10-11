resource "confluent_connector" "NETWORKRAIL_TRAIN_MVT_HY_TOC" {
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp-cluster.id
  }
  config_sensitive = {
    "activemq.username" = var.nrod_username
    "activemq.password" = var.nrod_password
  }
  config_nonsensitive = {
    "connector.class" = "io.confluent.connect.activemq.ActiveMQSourceConnector"
    "name" = "NETWORKRAIL_TRAIN_MVT_HY_TOC"
    "activemq.url" = "tcp://datafeeds.networkrail.co.uk:61619"
    "jms.destination.type" = "topic"
    "jms.destination.name": "TRAIN_MVT_HY_TOC"
    "kafka.topic": confluent_kafka_topic.NETWORKRAIL_TRAIN_MVT.topic_name
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "confluent.topic.bootstrap.servers": "kafka-1:19094",
    "confluent.topic.replication.factor": 3,
    "transforms": "extractText",
    "transforms.extractText.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
    "transforms.extractText.field": "text"
  }
  depends_on = [
    // ACLs dependencies go here
  ]
}