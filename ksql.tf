locals {
  ksql-jar-file = "lib/KSQLJavaClient-1.0.2.jar"
  ksql-statements-dir = "ksql"
}

resource "null_resource" "ksql-stream-creation" {
  provisioner "local-exec" {
    command = "java -jar ${local.ksql-jar-file} -c ${local_file.ksql-property-file.filename} -d ${local.ksql-statements-dir}"
  }

  depends_on = [
    confluent_ksql_cluster.bootcamp,
    local_file.ksql-property-file,
    confluent_connector.NETWORKRAIL_CIF_TOTAL,
    confluent_connector.NETWORKRAIL_TRAIN_MVT_ALL_TOC,
    confluent_connector.TD_ALL_SIG_AREA,
    confluent_kafka_topic.LOCATIONS_RAW,
    confluent_kafka_topic.CANX_REASON_CODE,
    confluent_kafka_topic.CIF_FULL_DAILY,
    confluent_kafka_topic.NETWORKRAIL_TRAIN_MVT,
    confluent_kafka_topic.TD_ALL_SIG_AREA,
    confluent_kafka_topic.TOC_CODES
  ]
}
