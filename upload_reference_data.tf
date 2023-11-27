variable "ukrail_locations" {
  default = "data/ukrail_locations.csv"
}

variable "canx_reasons" {
  default = "data/canx_reason_code.dat"
}

resource "null_resource" "ukrail_locations_upload" {
  provisioner "local-exec" {
    command = "java -cp lib/CSVFileAvroUploader-1.0.0.jar io.confluent.bootcamp.CSVFileAvroUploader -c ${var.ccloud-properties} -f ${var.ukrail_locations} --topic ${confluent_kafka_topic.LOCATIONS.topic_name} --key-field location_id"
  }

  depends_on = [
    confluent_kafka_topic.LOCATIONS
  ]
}

resource "null_resource" "canx_reason_code_upload" {
  provisioner "local-exec" {
    command = "kcat -F ${var.kcat-properties} -P -t ${confluent_kafka_topic.CANX_REASON_CODE.topic_name} -K: -l ${var.canx_reasons}"
  }

  depends_on = [
    confluent_kafka_topic.CANX_REASON_CODE
  ]
}
