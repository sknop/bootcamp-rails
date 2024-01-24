variable "ukrail_locations" {
  default = "data/ukrail_locations.csv"
}

variable "toc_codes" {
  default = "data/toc_codes.csv"
}

variable "canx_reasons" {
  default = "data/canx_reason_code.dat"
}

variable "cancellation_reasons" {
  default = "data/canx_reason_code.csv"
}

locals {
  upload-jar-file = "lib/CSVFileAvroUploader-1.0.1.jar"
}

resource "null_resource" "ukrail_locations_upload" {
  provisioner "local-exec" {
    command = "java -jar ${local.upload-jar-file} -c ${var.ccloud-properties} -f ${var.ukrail_locations} --topic ${confluent_kafka_topic.LOCATIONS_RAW.topic_name} --key-field location_id -s location"
  }

  depends_on = [
    confluent_kafka_topic.LOCATIONS_RAW
  ]
}

#resource "null_resource" "canx_reason_code_upload" {
#  provisioner "local-exec" {
#    command = "kcat -F ${var.kcat-properties} -P -t ${confluent_kafka_topic.CANX_REASON_CODE.topic_name} -K: -l ${var.canx_reasons}"
#  }
#
#  depends_on = [
#    confluent_kafka_topic.CANX_REASON_CODE
#  ]
#}

resource "null_resource" "cancellation_reason_code_upload" {
  provisioner "local-exec" {
    command = "java -jar ${local.upload-jar-file} -c ${var.ccloud-properties} -f ${var.cancellation_reasons} --topic ${confluent_kafka_topic.CANX_REASON_CODE.topic_name} --key-field canx_reason_code -s cancellation --separator '|'"
  }

  depends_on = [
    confluent_kafka_topic.CANX_REASON_CODE
  ]
}

resource "null_resource" "toc_upload" {
  provisioner "local-exec" {
    command = "java -jar ${local.upload-jar-file} -c ${var.ccloud-properties} -f ${var.toc_codes} --topic ${confluent_kafka_topic.TOC_CODES.topic_name} --key-field toc_id -s toc_codes"
  }

  depends_on = [
    confluent_kafka_topic.TOC_CODES
  ]
}
