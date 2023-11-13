variable "ukrail_loccations" {
  default = "ukrail_locations.csv"
}

resource "null_resource" "ukrail_locations_upload" {
  provisioner "local-exec" {
    command = "java -cp lib/CSVFileAvroUploader-1.0.0.jar io.confluent.bootcamp.CSVFileAvroUploader -c ${var.ccloud-properties} -f ${var.ukrail_loccations} --topic ${confluent_kafka_topic.LOCATIONS.topic_name} --key-field location_id"
  }

  depends_on = [
    confluent_kafka_topic.LOCATIONS
  ]
}
