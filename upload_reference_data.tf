variable "ukrail_locations" {
  default = "data/ukrail_locations.csv"
}

variable "toc_codes" {
  default = "data/toc_codes.csv"
}

# variable "canx_reasons" {
#   default = "data/canx_reason_code.dat"
# }

variable "cancellation_reasons" {
  default = "data/canx_reason_code.csv"
}

locals {
  upload-jar-file = "lib/CSVFileAvroUploader-1.1.4.jar"
}

locals {
  locations-output-file = "locations_output.json"
}

resource "null_resource" "ukrail_locations_upload" {
  provisioner "local-exec" {
    command = <<EOT
      java \
        -jar ${local.upload-jar-file} \
        -c ${var.ccloud-properties} \
        -f ${var.ukrail_locations} \
        --topic ${confluent_kafka_topic.LOCATIONS_RAW.topic_name} \
        --key-field location_id \
        -s LocationRaw \
        --output-file ${local.locations-output-file} \
        -n io.confluent.bootcamp.rails.schema
    EOT
  }

  depends_on = [
    confluent_kafka_topic.LOCATIONS_RAW
  ]
}

data "local_file" "locations-offset" {
  filename = local.locations-output-file
  depends_on = [ null_resource.ukrail_locations_upload ]
}

locals {
  locations_partitions = join(";", [ for part,offset in jsondecode(data.local_file.locations-offset.content).offset : "partition:${part},offset:${offset}" ])
}

locals {
  cancellation-reason-output-file = "canx_reason_output.json"
}

resource "null_resource" "cancellation_reason_code_upload" {
  provisioner "local-exec" {
    command = <<EOT
      java \
        -jar ${local.upload-jar-file} \
        -c ${var.ccloud-properties} \
        -f ${var.cancellation_reasons} \
        --topic ${confluent_kafka_topic.CANX_REASON_CODE.topic_name} \
        --key-field canx_reason_code \
        -s CancellationReasonCode  \
        -n io.confluent.bootcamp.rails.schema \
        --output-file ${local.cancellation-reason-output-file} \
        --separator '|'
    EOT
  }

  depends_on = [
    confluent_kafka_topic.CANX_REASON_CODE
  ]
}

data "local_file" "cancellations-offset" {
  filename = local.cancellation-reason-output-file
  depends_on = [ null_resource.cancellation_reason_code_upload]
}

locals {
  cancellations_partitions = join(";", [ for part,offset in jsondecode(data.local_file.cancellations-offset.content).offset : "partition:${part},offset:${offset}" ])
}

locals {
  toc-code-output-file = "toc_code_output.json"
}

resource "null_resource" "toc_upload" {
  provisioner "local-exec" {
    command = <<EOT
      java \
        -jar ${local.upload-jar-file} \
        -c ${var.ccloud-properties} \
        -f ${var.toc_codes} \
        --topic ${confluent_kafka_topic.TOC_CODES.topic_name} \
        --key-field toc_id \
        -s TocCode \
        --output-file ${local.toc-code-output-file} \
        -n io.confluent.bootcamp.rails.schema
    EOT
  }

  depends_on = [
    confluent_kafka_topic.TOC_CODES
  ]
}