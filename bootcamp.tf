# Configure the Confluent Cloud Provider
terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "1.51.0"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_api_key
  cloud_api_secret = var.confluent_api_secret
}

resource "confluent_environment" "stream_bootcamp" {
  display_name = var.confluent_environment
}

locals {
  env_id = confluent_environment.stream_bootcamp.id
}
