# Configure the Confluent Cloud Provider
terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.34.0"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_api_key
  cloud_api_secret = var.confluent_api_secret
}

resource "confluent_environment" "rails_environment" {
  display_name = var.confluent_environment

  stream_governance {
    package = "ESSENTIALS"
  }
}

data "confluent_organization" "bootcamp" {
}
