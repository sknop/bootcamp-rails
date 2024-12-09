resource "confluent_flink_compute_pool" "main" {
  display_name     = "rails_pool"
  cloud            = "AWS"
  region           = var.confluent_region
  max_cfu          = 10
  environment {
    id = confluent_environment.stream_bootcamp.id
  }
}
