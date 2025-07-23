resource "confluent_service_account" "app-tableflow" {
  display_name = "app-tableflow"
  description  = "Service account to manage Tableflow"
}


resource "confluent_role_binding" "app-tableflow-schema" {
  principal   = "User:${confluent_service_account.app-tableflow.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.essentials.resource_name}/subject=*"
}

resource "confluent_role_binding" "app-tableflow-kafka-cluster-admin" {
  principal   = "User:${confluent_service_account.app-tableflow.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.bootcamp.rbac_crn
}

resource "confluent_role_binding" "app-tableflow-integration-provider" {
  principal   = "User:${confluent_service_account.app-tableflow.id}"
  role_name   = "Assigner"
  crn_pattern = confluent_environment.rails_environment.resource_name
}

resource "confluent_api_key" "app-tableflow-api-key" {
  display_name = "app-tableflow-api-key"
  description  = "Tableflow API Key that is owned by 'app-tableflow' service account"
  owner {
    id          = confluent_service_account.app-tableflow.id
    api_version = confluent_service_account.app-tableflow.api_version
    kind        = confluent_service_account.app-tableflow.kind
  }

  managed_resource {
    id          = "tableflow"
    api_version = "tableflow/v1"
    kind        = "Tableflow"

    environment {
      id = confluent_environment.rails_environment.id
    }
  }
}

resource "confluent_tableflow_topic" "movement" {
  environment {
    id = confluent_environment.rails_environment.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  display_name = confluent_flink_statement.flink_movements.statement_name
  table_formats = ["ICEBERG"]

  managed_storage {}

  credentials {
    key = confluent_api_key.app-tableflow-api-key.id
    secret = confluent_api_key.app-tableflow-api-key.secret
  }

  # The goal is to ensure that confluent_flink_statement.flink_movements is created before
  # an instance of confluent_tableflow_topic is created since it requires
  # a topic with a schema.
  depends_on = [
    confluent_flink_statement.flink_movements
  ]
}

resource "confluent_tableflow_topic" "cancellation" {
  environment {
    id = confluent_environment.rails_environment.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  display_name = confluent_flink_statement.flink_cancellations.statement_name
  table_formats = ["ICEBERG"]

  managed_storage {}

  credentials {
    key = confluent_api_key.app-tableflow-api-key.id
    secret = confluent_api_key.app-tableflow-api-key.secret
  }

  # The goal is to ensure that confluent_flink_statement.flink_movements is created before
  # an instance of confluent_tableflow_topic is created since it requires
  # a topic with a schema.
  depends_on = [
    confluent_flink_statement.flink_cancellations
  ]
}

resource "confluent_tableflow_topic" "reinstatement" {
  environment {
    id = confluent_environment.rails_environment.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  display_name = confluent_flink_statement.flink_reinstatements.statement_name
  table_formats = ["ICEBERG"]

  managed_storage {}

  credentials {
    key = confluent_api_key.app-tableflow-api-key.id
    secret = confluent_api_key.app-tableflow-api-key.secret
  }

  # The goal is to ensure that confluent_flink_statement.flink_movements is created before
  # an instance of confluent_tableflow_topic is created since it requires
  # a topic with a schema.
  depends_on = [
    confluent_flink_statement.flink_reinstatements
  ]
}

resource "confluent_tableflow_topic" "train_describers" {
  environment {
    id = confluent_environment.rails_environment.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.bootcamp.id
  }
  display_name = confluent_flink_statement.flink_train_describers.statement_name
  table_formats = ["ICEBERG"]

  managed_storage {}

  credentials {
    key = confluent_api_key.app-tableflow-api-key.id
    secret = confluent_api_key.app-tableflow-api-key.secret
  }

  # The goal is to ensure that confluent_flink_statement.flink_movements is created before
  # an instance of confluent_tableflow_topic is created since it requires
  # a topic with a schema.
  depends_on = [
    confluent_flink_statement.flink_train_describers
  ]
}
