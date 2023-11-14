resource "confluent_custom_connector_plugin" "http-compressed-source" {
  display_name                = "Http Compressed Source"
  connector_class             = "io.confluent.bootcamp.connect.http.HttpCompressedSourceConnector"
  connector_type              = "SOURCE"
  sensitive_config_properties = [ "http.password","http.user" ]
  filename                    = "lib/HttpCompressedSourceConnector-1.0.3.jar"

  # This is a bit of a hack - there is a bug in the Confluent Terraform provider that causes Terraform to reapply
  # the sensitive_config_properties, causing an error when rerunning "terraform apply"
  # Keep in mind that confluent_custom_connector_plugin is still on early access, a bug has been filed

  lifecycle {
    ignore_changes = [sensitive_config_properties]
  }
}
