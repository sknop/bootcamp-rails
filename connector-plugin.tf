resource "confluent_custom_connector_plugin" "http-compressed-source" {
  display_name                = "Http Compressed Source"
  connector_class             = "io.confluent.bootcamp.connect.http.HttpCompressedSourceConnector"
  connector_type              = "SOURCE"
  sensitive_config_properties = [ "http.user", "http.password" ]
  filename                    = "HttpCompressedSourceConnector-1.0.0.jar"
}
