# bootcamp-rails
Automation for a streaming bootcamp based on an external continuous source of train movements in the UK

This is mainly a terraform script that will

- Create an environment in the Confluent Cloud
- Create a (currently Essentials) schema registry
- Create a standard cluster
- Add service users and API Keys
- Add topics
- Upload a connector plugin for downloading, decompressing and uploading a file with reference data regularly (usually once a day)
- Configure two connectors
  - HttpCompressedSource for the locations and schedule data
  - ActiveMQSource for the train movement updates

Add a terraform.tfvars file with target locations and credentials. 
I tend to add a file called .envrc (using direnv) that contains my actual credentials (such as API Key and Secret) so that I can use these from the command line as well.

More details to follow (TODO)
