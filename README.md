# bootcamp-rails
Automation for a streaming bootcamp based on an external continuous source of train movements in the UK

This is a terraform script that will

- Create an environment in the Confluent Cloud
- Create a (currently Essentials) schema registry
- Create a standard cluster
- Add service users and API Keys
- Add topics
- Upload a connector plugin for downloading, decompressing and uploading a file with reference data regularly (usually once a day)
- Upload additional reference data (referenced from the `data` directory)
  - Cancellation reasons
  - TOC codes (Train Operating Companies)
  - UK Rail locations
- Configure two connectors
  - HttpCompressedSource for the locations and schedule data
  - ActiveMQSource for the train movement updates
- Create a KSQL cluster

### Preparation

Add a `terraform.tfvars` file with target locations and credentials. 
I tend to add a file called .envrc (using direnv) that contains my actual credentials (such as API Key and Secret) so that I can use these from the command line as well.

You will need to define the following variables in your shell or the equivalent in your `terraform.tfvars` file before running :

    export TF_VAR_confluent_api_key="XXXX"
    export TF_VAR_confluent_api_secret="XXXX"
    export TF_VAR_nrod_username="XXXX"
    export TF_VAR_nrod_password="XXXX"

This requires getting an account at https://publicdatafeeds.networkrail.co.uk/ if you do not have one yet.

### More details to follow (TODO)

- Finish uploading of KSQL statements
- Add student user management (take from bootcamp-streams)
- Labs
