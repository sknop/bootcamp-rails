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
- Create a Flink compute pool
- Run the Flink statements required to pre-process all events for consumption

### Preparation

Add a `terraform.tfvars` file with target locations and credentials. 
I tend to add a file called .envrc (using direnv) that contains my actual credentials (such as API Key and Secret) so that I can use these from the command line as well.

You will need to define the following variables in your shell or the equivalent in your `terraform.tfvars` file before running :

    export TF_VAR_confluent_api_key="XXXX"
    export TF_VAR_confluent_api_secret="XXXX"
    export TF_VAR_nrod_username="XXXX"
    export TF_VAR_nrod_password="XXXX"

This requires getting an account at https://publicdatafeeds.networkrail.co.uk/ if you do not have one yet.

### Hints:

Regular expression to convert CANX code:

Find: ([A-Z0-9][A-Z0-9]):{"canx_reason":"(.*)","canx_abbrev":"(.*)"}

Replace: \1|\2|\3

### Possible queries

select FORMAT_TIMESTAMP(FROM_UNIXTIME(actual_timestamp) , 'yyyy-MM-dd HH:mm:ss') TIMESTAMP, event_type,  MVT_DESCRIPTION ,  PLATFORM, VARIATION_STATUS, TOC from train_movements;

### More details to follow (TODO)

- Add student user management (take from bootcamp-streams)
- Labs
