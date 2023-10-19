# Amazon Redshift Node Configuration Comparison utility

### ** Note: This readme walks you through the latest version of this utility which now supports Redshift Serverless to test your workload for performance.If you want to either explore different Redshift Serverless configurations or combination of Redshift Provisioned and Serverless configurations based on your workload, please follow instructions in this readme. If you are still using the previous version which only supports provisioned clusters, please refer to this [readme](https://github.com/aws-samples/amazon-redshift-config-compare/blob/main/README-v1.md) **

mazon Redshift Node Configuration Comparison utility answers a very common question on which instance type and number of nodes should we choose for your workload on Amazon Redshift. You can use this utility to find the right datawarehouse configuration for your workload based on your query performance expectation for sequential or concurrently running queries. If you are already using Amazon Redshift, you may also run your past workloads using [Amazon Redshift Simple Replay utility](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/SimpleReplay) to evaluate performance metrics for different Amazon Redshift configurations to meet your needs. It helps you find the best Amazon Redshift datawarehouse configuration based on your price performance expectation.

## Solution Overview

The solution uses [AWS Step Functions](https://aws.amazon.com/step-functions/), [AWS Lambda](https://aws.amazon.com/lambda/) and [AWS Batch](https://aws.amazon.com/batch/) to run an end-to-end automated orchestration to find the best [Amazon Redshift](https://aws.amazon.com/redshift/) configuration based on your price/performance requirements. [AWS CloudFormation template](https://aws.amazon.com/cloudformation/) is used to deploy and run this solution in your AWS Account. Along with other resources, this template also creates an [Amazon S3](https://aws.amazon.com/s3/) bucket to store all data and metadata related to this process.

![Architecture Diagram](https://github.com/aws-samples/amazon-redshift-config-compare/blob/main/serverless-v2/images/architecure-serverless.png)

You need to create a JSON file to provide your input configurations for your test:

1. Amazon Redshift provisioned clusters and Serverless workgroups configurations
2. DDL and Load Script (Optional)
3. Redshift Snapshot Identifier (Optional)
4. SQL Script to conduct sequential and concurrency test (Optional)
5. Amazon Redshift Audit Log location and simple replay time window (Optional)

You need to store this file in an existing Amazon S3 bucket and then use [this AWS CloudFormation template](https://amazon-redshift-node-config-compare.s3.amazonaws.com/cfn/redshift_node_config_compare_v2.yaml) to deploy this solution, which will also initiate an iteration of this test by invoking an Amazon Step Functions state machine in your AWS account.

## Prerequisites

This solution uses [AWS CloudFormation](https://aws.amazon.com/cloudformation/) to automatically provision all the required resources in your AWS accounts. It uses AWS Lake Formation to manage access on the AWS Glue catalog which stores the performance comparison stats. If you haven't used AWS Lakeformation before , you need to add yourself as Data Lake Administrator, please follow the instructions here on [Setting up AWS Lake Formation](https://docs.aws.amazon.com/lake-formation/latest/dg/getting-started-setup.html#create-data-lake-admin). For more information, see [Getting started with AWS CloudFormation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/GettingStarted.html).

If you are already running Amazon Redshift workload in production, you may like to use this solution to replay your past workload leveraging [Amazon Redshift Simple Replay Utility](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/SimpleReplay). As a prerequisite to use simple replay utility, you need to enable [audit logging](https://docs.aws.amazon.com/redshift/latest/mgmt/db-auditing.html#db-auditing-enable-logging) and [user-activity logging](https://docs.aws.amazon.com/redshift/latest/mgmt/db-auditing.html#db-auditing-user-activity-log) in your Amazon Redshift provisioned clusters and serverless workgroups.

If you are going to replay your workload into Serverless workgroup then make sure that you must have at least three subnets, and they must span across three Availability Zones. You can review the considerations when using Amazon Redshift Serverless [here](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-known-issues.html).

You need to provide at least one subnet in the same VPC (where you have Redshift provisioned clusters or serverless workgroups) which has access to the internet to download the ECR container image.

## Example Use Case

As an example, you may assume you have an existing Amazon Redshift cluster with 2 nodes of DC2.8XLarge instances. You would like to evaluate moving this cluster to RA3.4XLarge instances with four nodes and Redshift Serverless with base RPU 64 and 128. You would replay one hour past workload in these Redshift datawareshouse configurations and workgroups and compare their performance.

For your RA3.4XLarge four node configuration, you would also like to test your workload performance with [concurrency scaling](https://docs.aws.amazon.com/redshift/latest/dg/concurrency-scaling.html) enabled in that provisioned cluster, which could help improve concurrent workloads with consistently fast query performance.

At the end of this test, you would like to compare various metrics like total, average, median and maximum time taken for these five Amazon Redshift datawarehouse configurations:

| **node type** | **number of nodes/Base RPU** | **option** |
| --- | --- | --- |
| dc2.8xlarge | 2 | default auto wlm |
| ra3.4xlarge | 4 | default auto wlm |
| ra3.4xlarge | 4 | concurrency scaling enabled |
| Redshift Serverless | 64 | auto scaling |
| Redshift Serverless | 128 | auto scaling |

To perform this test using [Amazon Redshift node configuration comparison utility](https://github.com/aws-samples/amazon-redshift-config-compare), you would like to provide these configurations in a [JSON file](https://github.com/aws-samples/amazon-redshift-config-compare/blob/main/serverless-v2/user_config.json) and store it in an Amazon S3 bucket. You may then use [AWS CloudFormation Template](https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=redshift-node-config-comparison&templateURL=https://amazon-redshift-node-config-compare.s3.amazonaws.com/cfn/redshift_node_config_compare_v2.yaml) to deploy this utility, which would perform the end-to-end performance testing in all above configurations in parallel and produce a price/performance evaluation summary. Based on that summary, you would be easily deciding which configuration works best for you.

## Input JSON File

You need to provide a configuration JSON file to use this solution. Below are the input parameters for this JSON file.

| **JSON Parameter** | **Valid Values** | **Description** |
| --- | --- | --- |
| SNAPSHOT\_ID | N/A, Redshift Snapshot Identifier | Input Snapshot Identifier, if you would like to create new Amazon Redshift provisioned clusters and Serverless workgroups by restoring from a snapshot. If you are using this solution in a different AWS account, please make sure to share your Amazon Redshift provisioned cluster snapshot with this account. Please read the [documentation](https://aws.amazon.com/premiumsupport/knowledge-center/account-transfer-redshift/) for more. Input N/A if not applicable |
| SNAPSHOT\_ACCOUNT\_ID | N/A,AWS Account ID | AWS Account ID where above snapshot was created. Input N/A if not applicable |
| PARAMETER\_GROUP\_CONFIG\_S3\_PATH | N/A,Amazon S3 URI | If you may use a custom parameter group for this testing, please input its S3 URI. You may get this JSON by running this command in AWS Command Line interface: &quot;aws redshift describe-cluster-parameters --parameter-group-name your-custom-param-group --output json&quot; Input N/A if not applicable |
| DDL\_AND\_COPY\_SCRIPT\_S3\_PATH | N/A,Amazon S3 URI | If you may create tables and load data on them before performing the testing, please input its S3 URI. InputN/A if not applicable |
| SQL\_SCRIPT\_S3\_PATH | N/A,Amazon S3 URI | If you may run performance testing of your queries, input S3 URI of your script consisting of all your SQL commands. These commands should be deliminated by semicolon (;). InputN/A if not applicable |
| NUMBER\_OF\_PARALLEL\_SESSIONS\_LIST | N/A | Input comma separated numbers to denote number of parallel sessions in which you would like to run above script |
| SIMPLE\_REPLAY\_LOG\_LOCATION | N/A,Amazon S3 URI | If you are already running Amazon Redshift workload and your provisioned cluster has audit logging enabled. Please input the S3 URI of your Redshift Audit Logging location. If you are using this solution in a different AWS account, please make sure to copy these logs from your source clusters&#39; audit logging bucket to an Amazon S3 bucket in this account. |
| SIMPLE\_REPLAY\_EXTRACT\_START\_TIME | N/A,Amazon S3 URI | If using simple-replay in this testing to replay your past workload, input the start time of that workload in ISO-8601 format (e.g. 2021-01-26T21:41:16+00:00) |
| SIMPLE\_REPLAY\_EXTRACT\_END\_TIME | N/A, Amazon S3 URI | If using simple-replay in this testing to replay your past workload, input the end time of that workload in ISO-8601 format (e.g. 2021-01-26T21:41:16+00:00) |
| SIMPLE\_REPLAY\_EXTRACT\_OVERWRITE\_S3\_PATH | N/A,Amazon S3 URI | If using simple-replay and you may like to use a custom extract.yaml file, please input its S3 URI |
| SIMPLE\_REPLAY\_OVERWRITE\_S3\_PATH | N/A,Amazon S3 URI | If using simple-replay and you may like to use a custom replay.yaml file, please input its S3 URI |
| AUTO\_PAUSE | true,false | Input true if you would like to automatically pause all Amazon Redshift provisioned clusters after completion of the step function |
| DATABASE\_NAME | N/A,Redshift database name | Specify the primary database name of your Redshift endpoint. If you’re using Simple Replay, provide the database name for which you want to replay the workload. Amazon Redshift automatically creates a default database named dev, which may not be your primary database|
| CONFIGURATIONS | JSON Array with parameters NODE\_TYPE, NUMBER\_OF\_NODES, WLM\_CONFIG\_S3\_PATH | Input a JSON Array mentioning your Amazon Redshift provisioned cluster and/or Serverless workgroups configurations, for which you may like to perform this testing. Below are the parameters for this: |
|  |  |  |
| NODE\_TYPE | ra3.xlplus, ra3.4xlarge, ra3.16xlarge, dc2.large, dc2.8xlarge, ds2.xlarge, ds2.8xlarge | Input Amazon Redshift provisioned cluster Node Type for which, you would like to run this testing. This is applicable only for provisioned cluster. |
| NUMBER\_OF\_NODES | a number between 1 and 128 | Input number of nodes for your Amazon Redshift provisioned cluster. This is applicable only for provisioned cluster. |
| WLM\_CONFIG\_S3\_PATH | N/A,Amazon S3 URI | If you may like to use custom workload management settings if different Amazon Redshift provisioned clusters, please provide the S3 URI for that. This is applicable only for provisioned cluster. |
| TYPE | Provisioned, Serverless  | Input Redshift datawarehouse type for which you would like to replay your workload |
| MAINTENANCE_TRACK | N/A, Trailing, Current  | Amazon Redshift version against which you would like to replay your workload. This is applicable only for provisioned cluster. |
| BASE_RPU | Base capacity setting from 32 RPUs to 512 RPUs  | This setting specifies the base data warehouse capacity of your Amazon Redshift serverless workgroup. This is applicable only for Serverless workgroup. |


Here is a sample configuration JSON file, used to implement this example use-case:

```json
{
  "SNAPSHOT_ID": "redshift-cluster-manual-snapshot",
  "SNAPSHOT_ACCOUNT_ID": "123456789012",

  "PARAMETER_GROUP_CONFIG_S3_PATH": "s3://node-config-compare-bucket/pg_config.json",

  "DDL_AND_COPY_SCRIPT_S3_PATH": "s3://node-config-compare-bucket/ddl.sql",
  "SQL_SCRIPT_S3_PATH":"s3://node-config-compare-bucket/test_queries.sql",
  "NUMBER_OF_PARALLEL_SESSIONS_LIST": "1",

  "SIMPLE_REPLAY_LOG_LOCATION":"s3://redshift-logging-xxxxxxxx/RSLogs/",
  "SIMPLE_REPLAY_EXTRACT_START_TIME":"2021-08-28T11:15:00+00:00",
  "SIMPLE_REPLAY_EXTRACT_END_TIME":"2021-08-28T12:00:00+00:00",

  "SIMPLE_REPLAY_EXTRACT_OVERWRITE_S3_PATH":"N/A",
  "SIMPLE_REPLAY_OVERWRITE_S3_PATH":"N/A",

  "AUTO_PAUSE": true,
  "DATABASE_NAME": "database_name",

  "CONFIGURATIONS": [
  	{
  	"TYPE": "Provisioned",
  	"NODE_TYPE": "dc2.8xlarge",
  	"NUMBER_OF_NODES": "2",
  	"WLM_CONFIG_S3_PATH": "N/A"
  	},
  	{
  	"TYPE": "Provisioned",
  	"NODE_TYPE": "ra3.4xlarge",
  	"NUMBER_OF_NODES": "4",
  	"WLM_CONFIG_S3_PATH": "N/A"
  	},
  	{
  	"TYPE": "Provisioned",
  	"NODE_TYPE": "ra3.4xlarge",
  	"NUMBER_OF_NODES": "4",
  	"WLM_CONFIG_S3_PATH": "s3://node-config-compare-bucket/wlmconfig.json"
  	},
  	{
  	"TYPE": "Serverless",
  	"BASE_RPU": "64"
  	},
  	{
  	"TYPE": "Serverless",
  	"BASE_RPU": "128"
  	}
  ]
  }
```

**Please Note:** Make sure to use same Amazon S3 bucket to store all your configurations for this testing. For example, we used Amazon S3 bucket node-config-compare-bucket to store all configuration scripts. After populating all parameters in this JSON file, please save this JSON file in the same Amazon S3 bucket in your AWS Account.

## Deployment using AWS CloudFormation

Once the configuration JSON file is saved in an Amazon S3 bucket, you may use [this AWS CloudFormation template](https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=redshift-node-config-comparison&templateURL=https://amazon-redshift-node-config-compare.s3.amazonaws.com/cfn/redshift_node_config_compare_v2.yaml) to deploy this solution.This template provisions the required AWS Resources except the Amazon Redshift provisioned clusters and/or Serverless workgroups, which gets created in the subsequent step by an AWS Step Functions state machine. This template requires you to provide the following parameters:

| **CloudFormation Parameter** | **Valid Values** | **Description** |
| --- | --- | --- |
| ConfigJsonS3Path | Amazon S3 URI | Input S3 URI where you stored your JSON Configuration File from the previous step. The template would grant access on this Amazon S3 bucket to appropriate AWS resources created by this solution. |
| ClusterIdentifierPrefix | Prefix of Amazon Redshift endpoints ( **only lowercase is supported** ) | Input a valid string like rs, to be used as prefix of your Amazon Redshift provisioned clusters, namespaces & workgroups |
| AuditLoggingS3Bucket | N/A,Amazon S3 URI | If using Redshift Simple Replay, please input Redshift Audit Logging Bucket Name here so that it can grant appropriate permissions to the AWS Resources. You may also add an existing Amazon S3 bucket in same AWS Region, which can be accessed by Redshift. Input N/A if not applicable |
| GrantS3ReadOnlyAccessToRedshift | Yes,No | If you’re using Simple Replay in the same AWS account as the source Amazon Redshift provisioned cluster, enter Yes for this parameter, which grants AmazonS3ReadOnlyAccess to the new Amazon Redshift provisioned clusters and Serverless workgroups to replay copy statements within the account. Otherwise, enter No so you can’t replay copy statements if running on a different AWS account without manually configuring it.
| SourceRedshiftClusterKMSKeyARN | N/A, AWS KMS Key ARN |  [AWS Key Management Service (KMS) ](https://aws.amazon.com/kms/)Key ARN (Amazon Resource Name) if your source Redshift provisioned cluster is encrypted (available on the stack Outputs tab). You need to run extract and replay in the same account, if your source provisoned cluster is encrypted.
| OnPremisesCIDR | CIDR Notation |  The IP range (CIDR notation) for your existing infrastructure to access the target and replica provisioned clusters and Serverless workgroups from a SQL client. If unsure, enter your corporate desktop&#39;s CIDR address. For instance, if your desktop&#39;s IP address is 10.156.87.45, enter10.156.87.45/32.
| VPC | VPC ID	| An existing [Amazon Virtual Private Cloud](https://aws.amazon.com/vpc/) (Amazon VPC) where you want to deploy the provisioned clusters, Serverless workgroups and EC2 instances.
| RedshiftSubnetId | Subnet ID | You can provide upto 3 subnets within the same VPC to deploy the Amazon Redshift provisioned clusters and Serverless workgroups.
| AWSBatchSubnetId | Subnet ID | Provide 1 existing subnet (subnet should have route to the internet) within the VPC in which you deploy AWS Batch compute environment.
| AWSECRContainerImage | N/A, Amazon Elastic Container Registry | Default value is N/A, Provide container image if you would like to use your private image which is already available.
| UseAWSLakeFormationForGlueCatalog | No,Yes | Default value is No ,Select Yes if AWS Lake Formation is enabled for the account and manages access for Glue catalog
| NotificationEmail | N/A, Email address | Default value is N/A , Provide one email address if you would like to receive step function status notifications

## Orchestration with AWS Step Functions State Machine

This solution uses AWS Step Functions state machine to orchestrate the end-to-end workflow. The state machine performs the following steps to evaluate price performance of your Amazon Redshift workload:

1. First, it validates the inputs provided in the user configuration file and checks if audit logging is enabled.
2. If you have provided a valid SIMPLE\_REPLAY\_LOG\_LOCATION parameter value in the input JSON file, it runs extract and generates SQL files to replay the steps from [Amazon Redshift Simple Replay Utility](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/SimpleReplay) on your past workloads in Amazon Redshift provisoned clusters and/or Serverless workgroups based on the configurations you provided in the input JSON file. It replays your past workloads starting SIMPLE\_REPLAY\_EXTRACT\_START\_TIME till SIMPLE\_REPLAY\_EXTRACT\_END\_TIME as mentioned in the input JSON file.
3. It reads the configuration JSON file you provided and creates parallel steps work on different Amazon Redshift datawarehouse configurations in parallel.
2. For each of these steps, it starts by creating new Amazon Redshift provisoned clusters and/or Serverless workgroups based on the configurations you provided in the input JSON file.
3. If you have provided a valid SQL\_SCRIPT\_S3\_PATH parameter value in the input JSON file, it runs performance testing on each of these new Amazon Redshift provisoned clusters and/or Serverless workgroups in parallel. It runs these iterations concurrently based on the input parameter NUMBER\_OF\_PARALLEL\_SESSIONS\_LIST
4. It replay's the extracted workload from Step 2 in each of the Redshift datawarehouse configuration in parallel.
5. Then it [unloads](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) statistics of this testing from each of these Redshift datawarehouse configuration to an Amazon S3 bucket, which got created by the CloudFormation template stack in previous step.
6. If AUTO\_PAUSE parameter in the input JSON file is True, it will pause all the Amazon Redshift provisioned clusters
7. When above steps are completed for all new Amazon Redshift clusters and serverless workgroups that were created as part of this process, it runs an [AWS Glue Crawler](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html) to create tables in [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/populate-data-catalog.html) to facilitate comparing performance of these Amazon Redshift clusters from the unloaded statistics.
8. At the last step, it unloads the comparison results to the Amazon S3 bucket for your future reference.

You need to start a new execution of the state machine after the CloudFormation stack is deployed in your account. Subsequently, you may re-upload your input parameter JSON file to try changing different parameter values ( for e.g adding new Redshift datawarehouse configuration ) and then rerun this state machine from the [AWS Console](https://console.aws.amazon.com/states/home). Following diagram shows this AWS Step Functions State Machine workflow:

![Step Function](https://github.com/aws-samples/amazon-redshift-config-compare/blob/main/serverless-v2/images/statemachine.png)


For the example use-case, below Amazon Redshift provisioned clusters and serverless workgroups got created as part of the state machine execution.

![Redshift Clusters](https://github.com/aws-samples/amazon-redshift-config-compare/blob/main/serverless-v2/images/redshift-clusters-provisioned.png)
![Redshift Clusters](https://github.com/aws-samples/amazon-redshift-config-compare/blob/main/serverless-v2/images/redshift-clusters-serverless.png)

## Performance Evaluation

This solution creates an external schema redshift\_config\_comparison and creates three external tables comparison\_stats, cluster\_config and pricingin that schema to read the raw data created by this solution in an Amazon S3 bucket. Based on these external tables, it creates the views redshift\_config\_comparison\_results, redshift\_config\_comparison\_raw and redshift\_config\_comparison\_aggregrate in public schema of your Amazon Redshift clusters and serverless workgroups to compare the price-performance metrics across the different Redshift datawarehouse configurations.

**REDSHIFT\_CONFIG\_COMPARISON\_RESULTS:**

This view provides the aggregated comparison summary of your Amazon Redshift provisioned clusters and serverless workgroups. Test Type column in this view indicates that the test type was to replay your past workload using simple replay utility.

It provides the raw value and a percentage number for metrices like total, mean, median, max query times, percentile-75, percentile-90 and percentile-95 to show how your Amazon Redshift clusters and serverless workgroups are performing against each other.

For example, below was the outcome of your example use-case: ( ** **please note - the results below are for illustration purposes only based on our internal sample workloads, your test results might vary based on your workload** ** )

```sql
SELECT test_type,cluster_identifier,total_query_time_seconds,improvement_total_query_time,pct75_query_time_seconds,pct95_query_time_seconds
FROM public.redshift_config_comparison_results
order by total_query_time_seconds;
```

| **test_type** | **cluster_identifier** | **total_query_time_seconds** | **improvement_total_query_time** | **pct75_query_time_seconds** | **pct95_query_time_seconds** |
| --- | --- | --- | --- | --- | --- |
| simple-replay | workgroup-ncc-128 | 120.51 | 237% | 0.006 | 3.313
| simple-replay | ncc-ra3-4xlarge-4wlmconfig | 228.64 | 78% | 1.301 | 4.017
| simple-replay | ncc-ra3-4xlarge-4  | 252.2 | 61% | 1.276 | 4.665
| simple-replay | workgroup-ncc-64 | 269.71 | 51% | 1.937 | 5.677
| simple-replay | ncc-dc2-8xlarge-2 | 406.2 | 0% | 1.922 | 9.295

Based on above results, you may observe that Redshift serverless with 128 RPUs was the best performing configuration across all the Redshift configurations , ra3.4xlarge 4 nodes with concurreny scaling enabled was the best among the provisioned Redshift clusters.

**REDSHIFT\_CONFIG\_COMPARISON\_RAW:**

This view provides the query level comparison summary of your Amazon Redshift clusters and/or Serverless workgroups.

```sql
select query_hash,cluster_identifier,exec_time_seconds,elasped_time_seconds,queue_time_seconds,user_id,query_id
from public.redshift_config_comparison_raw;
```

| **query_hash** | **cluster_identifier** | **exec_time_seconds** | **elasped_time_seconds** | **queue_time_seconds** | **user_id** | **query_id** |
| --- | --- | --- | --- | --- | --- | --- |
| 0531f3b54885afb | ncc-dc2-8xlarge-2 | 5 | 6 | 0 | 100 | 623 |
| 0531f3b54885afb | ncc-ra3-4xlarge-4 | 4 | 5 | 0 | 100 | 727 |
| 0531f3b54885afb | ncc-ra3-4xlarge-4wlmconfig | 3 | 3 | 0 | 100 | 735 |
| 0531f3b54885afb | workgroup-ncc-64 | 2 | 3 | 0 | 100 | 718 |
| 0531f3b54885afb | workgroup-ncc-128 | 1 | 1 | 0 | 100 | 718 |

## Access permissions and security
To deploy this solution, you need administrator access on the AWS accounts where you plan to deploy the AWS CloudFormation resources for this solution.

User deploying the AWS CloudFormation stack needs full permission on these services:

AWS IAM, Amazon ECR, AWS Batch, AWS Lambda, Amazon CloudWatch, AWS Glue, Amazon S3, AWS StepFunction, Amazon Redshift, AWS Secrets Manager, Amazon EC2 – SecurityGroup, AWS LakeFormation (if Selected Yes for the CloudFormation parameter UseAWSLakeFormationForGlueCatalog)

The CloudFormation template provisions all the required resources using security best practices based on the principle of least privileges and hosts all resources within your account VPC. Access to the Amazon Redshift clusters is controlled with the CloudFormation template parameter OnPremisesCIDR, which you need to provide to allow on-premises users to connect to the new clusters using their SQL clients on the Amazon Redshift port.

Access permissions for all the resources are controlled using AWS Identity and Access Management (IAM) roles granting appropriate permissions to Amazon Redshift, AWS Lambda, AWS Step Functions, AWS Glue, and AWS Batch. Read and write access privileges are granted to the Amazon Redshift clusters and AWS Batch jobs on the S3 bucket created by the CloudFormation template so that it can read and update data and metadata configurations from that bucket. Read and write access privileges are also granted on the S3 bucket where the user configuration JSON file is uploaded. AWS Batch requires internet access in order to pull images from Amazon ECR public repository. AWS LakeFormation is used to manage access control on the AWS Glue catalog tables created for performance evaluation, this is optional, based on the UseAWSLakeFormationForGlueCatalog parameter in the CloudFormation template.

You can find [here](https://github.com/aws-samples/amazon-redshift-config-compare/blob/main/IAM_Permissions.pdf) the list of IAM permissions used in the utility.

## Troubleshooting

AWS Batch jobs can fail with error **– CannotPullContainerError**, if the subnet doesn’t have route to the internet to pull the container image. Refer to [this](https://aws.amazon.com/premiumsupport/knowledge-center/ecs-fargate-pull-container-error/) KB article to resolve the issue.

There might be some rare instances in which failures occur in the state machine running this solution. To troubleshoot, refer to its logs, along with logs from the AWS Batch jobs in [Amazon CloudWatch Logs](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/WhatIsCloudWatchLogs.html). To view the AWS Batch logs, navigate to the [Amazon CloudWatch](https://aws.amazon.com/cloudwatch/) console and choose **Logs** in the navigation pane. Find the log group with name **`<`Your CloudFormation Stack Name`>`/log** and choose the latest log streams.

![Cloudwatch Console](https://github.com/aws-samples/amazon-redshift-config-compare/blob/main/serverless-v2/images/batch-cw-log-group.png)

To view the Step Functions logs, navigate to the state machine’s latest run on the Step Functions console and choose CloudWatch Logs for the failed Step Functions step.

![State Machine Console](https://github.com/aws-samples/amazon-redshift-config-compare/blob/main/serverless-v2/images/statemachine-log.png)

After you fix the issue, you can restart the state machine by choosing New execution.

## Clean up

Running this template in your AWS account may have some cost implications because it provisions new Amazon Redshift provisioned clusters, serverless namespaces and workgroups. Once you are done with the evaluation and you don’t plan to run this test in future, you should delete the CloudFormation stack. It deletes all the resources it created, except the below ones needs to be deleted manually:

1. Amazon Redshift clusters, serverless workgroups and namespaces
2. Amazon S3 bucket created by the cloudformation stack

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
