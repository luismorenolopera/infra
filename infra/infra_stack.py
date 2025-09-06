from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue as glue,
    aws_athena as athena,
    CfnOutput,
    aws_lakeformation as lf,
)
from constructs import Construct


class InfraStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        data_bucket = s3.Bucket(
            self,
            "DataLakeRawBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        athena_results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        lambda_fn = _lambda.Function(
            self,
            "ExtractUsersFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.main",
            code=_lambda.Code.from_asset("lambda/extract_users"),
            timeout=Duration.seconds(60),
            environment={
                "DATA_BUCKET": data_bucket.bucket_name,
                "OUTPUT_PREFIX": "raw/jsonplaceholder/users/",
            },
        )

        data_bucket.grant_write(lambda_fn)
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(actions=["s3:ListBucket"], resources=[data_bucket.bucket_arn])
        )

        glue_db = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="users_db",
                description="Database for users extracted from JSONPlaceholder",
            ),
        )

        crawler_role = iam.Role(
            self,
            "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3ReadOnlyAccess"),
            ],
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[data_bucket.bucket_arn],
                conditions={"StringLike": {"s3:prefix": ["raw/jsonplaceholder/users/*", "raw/jsonplaceholder/users"]}},
            )
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"{data_bucket.bucket_arn}/raw/jsonplaceholder/users*"],
            )
        )

        crawler = glue.CfnCrawler(
            self,
            "UsersCrawler",
            name="users_crawler",
            role=crawler_role.role_arn,
            database_name="users_db",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{data_bucket.bucket_name}/raw/jsonplaceholder/users/",
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
        )
        crawler.add_dependency(glue_db)

        lf_admins = lf.CfnDataLakeSettings(
            self,
            "LfAdmins",
            admins=[
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=f"arn:aws:iam::{self.account}:root"
                )
            ],
        )

        lf_resource = lf.CfnResource(
            self,
            "LfRegisterDataBucket",
            resource_arn=data_bucket.bucket_arn,
            use_service_linked_role=True,
        )

        lf_perm_location = lf.CfnPermissions(
            self,
            "LfPermDataLocationForCrawler",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=crawler_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                data_location=lf.CfnPermissions.DataLocationResourceProperty(
                    catalog_id=self.account,
                    resource_arn=data_bucket.bucket_arn,
                )
            ),
            permissions=["DATA_LOCATION_ACCESS"],
        )
        lf_perm_location.add_dependency(lf_resource)

        lf_perm_db = lf.CfnPermissions(
            self,
            "LfPermDbForCrawler",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=crawler_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database=lf.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=self.account,
                    name="users_db",
                )
            ),
            permissions=["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE"],
        )
        lf_perm_db.add_dependency(glue_db)

        workgroup = athena.CfnWorkGroup(
            self,
            "UsersAthenaWorkGroup",
            name="users_wg",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_results_bucket.bucket_name}/athena-results/",
                )
            ),
            description="WorkGroup for querying users_db",
            state="ENABLED",
        )

        CfnOutput(self, "DataBucketName", value=data_bucket.bucket_name)
        CfnOutput(self, "AthenaResultsBucketName", value=athena_results_bucket.bucket_name)
        CfnOutput(self, "GlueDatabaseName", value="users_db")
        CfnOutput(self, "GlueCrawlerName", value="users_crawler")
        CfnOutput(self, "AthenaWorkGroupName", value=workgroup.name)
