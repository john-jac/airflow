#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Interact with AWS Redshift clusters."""

from typing import Optional

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class RedshiftDataHook(AwsBaseHook):
    """
    Interact with AWS Redshift Data, using the boto3 library

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :type aws_conn_id: str
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "redshift-data"
        super().__init__(*args, **kwargs)

    def execute_statement(
        self,
        cluster_identifier: str,
        database: str,
        sql: str,
        db_user: Optional[str] = "",
        parameters: Optional[list] = None,
        secret_arn: Optional[str] = "",
        statement_name: Optional[str] = "",
        with_event: Optional[bool] = False,
    ):
        """
        Runs an SQL statement, which can be data manipulation language (DML)
        or data definition language (DDL)

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        :param database: the name of the database
        :type database: str
        :param sql: the SQL statement text to run
        :type sql: str
        :param db_user: the database user name
        :type db_user: str
        :param parameters: the parameters for the SQL statement
        :type parameters: list
        :param secret_arn: the name or ARN of the secret that enables db access
        :type secret_arn: str
        :param statement_name: the name of the SQL statement
        :type statement_name: str
        :param with_event: indicates whether to send an event to EventBridge
        :type with_event: bool

        """
        """only provide parameter argument if it is valid"""
        if parameters:
            response = self.get_conn().execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                Sql=sql,
                DbUser=db_user,
                WithEvent=with_event,
                SecretArn=secret_arn,
                StatementName=statement_name,
                Parameters=parameters,
            )
        else:
            response = self.get_conn().execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                Sql=sql,
                DbUser=db_user,
                WithEvent=with_event,
                SecretArn=secret_arn,
                StatementName=statement_name,
            )
        return response['Id'] if response['Id'] else None

    def describe_statement(
        self,
        id: str,
    ):
        """
        Describes the details about a specific instance when a query was run
        by the Amazon Redshift Data API

        :param id: the identifier of the SQL statement to describe.
        :type id: str

        """
        response = self.get_conn().describe_statement(
            Id=id,
        )
        return response

    def get_statement_result(
        self,
        id: str,
        next_token: Optional[str] = "",
    ):
        """
        Fetches the temporarily cached result of an SQL statement, a token is
        returned to page through the statement results

        :param id: the identifier of the SQL statement to describe.
        :type id: str
        :param next_token: a value that indicates the starting point for the next set of response records
        :type next_token: str

        """
        response = self.get_conn().get_statement_result(
            Id=id,
            NextToken=next_token,
        )
        return response

    def cancel_statement(
        self,
        id: str,
    ):
        """
        Cancels a running query, to be canceled, a query must be running

        :param id: the identifier of the SQL statement to describe.
        :type id: str

        """
        response = self.get_conn().cancel_statement(
            Id=id,
        )
        return response
