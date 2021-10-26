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
from time import sleep
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook


class RedshiftDataOperator(BaseOperator):
    """
    Executes SQL Statements against an Amazon Redshift cluster using Redshift Data

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftDataOperator`

    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        or an iterable of str (sql statements)
    :param aws_conn_id: AWS connection id (default: aws_default)
    :type aws_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)

    def __init__(
        self,
        *,
        aws_conn_id: str = 'aws_default',
        cluster_identifier: str,
        database: str,
        sql: str,
        db_user: Optional[str] = "",
        parameters: Optional[list] = None,
        secret_arn: Optional[str] = "",
        statement_name: Optional[str] = "",
        with_event: Optional[bool] = False,
        timeout: Optional[int] = 300,
        poll_interval: Optional[int] = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.sql = sql
        self.parameters = parameters
        self.cluster_identifier = cluster_identifier
        self.database = database
        self.db_user = db_user
        self.secret_arn = secret_arn
        self.statement_name = statement_name
        self.with_event = with_event
        self.timeout = timeout
        self.poll_interval = poll_interval

    def execute_query(self):
        hook = self.get_hook()

        resp = hook.execute_statement(
            cluster_identifier=self.cluster_identifier,
            database=self.database,
            db_user=self.db_user,
            sql=self.sql,
            parameters=self.parameters,
            secret_arn=self.secret_arn,
            statement_name=self.statement_name,
            with_event=self.with_event,
        )
        return resp

    def wait_for_results(self, id):
        hook = self.get_hook()

        elapsed = 0
        while elapsed < self.timeout:
            self.log.info("Polling", id)
            status = hook.describe_statement(
                id=id,
            )
            print(status)
            if status == 'FINISHED':
                return status
            elif status == 'FAILED':
                raise ValueError(f"RedshiftDataHook.describe_statement {status}")
            elif status == 'ABORTED':
                raise ValueError(f"Query {status}")
            else:
                self.log.info(f"Query {status}")
            sleep(self.poll_interval)
            elapsed = elapsed + self.poll_interval

        raise AirflowException("Timeout. The operation could not be completed within the allotted time.")

    def get_hook(self) -> RedshiftDataHook:
        """Create and return RedshiftDataHook.
        :return RedshiftDataHook: A RedshiftDataHook instance.
        """
        return RedshiftDataHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: dict) -> None:
        """Execute a statement against Amazon Redshift"""
        self.log.info(f"Executing statement: {self.sql}")
        id = self.execute_query()
        self.wait_for_results(id)
        return id
