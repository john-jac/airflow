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

import unittest
from unittest import mock
from unittest.mock import MagicMock

from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


class TestRedshiftDataOperator(unittest.TestCase):
    @mock.patch("airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator.get_hook")
    def test_redshift_operator(self, mock_get_hook):
        hook = MagicMock()
        mock_execute_statement = hook.execute_statement
        mock_describe_statement = hook.describe_statement
        mock_get_hook.return_value = hook
        mock_describe_statement.return_value = 'FINISHED'
        mock_execute_statement.return_value = '123456'
        sql = MagicMock()
        cluster_identifier = MagicMock()
        database = MagicMock()
        db_user = MagicMock()

        operator = RedshiftDataOperator(
            task_id='test',
            sql=sql,
            cluster_identifier=cluster_identifier,
            database=database,
            db_user=db_user,
            parameters=None,
            secret_arn='',
            statement_name='',
            with_event=False,
        )
        operator.execute(None)
        mock_execute_statement.assert_called_once_with(
            sql=sql,
            cluster_identifier=cluster_identifier,
            database=database,
            db_user=db_user,
            parameters=None,
            secret_arn='',
            statement_name='',
            with_event=False,
        )
