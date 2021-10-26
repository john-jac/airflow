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
#

import unittest
from unittest import mock

from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook


class TestRedshiftDataHook(unittest.TestCase):
    cluster_identifier = 'test-cluster'
    database = 'test-database'
    sql = 'SELECT * FROM test_table'
    id = '12345-67890'
    parameters = [{"name": "id", "value": "1"}]

    @mock.patch.object(RedshiftDataHook, 'get_conn')
    def test_execute_statement(self, mock_client):
        hook = RedshiftDataHook()
        result = hook.execute_statement(
            cluster_identifier=self.cluster_identifier,
            database=self.database,
            sql=self.sql,
        )
        assert result

    @mock.patch.object(RedshiftDataHook, 'get_conn')
    def test_execute_statement_with_parameters(self, mock_client):
        hook = RedshiftDataHook()
        result = hook.execute_statement(
            cluster_identifier=self.cluster_identifier,
            database=self.database,
            sql=self.sql,
            parameters=self.parameters,
        )
        assert result

    @mock.patch.object(RedshiftDataHook, 'get_conn')
    def test_describe_statement(self, mock_client):
        hook = RedshiftDataHook()
        result = hook.describe_statement(
            id=id,
        )
        assert result

    @mock.patch.object(RedshiftDataHook, 'get_conn')
    def test_get_statement_result(self, mock_client):
        hook = RedshiftDataHook()
        result = hook.get_statement_result(
            id=id,
        )
        assert result

    @mock.patch.object(RedshiftDataHook, 'get_conn')
    def test_cancel_statement(self, mock_client):
        hook = RedshiftDataHook()
        result = hook.cancel_statement(
            id=id,
        )
        assert result
