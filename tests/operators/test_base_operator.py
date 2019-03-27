# -*- coding: utf-8 -*-
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

from __future__ import print_function, unicode_literals

import unittest
from datetime import timedelta

import airflow

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from airflow.exceptions import AirflowException


DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

TI_CONTEXT_ENV_VARS = ['AIRFLOW_CTX_DAG_ID',
                       'AIRFLOW_CTX_TASK_ID',
                       'AIRFLOW_CTX_EXECUTION_DATE',
                       'AIRFLOW_CTX_DAG_RUN_ID']

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='example_kubernetes_executor', default_args=args,
    schedule_interval=None
)


def print_stuff():
    print("stuff!")


class BaseOperatorTest(unittest.TestCase):

    def test_base_operator_run(self):
        """Tests that the python callable is invoked on task run."""
        success_task = PythonOperator(
            task_id="start_task", python_callable=print_stuff, dag=dag,
            limit_resource="1C2G"
        )
        self.assertEqual(1, len(success_task.executor_config))

        self.assertRaises(
            AirflowException,
            PythonOperator, task_id="start_task", python_callable=print_stuff,
            dag=dag, limit_resource="8C2G")

        self.assertRaises(
            AirflowException,
            PythonOperator, task_id="start_task", python_callable=print_stuff,
            dag=dag, limit_resource="1C16G")

        self.assertRaises(
            AirflowException,
            PythonOperator, task_id="start_task", python_callable=print_stuff,
            dag=dag, limit_resource="1G")

        self.assertRaises(
            AirflowException,
            PythonOperator, task_id="start_task", python_callable=print_stuff,
            dag=dag, limit_resource="1C0G")
