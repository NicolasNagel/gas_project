from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

from pipeline import main