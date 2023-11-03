import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable

from kubernetes.client import models as k8s

default_args = {
    'owner': 'akotkova',    
    'retry_delay': timedelta(minutes=5),
}

IMAGE='harbor.neoflex.ru/dognauts/dognauts-airflow:2.5.3-py3.8-v6'


with DAG(
    dag_id = "fine_tune_rugpt",
    default_args=default_args,
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=60),
    description='fine-tune ruGPT model',
    start_date = airflow.utils.dates.days_ago(1),
    catchup=False
) as dag:

    def finetune_model():
        import sys
        import subprocess

        subprocess.check_call([sys.executable, '-m', 'pip', 'install','pytorch torchvision torchaudio cudatoolkit=11.4 -c pytorch -c nvidia'])         



    
    finetune_model_task = PythonOperator(
        task_id="finetune_model",
        python_callable=finetune_model,
        provide_context=True,
        executor_config = {
        "pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base", image=IMAGE)]))
    },
    )


finetune_model_task
