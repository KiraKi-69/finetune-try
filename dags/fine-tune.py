import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

from kubernetes.client import models as k8s

default_args = {
    'owner': 'akotkova',    
    'retry_delay': timedelta(minutes=5),
}

IMAGE='harbor.neoflex.ru/dognauts/dognauts-airflow:2.5.3-py3.8-v6T'


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
        import os
    
        os.system('mkdir models')

        os.system("""
        python run_clm.py 
        --model_name_or_path sberbank-ai/rugpt3small_based_on_gpt2 
        --train_file train.txt 
        --per_device_train_batch_size 1 
        --block_size 2048 
        --dataset_config_name plain_text 
        --do_train 
        --num_train_epochs 20 
        --output_dir models/rugpt3small 
        --overwrite_output_dir
        """)

        from transformers import pipeline

        generator = pipeline("text-generation", model="models/rugpt3small")

        print(generator("Вопрос: Что такое учебный центр Neoflex? Ответ: ", do_sample=True, max_length=200))

    
    finetune_this = BashOperator(
        task_id="finetune",
        bash_command="""python run_clm.py 
        --model_name_or_path sberbank-ai/rugpt3small_based_on_gpt2 
        --train_file train.txt 
        --per_device_train_batch_size 1 
        --block_size 2048 
        --dataset_config_name plain_text 
        --do_train 
        --num_train_epochs 20 
        --output_dir models/rugpt3small 
        --overwrite_output_dir""",
)

    
    finetune_model_task = PythonOperator(
        task_id="finetune_model",
        python_callable=finetune_model,
        provide_context=True,
        executor_config = {
        "pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base", image=IMAGE)]))
    },
    )


finetune_this >> finetune_model_task
