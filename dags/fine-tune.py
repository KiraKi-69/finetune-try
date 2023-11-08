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

import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"

IMAGE='harbor.neoflex.ru/dognauts/dognauts-airflow:2.5.3-py3.8-v6ACC'

vol1 = k8s.V1VolumeMount(
    name='test-volume', mount_path='/data')
volume = k8s.V1Volume(
    name='test-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name='my-volume'
        ),
)

container_resources = k8s.V1ResourceRequirements(
        limits={
            "memory": "30Gi",
            "cpu": 3.0,
            "nvidia.com/gpu": '1',
        },
        requests={
            "memory": "30Gi",
            "cpu": 3.0,
            "nvidia.com/gpu": '1',
        },
    )

pod_override = k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[k8s.V1Container(name="base", image=IMAGE, resources=container_resources, volume_mounts=[vol1])],
                volumes=[volume],
            )
)


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
        from transformers import pipeline

        generator = pipeline("text-generation", model="/data/models/rugpt3small")

        print(
            generator(
            "Вопрос: Что такое учебный центр Neoflex? Ответ: ", 
            do_sample=True, max_length=200)
            )


#     load_finetune_script = BashOperator(
#         task_id="load_finetune_script",
#         bash_command="wget -P /data https://raw.githubusercontent.com/KiraKi-69/finetune-try/main/dags/run_clm.py https://raw.githubusercontent.com/KiraKi-69/finetune-try/main/dags/train.txt",
#         executor_config = {
#         "pod_override": pod_override
#     },
# )
#     mkdir_script = BashOperator(
#         task_id="mkdir",
#         bash_command="mkdir /data/models",
#         executor_config = {
#         "pod_override": pod_override
#     },
# )
#     finetune_this = BashOperator(
#         task_id="finetune",
#         bash_command="""python /data/run_clm.py --model_name_or_path sberbank-ai/rugpt3small_based_on_gpt2 --train_file /data/train.txt --per_device_train_batch_size 1 --block_size 2048 --dataset_config_name plain_text --do_train --num_train_epochs 20 --output_dir /data/models/rugpt3small --overwrite_output_dir""",
#         executor_config = {
#         "pod_override": pod_override
#     },
# )
    
    save_model = PythonOperator(
        task_id="finetune_model",
        python_callable=finetune_model,
        provide_context=True,
        executor_config = {
        "pod_override": pod_override
    },
    )


# load_finetune_script >> mkdir_script >> finetune_this >> save_model
# load_finetune_script >> finetune_this >> save_model
save_model