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

IMAGE='harbor.neoflex.ru/dognauts/dognauts-airflow:2.5.3-py3.8-v6TWC'


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
    
        os.system('mkdir /opt/airflow/models')
        os.system("""python opt/airflow/run_clm.py 
        --model_name_or_path sberbank-ai/rugpt3small_based_on_gpt2 
        --train_file /opt/airflow/train.txt 
        --per_device_train_batch_size 1 
        --block_size 2048 
        --dataset_config_name plain_text 
        --do_train 
        --num_train_epochs 20 
        --output_dir /opt/airflow/models/rugpt3small 
        --overwrite_output_dir""")

        from transformers import pipeline

        generator = pipeline("text-generation", model="models/rugpt3small")

        print(generator("Вопрос: Что такое учебный центр Neoflex? Ответ: ", do_sample=True, max_length=200))

#     load_finetune_script = BashOperator(
#         task_id="load_finetune_script",
#         bash_command="wget https://raw.githubusercontent.com/huggingface/transformers/main/examples/pytorch/language-modeling/run_clm.py",
#         executor_config = {
#         "pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base", image=IMAGE)]))
#     },
# )
#     mkdir_script = BashOperator(
#         task_id="mkdir",
#         bash_command="mkdir models",
#         executor_config = {
#         "pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base", image=IMAGE)]))
#     },
# )
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
        executor_config = {
        "pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base", image=IMAGE)]))
    },
)
    
    save_model = PythonOperator(
        task_id="finetune_model",
        python_callable=finetune_model,
        provide_context=True,
        executor_config = {
        "pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base", image=IMAGE)]))
    },
    )


# load_finetune_script >> mkdir_script >> finetune_this >> save_model
save_model