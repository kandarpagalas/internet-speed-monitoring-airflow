from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable

alert_email_to = Variable.get("infrastructure_manager_email")
def run_speedtest(**kwargs):
    import speedtest
    servers = []
    threads = None

    s = speedtest.Speedtest()
    s.get_servers(servers)
    s.get_best_server()
    
    # Converte velocidade para Mbps
    velocidade_download = round(s.download(threads=threads)*(10**-6))
    velocidade_upload = round(s.upload(threads=threads)*(10**-6))
    
    # Resultado da medição
    results_dict = s.results.dict()
    
    # Registra no log o resultado
    data_atual = datetime.datetime.now().strftime('Data: %d/%m/%Y Hora: %H:%M')
    print(f'{data_atual} Download: {velocidade_download}mbps Upload: {velocidade_upload}mbps')
    
    return results_dict

def create_body(**kwargs):
    # Realiza leitura do XComs para coletar retorno da taskt 'speedtest'
    task_instance = kwargs['ti']
    speedtest_result = dict(task_instance.xcom_pull(task_ids='speedtest'))
    
    # Converte velocidade para Mbps
    velocidade_download = round(speedtest_result['download']*(10**-6))
    velocidade_upload = round(speedtest_result['upload']*(10**-6))
    
    data_atual = datetime.datetime.now().strftime('Data: %d/%m/%Y Hora: %H:%M')
    
    # Cria estrutura da mensagem
    body = f"""
    # Velocidade da internet \n
        - Leitura: {data_atual}\n
        - Download: {velocidade_download} mbps\n
        - Upload: {velocidade_upload} mbps\n
        - Porcentagem do contratado: {velocidade_download/100}%\n\n
        
        ### Talvez seja o momento de resetar o modem
    """
    return body

def speed_analyses(**kwargs):
    task_instance = kwargs['ti']
    speedtest_result = dict(task_instance.xcom_pull(task_ids='speedtest'))
    
    # Converte velocidade para Mbps
    velocidade_download = round(speedtest_result['download']*(10**-6))
    velocidade_upload = round(speedtest_result['upload']*(10**-6))
    
    # Verifica se a velocidade da internet está aceitável ou deve ser enviado alerta
    velocidade_contratada = 100
    if(velocidade_download < 0.2 * velocidade_contratada):
        return 'open_service_order_to_provider'
    elif(velocidade_download < 0.35 * velocidade_contratada):
        return 'create_alert_body'
    else:
        return 'internet_speed_is_ok'

with DAG(
    dag_id="speedtest",
    schedule="0,15,30,45 * * * *",
    start_date=pendulum.datetime(2023, 3, 25, tz="America/Fortaleza"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["internet"],
) as dag:
    
    speedtest = PythonOperator(
        task_id="speedtest",
        python_callable = run_speedtest,
        op_kwargs = {},
        provide_context = True 
    )
    
    download_speed_analyses = BranchPythonOperator(
        task_id="download_speed_analyses",
        python_callable = speed_analyses,
        op_kwargs = {},
        provide_context = True,
    )
    
    upload_to_s3 = S3CreateObjectOperator(
        task_id="upload_to_s3",
        aws_conn_id = 'minio_s3_conn',
        s3_bucket = 'speedtest',
        s3_key = '{{ ts_nodash }}.txt',
        data = "{{ task_instance.xcom_pull(task_ids='speedtest') }}",
        replace = False,
    )
    
    send_alert = EmailOperator(
        task_id="send_alert",
        to = ['alert_email_to'],
        subject = 'Internet está lenta',
        html_content = "{{ task_instance.xcom_pull(task_ids='create_alert_body') }}",
    )
    
    create_alert_body = PythonOperator(
        task_id="create_alert_body",
        python_callable = create_body,
        op_kwargs = {},
        provide_context = True,
    )
    
    internet_speed_is_ok = EmptyOperator(
        task_id="internet_speed_is_ok",
    )
    
    open_service_order_to_provider = EmptyOperator(
        task_id="open_service_order_to_provider",
    )

    speedtest >> upload_to_s3
    speedtest >> download_speed_analyses >> [create_alert_body, internet_speed_is_ok, open_service_order_to_provider]
    create_alert_body >> send_alert
if __name__ == "__main__":
    dag.test()
    
