# from airflow.contrib.hooks.fs_hook import FSHook
#
# from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator    import DummyOperator
from airflow.operators.python_operator    import PythonOperator
from airflow.operators.trigger_dagrun import  TriggerDagRunOperator

#file place in /data/ds_env/..../packages-site/
from omega_plugin_file import OmegaFileSensor, ArchiveFileOperator

#installed using pip (check pip freez)
from airflow.providers.papermill.operators.papermill import PapermillOperator

import datetime
from datetime import date, timedelta
import airflow

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from textwrap import dedent


default_args = {
    "depends_on_past" : False,
    "start_date"      : airflow.utils.dates.days_ago( 1 ),
    "retries"         : 1,
    "retry_delay"     : datetime.timedelta( hours= 5 ),
}


user = 'ys'


with airflow.DAG( "A_ETL", default_args= default_args, schedule_interval= "@once"  ) as dag:
    start_task  = DummyOperator(  task_id= "start" )
    stop_task   = DummyOperator(  task_id= "stop"  )
    
    Libraries_Installation = BashOperator(
        task_id='Libraries_Installation',
        bash_command=f'jupyter nbconvert --execute --clear-output  /home/{user}/notebook/Libraries_Installation.ipynb',
    )
    
    DS_Cleaning = BashOperator(
        task_id='DS_Cleaning',
        bash_command=f'jupyter nbconvert --execute --clear-output  /home/{user}/notebook/DS_Cleaning.ipynb',
    )
    
    DSE_Cleaning = BashOperator(
        task_id='DSE_Cleaning',
        bash_command=f'jupyter nbconvert --execute --clear-output  /home/{user}/notebook/DSE_Cleaning.ipynb',
    )
    
    Prof_Cleaning = BashOperator(
        task_id='Prof_Cleaning',
        bash_command=f'jupyter nbconvert --execute --clear-output  /home/{user}/notebook/Prof_Cleaning.ipynb',
    )    
        
    DS_Insertion_to_DB = BashOperator(
        task_id='DS_Insertion_to_DB',
        bash_command=f'jupyter nbconvert --execute --clear-output  /home/{user}/notebook/DS_Insertion_to_DB.ipynb',
    )
    
        
    DSE_Insertion_to_DB = BashOperator(
        task_id='DSE_Insertion_to_DB',
        bash_command=f'jupyter nbconvert --execute --clear-output  /home/{user}/notebook/DSE_Insertion_to_DB.ipynb',
    )
    
    
        
    Prof_Insertion_to_DB = BashOperator(
        task_id='Prof_Insertion_to_DB',
        bash_command=f'jupyter nbconvert --execute --clear-output  /home/{user}/notebook/Prof_Insertion_to_DB.ipynb',
    )
    
    
        
    Loggin_Data_Cleaning = BashOperator(
        task_id='Loggin_Data_Cleaning',
        bash_command=f'jupyter nbconvert --execute --clear-output  /home/{user}/notebook/Loggin_Data_Cleaning.ipynb',
    )
    
    Logging_Data_Insertion_to_DB = BashOperator(
        task_id='Logging_Data_Insertion_to_DB',
        bash_command=f'jupyter nbconvert --execute --clear-output  /home/{user}/notebook/Logging_Data_Insertion_to_DB.ipynb',
    )
    
    
    Statistics_Output = BashOperator(
        task_id='Statistics_Output',
        bash_command=f'jupyter nbconvert --execute --clear-output  /home/{user}/notebook/Statistics_Output.ipynb',
    )

    trigger_again = TriggerDagRunOperator(
        task_id='trigger_dag_again', 
        trigger_dag_id="A_ETL", 
        dag=dag
    )
   
start_task >> Libraries_Installation >>  DS_Cleaning >> DSE_Cleaning >> Prof_Cleaning >> DS_Insertion_to_DB >> DSE_Insertion_to_DB >> Prof_Insertion_to_DB >> Loggin_Data_Cleaning >> Logging_Data_Insertion_to_DB >> Statistics_Output >>  stop_task >> trigger_again