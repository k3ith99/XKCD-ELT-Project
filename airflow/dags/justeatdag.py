from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.python import PythonOperator
from utils import initial_extraction, fetch_latest_comic,polling_function
from airflow.sensors.base import PokeReturnValue
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

default_args = {
    "retries": 1,
    "start_date": datetime(2025, 11, 15),
    "catchup": False,
}
def get_dbt_config(group_id: str, select: list = None):
    '''
    reusable dbt config
    Args:
    group_id (str): Unique ID for the dbt TaskGroup.
        select (list): List of dbt models/tests to run in this TaskGroup.

    Returns:
        dict: A dictionary of configuration options used to initialize DbtTaskGroup.
    '''
    config = {
        'group_id': group_id,
        'project_config': ProjectConfig(
            dbt_project_path='/usr/local/airflow/include/dbt/comics_project'
        ),
        'profile_config': ProfileConfig(
            profile_name='comics_project',   
            target_name='dev',
            #profile_mapping=None 
            profiles_yml_filepath='/usr/local/airflow/include/dbt/comics_project/profiles.yml'         
        ),
        'operator_args': {
            'install_deps': False,
        }
    }

    if select:
        config['operator_args']['select'] = select

    return config

@dag(
    dag_id="comics_initial_load",
    schedule="@once",
    default_args=default_args,
)
def initial_load():
    '''
    Initial ELT to process historical data
    '''
    initial_call = PythonOperator(
        task_id="initial_extraction", python_callable=initial_extraction
    )
    #validate raw data
    test_source = DbtTaskGroup(
        **get_dbt_config('test_source'),
    )
    #build dimension tables in dbt
    dim_tables = DbtTaskGroup(
        **get_dbt_config('dim_tables', select=['dim_comics', 'dim_date'])
    )
    #build fact table in dbt
    fact_table = DbtTaskGroup(
        **get_dbt_config('fact_table', select=['fact_metrics'])
    )
    #build gold table in dbt
    gold_table = DbtTaskGroup(
        **get_dbt_config('gold_table', select=['gold_comics'])
    )
    #Run all remaining dbt tests
    test_all = DbtTaskGroup(
    **get_dbt_config('test_all', select=['test:*'])
)
    initial_call >> test_source >> dim_tables >> fact_table >> gold_table >> test_all

@dag(
    dag_id="comics_incremental",
    schedule= "0 8 * * 1,3,5",
    max_active_runs=1,
    default_args=default_args,
    is_paused_upon_creation=False,
)
def comics_incremental():
    '''
    Incremental ELT pipeline
    '''
    @task.sensor(poke_interval=300, timeout=3600, mode="poke")
    def check_new_api_data() -> PokeReturnValue:
        '''
        Sensor function that polls xkcd api to check whether new comic is available
        '''
        condition_met = polling_function()
        #add useful logs
        # the function has to return a PokeReturnValue
        # if is_done = True the sensor will exit successfully, if
        # is_done=False, the sensor will either poke or be rescheduled
        return PokeReturnValue(is_done=condition_met, xcom_value=condition_met)
    
    incremental_raw = PythonOperator(
        task_id="incremental_extraction", python_callable=fetch_latest_comic
        )
    rebuild_dimensions = DbtTaskGroup(
        **get_dbt_config('rebuild_dimensions', select=['dim_comics', 'dim_date'])
    )
    rebuild_fact = DbtTaskGroup(
        **get_dbt_config('rebuild_fact', select=['fact_metrics'])
    )
    rebuild_gold = DbtTaskGroup(
        **get_dbt_config('rebuild_gold', select=['gold_comics'])
    )
    validate = DbtTaskGroup(
        **get_dbt_config('validate_data',select=['test:*']),
    )
    check_new_api_data() >> incremental_raw >> rebuild_dimensions >> rebuild_fact >> rebuild_gold >> validate


initial_load()
comics_incremental()
