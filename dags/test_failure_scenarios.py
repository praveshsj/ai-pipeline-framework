"""
Sample DAGs with Intentional Failures
For testing AI Failure Analyzer

Author: Pravesh Sundriyal
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import random
import time
import pandas as pd
import sys

# Import AI analyzer
sys.path.append('/opt/airflow')
# #from ai_analyzer.failure_analyzer import analyze_airflow_failure

default_args = {
    'owner': 'pravesh',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Don't auto-retry, let AI decide
}


# ============================================================================
# FAILURE SCENARIO 1: Out of Memory (OOM)
# ============================================================================

def task_oom_error(**context):
    """
    Simulates an Out of Memory error
    AI should recommend: SCALE_UP
    """
    print("Loading large dataset...")
    # Try to allocate huge memory
    large_list = []
    try:
        for i in range(100000000):  # 100 million items
            large_list.append([random.random() for _ in range(1000)])
            if i % 1000000 == 0:
                print(f"Loaded {i} records...")
    except MemoryError as e:
        print("ERROR: Out of memory!")
        raise MemoryError("Task ran out of memory while processing data. Heap space exhausted.")


dag_oom = DAG(
    'test_oom_failure',
    default_args=default_args,
    description='Test OOM failure - AI should recommend scaling',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'failure', 'oom'],
)

task_oom = PythonOperator(
    task_id='process_large_dataset',
    python_callable=task_oom_error,
    dag=dag_oom,
)


# ============================================================================
# FAILURE SCENARIO 2: Data Quality Issue
# ============================================================================

def task_data_quality_error(**context):
    """
    Simulates a data quality issue (null values, schema mismatch)
    AI should recommend: CHECK_DATA
    """
    print("Reading data from source...")
    
    # Simulate reading data with quality issues
    data = {
        'user_id': [1, 2, None, 4, 5],  # NULL value!
        'amount': [100, 200, 300, -50, 500],  # Negative value!
        'date': ['2026-02-01', '2026-02-02', 'invalid', '2026-02-04', '2026-02-05']  # Invalid date!
    }
    
    df = pd.DataFrame(data)
    print(f"Loaded {len(df)} records")
    
    # Validation checks
    null_count = df['user_id'].isna().sum()
    if null_count > 0:
        raise ValueError(f"Data quality check failed: Found {null_count} null values in user_id column. "
                        f"This violates NOT NULL constraint. Data source may have issues.")
    
    negative_amounts = (df['amount'] < 0).sum()
    if negative_amounts > 0:
        raise ValueError(f"Data quality check failed: Found {negative_amounts} negative amounts. "
                        f"Business rule violation: amount must be positive.")


dag_data_quality = DAG(
    'test_data_quality_failure',
    default_args=default_args,
    description='Test data quality failure - AI should recommend checking data',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'failure', 'data-quality'],
)

task_dq = PythonOperator(
    task_id='validate_data',
    python_callable=task_data_quality_error,
    dag=dag_data_quality,
)


# ============================================================================
# FAILURE SCENARIO 3: Transient Network Error
# ============================================================================

def task_network_error(**context):
    """
    Simulates a transient network error
    AI should recommend: RETRY
    """
    print("Connecting to external API...")
    time.sleep(2)
    
    # Simulate random network failure (70% chance)
    if random.random() < 0.7:
        raise ConnectionError("Connection to external API failed: [Errno 111] Connection refused. "
                            "Remote server at api.example.com:443 is not responding. "
                            "This is typically a transient network issue.")
    
    print("Successfully connected!")
    return "Data fetched"


dag_network = DAG(
    'test_network_failure',
    default_args=default_args,
    description='Test network failure - AI should recommend retry',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'failure', 'network'],
)

task_network = PythonOperator(
    task_id='fetch_external_data',
    python_callable=task_network_error,
    dag=dag_network,
)


# ============================================================================
# FAILURE SCENARIO 4: Code Error (Bug)
# ============================================================================

def task_code_error(**context):
    """
    Simulates a code error (NameError, TypeError, etc.)
    AI should recommend: FIX_CODE
    """
    print("Processing data...")
    
    # Code has a bug - undefined variable
    result = process_data(data)  # 'data' and 'process_data' not defined!
    
    return result


dag_code_error = DAG(
    'test_code_error',
    default_args=default_args,
    description='Test code error - AI should recommend fixing code',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'failure', 'code-error'],
)

task_code = PythonOperator(
    task_id='process_with_bug',
    python_callable=task_code_error,
    dag=dag_code_error,
)


# ============================================================================
# FAILURE SCENARIO 5: Timeout
# ============================================================================

def task_timeout_error(**context):
    """
    Simulates a task that takes too long
    AI should recommend: INCREASE_TIMEOUT or OPTIMIZE_CODE
    """
    print("Starting long-running process...")
    
    # Simulate a very slow operation
    for i in range(100):
        time.sleep(1)  # Total: 100 seconds
        print(f"Progress: {i+1}/100")
        
        # Intentionally never finishes in time
        if i == 99:
            time.sleep(3600)  # Sleep for 1 hour (will timeout)
    
    return "Completed"


dag_timeout = DAG(
    'test_timeout_failure',
    default_args={**default_args, 'execution_timeout': timedelta(seconds=30)},
    description='Test timeout failure - AI should recommend increasing timeout',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'failure', 'timeout'],
)

task_timeout = PythonOperator(
    task_id='long_running_task',
    python_callable=task_timeout_error,
    dag=dag_timeout,
)


# ============================================================================
# FAILURE SCENARIO 6: Spark Job Failure
# ============================================================================

def task_spark_error(**context):
    """
    Simulates a Spark job failure
    AI should provide Spark-specific recommendations
    """
    from pyspark.sql import SparkSession
    
    print("Starting Spark job...")
    
    spark = SparkSession.builder \
        .appName("TestSparkFailure") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "512m") \
        .getOrCreate()
    
    # Try to process large data with insufficient memory
    large_data = [(i, f"value_{i}", random.random()) for i in range(10000000)]
    df = spark.createDataFrame(large_data, ["id", "value", "amount"])
    
    # This will likely fail due to insufficient memory
    result = df.groupBy("value").sum("amount").collect()
    
    spark.stop()
    return f"Processed {len(result)} groups"


dag_spark = DAG(
    'test_spark_failure',
    default_args=default_args,
    description='Test Spark failure - AI should provide Spark tuning advice',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'failure', 'spark'],
)

task_spark = PythonOperator(
    task_id='run_spark_job',
    python_callable=task_spark_error,
    dag=dag_spark,
)


# ============================================================================
# FAILURE SCENARIO 7: Permission/Access Error
# ============================================================================

task_permission_error = BashOperator(
    task_id='write_to_restricted_location',
    bash_command='echo "test" > /root/restricted_file.txt',  # Permission denied!
    dag=DAG(
        'test_permission_failure',
        default_args=default_args,
        description='Test permission failure - AI should recommend fixing permissions',
        schedule_interval=None,
        catchup=False,
        tags=['test', 'failure', 'permission'],
    ),
)


# ============================================================================
# SUCCESS SCENARIO: For comparison
# ============================================================================

def task_success(**context):
    """
    A task that always succeeds
    """
    print("Processing data...")
    time.sleep(2)
    print("✅ Task completed successfully!")
    return {"status": "success", "records_processed": 1000}


dag_success = DAG(
    'test_success',
    default_args=default_args,
    description='Test successful task - for comparison',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'success'],
)

task_success_op = PythonOperator(
    task_id='successful_task',
    python_callable=task_success,
    dag=dag_success,
)
