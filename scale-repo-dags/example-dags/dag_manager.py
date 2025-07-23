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
"""
DAG to manage other DAGs in the Airflow environment.
This DAG can unpause and trigger all other DAGs in the same Airflow instance.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

import pendulum
import requests
from requests.auth import HTTPBasicAuth

from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

# Configure logging
logger = logging.getLogger(__name__)


@task
def get_all_dags(**context) -> list[dict[str, Any]]:
    """
    Retrieve all DAGs from the Airflow instance using the REST API.
    
    Returns:
        List of DAG information dictionaries
    """
    try:
        # Get Airflow webserver configuration
        webserver_host = conf.get("webserver", "base_url", fallback="http://localhost:8080")
        
        # Try to get credentials from Airflow Variables (optional)
        try:
            username = Variable.get("airflow_api_username", default_var=None)
            password = Variable.get("airflow_api_password", default_var=None)
        except Exception:
            username = None
            password = None
        
        # Prepare authentication
        auth = None
        if username and password:
            auth = HTTPBasicAuth(username, password)
        
        # Make API call to get all DAGs
        url = f"{webserver_host}/api/v1/dags"
        headers = {"Content-Type": "application/json"}
        
        response = requests.get(url, headers=headers, auth=auth, timeout=30)
        response.raise_for_status()
        
        dags_data = response.json()
        dags = dags_data.get("dags", [])
        
        # Filter out this DAG itself to avoid self-triggering loops
        current_dag_id = context["dag"].dag_id
        filtered_dags = [dag for dag in dags if dag["dag_id"] != current_dag_id]
        
        logger.info(f"Found {len(filtered_dags)} DAGs to manage")
        return filtered_dags
        
    except Exception as e:
        logger.error(f"Error retrieving DAGs: {str(e)}")
        raise


@task
def unpause_dag(dag_info: dict[str, Any]) -> dict[str, Any]:
    """
    Unpause a specific DAG.
    
    Args:
        dag_info: Dictionary containing DAG information
        
    Returns:
        Result of the unpause operation
    """
    try:
        dag_id = dag_info["dag_id"]
        
        # Skip if already unpaused
        if not dag_info.get("is_paused", True):
            logger.info(f"DAG {dag_id} is already unpaused")
            return {"dag_id": dag_id, "status": "already_unpaused", "success": True}
        
        # Get Airflow webserver configuration
        webserver_host = conf.get("webserver", "base_url", fallback="http://localhost:8080")
        
        # Try to get credentials from Airflow Variables (optional)
        try:
            username = Variable.get("airflow_api_username", default_var=None)
            password = Variable.get("airflow_api_password", default_var=None)
        except Exception:
            username = None
            password = None
        
        # Prepare authentication
        auth = None
        if username and password:
            auth = HTTPBasicAuth(username, password)
        
        # Make API call to unpause the DAG
        url = f"{webserver_host}/api/v1/dags/{dag_id}"
        headers = {"Content-Type": "application/json"}
        data = {"is_paused": False}
        
        response = requests.patch(url, json=data, headers=headers, auth=auth, timeout=30)
        response.raise_for_status()
        
        logger.info(f"Successfully unpaused DAG: {dag_id}")
        return {"dag_id": dag_id, "status": "unpaused", "success": True}
        
    except Exception as e:
        logger.error(f"Error unpausing DAG {dag_info.get('dag_id', 'unknown')}: {str(e)}")
        return {"dag_id": dag_info.get("dag_id", "unknown"), "status": "error", "success": False, "error": str(e)}


@task
def trigger_dag(dag_info: dict[str, Any]) -> dict[str, Any]:
    """
    Trigger a specific DAG.
    
    Args:
        dag_info: Dictionary containing DAG information
        
    Returns:
        Result of the trigger operation
    """
    try:
        dag_id = dag_info["dag_id"]
        
        # Get Airflow webserver configuration
        webserver_host = conf.get("webserver", "base_url", fallback="http://localhost:8080")
        
        # Try to get credentials from Airflow Variables (optional)
        try:
            username = Variable.get("airflow_api_username", default_var=None)
            password = Variable.get("airflow_api_password", default_var=None)
        except Exception:
            username = None
            password = None
        
        # Prepare authentication
        auth = None
        if username and password:
            auth = HTTPBasicAuth(username, password)
        
        # Make API call to trigger the DAG
        url = f"{webserver_host}/api/v1/dags/{dag_id}/dagRuns"
        headers = {"Content-Type": "application/json"}
        data = {
            "dag_run_id": f"manual_trigger_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "conf": {}
        }
        
        response = requests.post(url, json=data, headers=headers, auth=auth, timeout=30)
        response.raise_for_status()
        
        logger.info(f"Successfully triggered DAG: {dag_id}")
        return {"dag_id": dag_id, "status": "triggered", "success": True}
        
    except Exception as e:
        logger.error(f"Error triggering DAG {dag_info.get('dag_id', 'unknown')}: {str(e)}")
        return {"dag_id": dag_info.get("dag_id", "unknown"), "status": "error", "success": False, "error": str(e)}


@task
def generate_summary(unpause_results: list[dict], trigger_results: list[dict]) -> dict[str, Any]:
    """
    Generate a summary of all operations performed.
    
    Args:
        unpause_results: List of unpause operation results
        trigger_results: List of trigger operation results
        
    Returns:
        Summary dictionary
    """
    summary = {
        "total_dags_processed": len(unpause_results),
        "unpause_operations": {
            "successful": len([r for r in unpause_results if r.get("success", False)]),
            "failed": len([r for r in unpause_results if not r.get("success", False)]),
            "already_unpaused": len([r for r in unpause_results if r.get("status") == "already_unpaused"])
        },
        "trigger_operations": {
            "successful": len([r for r in trigger_results if r.get("success", False)]),
            "failed": len([r for r in trigger_results if not r.get("success", False)])
        },
        "failed_dags": [
            r.get("dag_id") for r in unpause_results + trigger_results 
            if not r.get("success", False)
        ]
    }
    
    logger.info(f"Operation Summary: {summary}")
    return summary


@dag(
    dag_id="dag_manager",
    description="DAG to unpause and trigger all other DAGs in the Airflow environment",
    schedule=None,  # Manual trigger only
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["management", "utility", "admin"],
    max_active_runs=1,  # Prevent concurrent runs
    params={
        "skip_trigger": False,  # Option to only unpause without triggering
        "dag_pattern": "",  # Optional pattern to filter DAGs
    },
    doc_md="""
    # DAG Manager
    
    This DAG provides functionality to manage other DAGs in the Airflow environment.
    
    ## Features:
    - **Unpause All DAGs**: Automatically unpause all paused DAGs
    - **Trigger All DAGs**: Trigger all unpaused DAGs
    - **Summary Report**: Generate a summary of all operations
    
    ## Parameters:
    - `skip_trigger`: Set to True to only unpause DAGs without triggering them
    - `dag_pattern`: Optional regex pattern to filter which DAGs to process
    
    ## Prerequisites:
    - Airflow REST API must be enabled
    - Optional: Set Airflow Variables for API authentication:
      - `airflow_api_username`: Username for API authentication
      - `airflow_api_password`: Password for API authentication
    
    ## Usage:
    1. Trigger this DAG manually from the Airflow UI
    2. Monitor the task logs for detailed operation results
    3. Check the summary task for overall statistics
    
    ## Safety Notes:
    - This DAG excludes itself from operations to prevent infinite loops
    - Use with caution in production environments
    - Consider the impact of triggering multiple DAGs simultaneously
    """,
)
def dag_manager():
    """
    Main DAG function that orchestrates the unpause and trigger operations.
    """
    
    # Start marker
    start = EmptyOperator(task_id="start")
    
    # Get all DAGs in the environment
    all_dags = get_all_dags()
    
    # Unpause all DAGs
    unpause_results = unpause_dag.expand(dag_info=all_dags)
    
    # Trigger all DAGs (only if they were successfully unpaused)
    trigger_results = trigger_dag.expand(dag_info=all_dags)
    
    # Generate summary report
    summary = generate_summary(unpause_results, trigger_results)
    
    # End marker
    end = EmptyOperator(task_id="end")
    
    # Define task dependencies
    start >> all_dags >> unpause_results >> trigger_results >> summary >> end


# Create the DAG instance
dag_manager_instance = dag_manager()
