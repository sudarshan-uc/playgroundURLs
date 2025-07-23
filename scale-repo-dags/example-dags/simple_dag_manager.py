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
Simple DAG Manager using Airflow CLI commands.
This DAG can unpause and trigger all other DAGs using bash commands.
"""

from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# DAG definition
with DAG(
    dag_id="simple_dag_manager",
    description="Simple DAG to unpause and trigger all other DAGs using CLI commands",
    schedule=None,  # Manual trigger only
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=30),
    tags=["management", "utility", "admin", "simple"],
    max_active_runs=1,
    params={
        "exclude_patterns": "simple_dag_manager,dag_manager",  # DAGs to exclude (comma-separated)
    },
    doc_md="""
    # Simple DAG Manager
    
    This DAG provides a simple way to manage other DAGs using Airflow CLI commands.
    
    ## Features:
    - **List All DAGs**: Show all DAGs in the environment
    - **Unpause All DAGs**: Automatically unpause all paused DAGs (except self)
    - **Trigger All DAGs**: Trigger all unpaused DAGs (except self)
    
    ## Usage:
    1. Trigger this DAG manually from the Airflow UI
    2. Monitor the task logs for operation results
    
    ## Safety Notes:
    - This DAG excludes itself and the main dag_manager to prevent loops
    - Modify the exclude_patterns parameter to exclude specific DAGs
    - Use with caution in production environments
    """,
) as dag:
    
    start = EmptyOperator(
        task_id="start",
    )
    
    # List all DAGs for reference
    list_all_dags = BashOperator(
        task_id="list_all_dags",
        bash_command="""
        echo "=== Listing all DAGs in the environment ==="
        airflow dags list
        echo ""
        echo "=== Paused DAGs ==="
        airflow dags list --only-paused
        echo ""
        echo "=== Active DAGs ==="
        airflow dags list --only-active
        """,
    )
    
    # Unpause all DAGs except this one and other manager DAGs
    unpause_all_dags = BashOperator(
        task_id="unpause_all_dags",
        bash_command="""
        echo "=== Unpausing all DAGs except management DAGs ==="
        
        # Get list of all paused DAGs
        PAUSED_DAGS=$(airflow dags list --only-paused --output table | grep -E '^[[:space:]]*[^|[:space:]]' | awk '{print $1}' | grep -v 'dag_id' | head -n -1)
        
        # Exclude patterns from params
        EXCLUDE_PATTERNS="{{ params.exclude_patterns }}"
        
        echo "Paused DAGs found:"
        echo "$PAUSED_DAGS"
        echo ""
        
        if [ -z "$PAUSED_DAGS" ]; then
            echo "No paused DAGs found."
        else
            for dag_id in $PAUSED_DAGS; do
                # Check if DAG should be excluded
                SHOULD_EXCLUDE=false
                for pattern in $(echo $EXCLUDE_PATTERNS | tr ',' ' '); do
                    if [[ "$dag_id" == *"$pattern"* ]]; then
                        echo "Skipping $dag_id (matches exclude pattern: $pattern)"
                        SHOULD_EXCLUDE=true
                        break
                    fi
                done
                
                if [ "$SHOULD_EXCLUDE" = false ]; then
                    echo "Unpausing DAG: $dag_id"
                    airflow dags unpause $dag_id
                    if [ $? -eq 0 ]; then
                        echo "✓ Successfully unpaused $dag_id"
                    else
                        echo "✗ Failed to unpause $dag_id"
                    fi
                else
                    echo "- Skipped $dag_id"
                fi
                echo ""
            done
        fi
        
        echo "=== Unpause operation completed ==="
        """,
    )
    
    # Small delay to allow DAGs to register as unpaused
    wait_for_unpause = BashOperator(
        task_id="wait_for_unpause",
        bash_command="echo 'Waiting for DAGs to register as unpaused...' && sleep 5",
    )
    
    # Trigger all active (unpaused) DAGs except this one
    trigger_all_dags = BashOperator(
        task_id="trigger_all_dags",
        bash_command="""
        echo "=== Triggering all active DAGs except management DAGs ==="
        
        # Get list of all active (unpaused) DAGs
        ACTIVE_DAGS=$(airflow dags list --only-active --output table | grep -E '^[[:space:]]*[^|[:space:]]' | awk '{print $1}' | grep -v 'dag_id' | head -n -1)
        
        # Exclude patterns from params
        EXCLUDE_PATTERNS="{{ params.exclude_patterns }}"
        
        echo "Active DAGs found:"
        echo "$ACTIVE_DAGS"
        echo ""
        
        if [ -z "$ACTIVE_DAGS" ]; then
            echo "No active DAGs found."
        else
            TRIGGER_COUNT=0
            for dag_id in $ACTIVE_DAGS; do
                # Check if DAG should be excluded
                SHOULD_EXCLUDE=false
                for pattern in $(echo $EXCLUDE_PATTERNS | tr ',' ' '); do
                    if [[ "$dag_id" == *"$pattern"* ]]; then
                        echo "Skipping $dag_id (matches exclude pattern: $pattern)"
                        SHOULD_EXCLUDE=true
                        break
                    fi
                done
                
                if [ "$SHOULD_EXCLUDE" = false ]; then
                    echo "Triggering DAG: $dag_id"
                    TRIGGER_TIME=$(date +"%Y%m%d_%H%M%S")
                    airflow dags trigger $dag_id --run-id "bulk_trigger_$TRIGGER_TIME"
                    if [ $? -eq 0 ]; then
                        echo "✓ Successfully triggered $dag_id"
                        TRIGGER_COUNT=$((TRIGGER_COUNT + 1))
                    else
                        echo "✗ Failed to trigger $dag_id"
                    fi
                else
                    echo "- Skipped $dag_id"
                fi
                echo ""
            done
            
            echo "=== Triggered $TRIGGER_COUNT DAGs total ==="
        fi
        
        echo "=== Trigger operation completed ==="
        """,
    )
    
    # Generate a final summary
    generate_summary = BashOperator(
        task_id="generate_summary",
        bash_command="""
        echo "=== FINAL SUMMARY ==="
        echo "Timestamp: $(date)"
        echo ""
        echo "Current DAG Status:"
        airflow dags list --output table
        echo ""
        echo "Recent DAG Runs (last 10):"
        airflow dags list-runs --limit 10 --output table
        echo ""
        echo "=== Operation completed successfully ==="
        """,
    )
    
    end = EmptyOperator(
        task_id="end",
    )
    
    # Define task dependencies
    start >> list_all_dags >> unpause_all_dags >> wait_for_unpause >> trigger_all_dags >> generate_summary >> end
