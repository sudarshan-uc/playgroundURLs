# Dynamic DAG Generator for Airflow Benchmarking

This `dynamic_dags` directory contains a dynamic DAG generator designed for benchmarking Airflow performance with varying workloads.

## Files

- **`dynamic_dag_generator.py`** - Main generator that creates 500, 1000, or 1500 DAGs
- **`dag_management_utility.py`** - Utility script for managing DAG generation parameters
- **`DYNAMIC_DAG_CONFIG.md`** - Configuration documentation
- **`README_DYNAMIC_DAGS.md`** - This documentation file

## Directory Structure
```
AIRFLOW_DEPLOYMENT/dags/dynamic_dags/
├── dynamic_dag_generator.py      # Main DAG generator
├── dag_management_utility.py     # Management utility
├── DYNAMIC_DAG_CONFIG.md         # Configuration docs
└── README_DYNAMIC_DAGS.md        # This README
```

## Quick Start

### 1. Generate 500 DAGs (Default)
The generator is pre-configured to create 500 DAGs. Simply ensure the file is in your Airflow DAGs folder and restart Airflow.

### 2. Change Number of DAGs
Use the management utility to change the number of generated DAGs:

```bash
# Navigate to the dynamic_dags directory
cd AIRFLOW_DEPLOYMENT/dags/dynamic_dags

# Set to 1000 DAGs
python dag_management_utility.py --set-count 1000

# Set to 1500 DAGs
python dag_management_utility.py --set-count 1500
```

### 3. View Current Configuration
```bash
cd AIRFLOW_DEPLOYMENT/dags/dynamic_dags
python dag_management_utility.py --stats
```

### 4. Validate Configuration
```bash
cd AIRFLOW_DEPLOYMENT/dags/dynamic_dags
python dag_management_utility.py --validate
```

## Generated DAG Characteristics

### DAG Structure
- **Number of DAGs**: 500, 1000, or 1500 (configurable)
- **Tasks per DAG**: 2-50 tasks (random)
- **Dependencies**: 1-4 sequential task dependencies per task
- **Execution Time**: 5 seconds to 5 minutes per task (random)

### Task Types
Each task performs one of these mathematical operations:
1. **Fibonacci Calculation** - Iterative computation
2. **Prime Factorization** - Number theory operations
3. **Matrix Multiplication** - Linear algebra computations
4. **Monte Carlo Pi Estimation** - Statistical simulation
5. **Sorting Algorithms** - Data structure operations
6. **Hash Computations** - Cryptographic operations
7. **Factorial Calculations** - Recursive mathematics
8. **Power Calculations** - Exponential operations
9. **Trigonometric Series** - Mathematical series
10. **Polynomial Evaluation** - Algebraic computations

### DAG Variations
Each DAG has randomized settings for realistic diversity:

- **Schedule Intervals**: `@once`, `@daily`, `@hourly`, `@weekly`, custom intervals, or manual trigger
- **Catchup Settings**: True/False randomly assigned
- **Max Active Runs**: 1, 2, 3, 5, 10, or 16
- **Concurrency**: 16, 32, 64, 128, or 256
- **Default Args**: Different owners, retry policies, and start dates

## Performance Estimates

| DAG Count | Avg Total Tasks | Est. Execution Time |
|-----------|----------------|-------------------|
| 500       | ~13,000        | ~54 hours        |
| 1000      | ~26,000        | ~108 hours       |
| 1500      | ~39,000        | ~162 hours       |

*Note: These are estimates assuming average values. Actual performance depends on system resources and Airflow configuration.*

## Usage Scenarios

### Light Testing (500 DAGs)
- Basic functionality testing
- Development environment validation
- Initial performance baseline

### Medium Load (1000 DAGs)
- Performance testing
- Scalability assessment
- Resource utilization analysis

### Heavy Load (1500 DAGs)
- Stress testing
- Maximum capacity evaluation
- System limit identification

## Monitoring and Analysis

The generated tasks return detailed execution information including:
- Operation type performed
- Target vs. actual execution time
- Task and DAG identifiers
- Performance metrics specific to each operation type

This data can be used for:
- Performance analysis
- Resource utilization tracking
- Execution time variance studies
- System bottleneck identification

## Safety Considerations

- **Resource Usage**: High task counts may consume significant CPU and memory
- **Database Load**: Many DAGs can stress the Airflow metadata database
- **Network Traffic**: Monitor Airflow web server and worker communication
- **Disk Space**: Ensure adequate space for logs and temporary files

## Customization

To modify the generator behavior, edit the `DAG_CONFIGS` dictionary in `dynamic_dag_generator.py`:

```python
DAG_CONFIGS = {
    'num_dags': 500,                    # Total DAGs to generate
    'min_tasks_per_dag': 2,             # Minimum tasks per DAG
    'max_tasks_per_dag': 50,            # Maximum tasks per DAG
    'min_execution_time': 5,            # Min task time (seconds)
    'max_execution_time': 300,          # Max task time (seconds)
    'max_sequential_depth': 4,          # Max dependency chain length
}
```

## Troubleshooting

### Common Issues
1. **Import Errors**: Ensure Airflow is properly installed and configured
2. **Memory Issues**: Reduce `num_dags` or `max_tasks_per_dag` for limited resources
3. **Database Overload**: Monitor Airflow metadata database performance
4. **Task Failures**: Check worker logs for specific task execution errors

### Performance Optimization
- Adjust `concurrency` settings based on available resources
- Monitor worker capacity and scale as needed
- Consider using Celery or Kubernetes executors for high loads
- Tune Airflow configuration parameters for your environment
