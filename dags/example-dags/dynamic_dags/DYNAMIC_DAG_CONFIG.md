# Dynamic DAG Generator Configuration

This configuration allows you to adjust the dynamic DAG generation parameters.

## Usage

To change the number of DAGs generated, edit the `num_dags` parameter in the `dynamic_dag_generator.py` file:

```python
DAG_CONFIGS = {
    'num_dags': 500,  # Change this to 500, 1000, or 1500
    'min_tasks_per_dag': 2,
    'max_tasks_per_dag': 50,
    'min_execution_time': 5,  # seconds
    'max_execution_time': 300,  # 5 minutes
    'max_sequential_depth': 4,
}
```

## Parameters

- **num_dags**: Total number of DAGs to generate (500, 1000, or 1500)
- **min_tasks_per_dag**: Minimum number of tasks per DAG (default: 2)
- **max_tasks_per_dag**: Maximum number of tasks per DAG (default: 50)
- **min_execution_time**: Minimum task execution time in seconds (default: 5)
- **max_execution_time**: Maximum task execution time in seconds (default: 300)
- **max_sequential_depth**: Maximum number of sequential dependencies (default: 4)

## Generated DAG Features

Each generated DAG will have:

1. **Random number of tasks** (2-50 per DAG)
2. **Variable dependencies** (1-4 sequential task dependencies)
3. **Different mathematical operations**:
   - Fibonacci calculations
   - Prime factorization
   - Matrix multiplication
   - Monte Carlo Pi estimation
   - Sorting algorithms
   - Hash computations
   - Factorial calculations
   - Power calculations
   - Trigonometric series
   - Polynomial evaluations

4. **Varied DAG settings**:
   - Different schedule intervals
   - Different catchup settings
   - Different max_active_runs
   - Different concurrency limits
   - Different default_args configurations

## Performance Notes

- **500 DAGs**: Light load, suitable for basic testing
- **1000 DAGs**: Medium load, good for performance testing
- **1500 DAGs**: Heavy load, for stress testing

Each task is designed to consume CPU time between 5 seconds and 5 minutes randomly to simulate real-world workloads.
