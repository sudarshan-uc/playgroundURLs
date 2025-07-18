#!/usr/bin/env python3
"""
Dynamic DAG Generator Management Utility

This script helps manage the dynamic DAG generation for Airflow benchmarking.
It provides utilities to:
1. Configure the number of DAGs to generate
2. Validate the generated DAGs
3. Generate statistics about the DAG structure
"""

import sys
import argparse
import json
from pathlib import Path


def update_dag_count(num_dags: int) -> None:
    """Update the number of DAGs to generate in the dynamic_dag_generator.py file."""
    dag_file = Path(__file__).parent / "dynamic_dag_generator.py"
    
    if not dag_file.exists():
        print(f"Error: {dag_file} not found!")
        sys.exit(1)
    
    # Read the file
    with open(dag_file, 'r') as f:
        content = f.read()
    
    # Replace the num_dags value
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if "'num_dags':" in line and "DAG_CONFIGS" in content[:content.find(line)]:
            # Extract indentation
            indent = len(line) - len(line.lstrip())
            lines[i] = f"{' ' * indent}'num_dags': {num_dags},  # Updated by management utility"
            break
    
    # Write back to file
    with open(dag_file, 'w') as f:
        f.write('\n'.join(lines))
    
    print(f"Updated DAG count to {num_dags} in {dag_file}")


def generate_dag_statistics() -> dict:
    """Generate statistics about the current DAG configuration."""
    dag_file = Path(__file__).parent / "dynamic_dag_generator.py"
    
    if not dag_file.exists():
        print(f"Error: {dag_file} not found!")
        return {}
    
    # Read configuration from the file
    with open(dag_file, 'r') as f:
        content = f.read()
    
    # Extract DAG_CONFIGS (simple parsing)
    config_start = content.find("DAG_CONFIGS = {")
    if config_start == -1:
        print("Error: Could not find DAG_CONFIGS in the file!")
        return {}
    
    config_end = content.find("}", config_start)
    config_section = content[config_start:config_end + 1]
    
    # Extract values manually (simple approach)
    stats = {}
    lines = config_section.split('\n')
    for line in lines:
        if "'num_dags':" in line:
            stats['num_dags'] = int(line.split(':')[1].split(',')[0].strip())
        elif "'min_tasks_per_dag':" in line:
            stats['min_tasks_per_dag'] = int(line.split(':')[1].split(',')[0].strip())
        elif "'max_tasks_per_dag':" in line:
            stats['max_tasks_per_dag'] = int(line.split(':')[1].split(',')[0].strip())
        elif "'min_execution_time':" in line:
            stats['min_execution_time'] = int(line.split(':')[1].split(',')[0].strip())
        elif "'max_execution_time':" in line:
            stats['max_execution_time'] = int(line.split(':')[1].split(',')[0].strip())
        elif "'max_sequential_depth':" in line:
            stats['max_sequential_depth'] = int(line.split(':')[1].split(',')[0].strip())
    
    # Calculate additional statistics
    if 'num_dags' in stats and 'min_tasks_per_dag' in stats and 'max_tasks_per_dag' in stats:
        avg_tasks_per_dag = (stats['min_tasks_per_dag'] + stats['max_tasks_per_dag']) / 2
        stats['estimated_total_tasks'] = int(stats['num_dags'] * avg_tasks_per_dag)
        
        # Estimate total execution time (in hours)
        avg_execution_time = (stats['min_execution_time'] + stats['max_execution_time']) / 2
        total_execution_seconds = stats['estimated_total_tasks'] * avg_execution_time
        stats['estimated_total_execution_hours'] = round(total_execution_seconds / 3600, 2)
    
    return stats


def print_statistics() -> None:
    """Print current DAG generation statistics."""
    stats = generate_dag_statistics()
    
    if not stats:
        return
    
    print("\n=== Dynamic DAG Generator Statistics ===")
    print(f"Number of DAGs: {stats.get('num_dags', 'Unknown')}")
    print(f"Tasks per DAG: {stats.get('min_tasks_per_dag', '?')}-{stats.get('max_tasks_per_dag', '?')}")
    print(f"Task execution time: {stats.get('min_execution_time', '?')}-{stats.get('max_execution_time', '?')} seconds")
    print(f"Max sequential depth: {stats.get('max_sequential_depth', 'Unknown')}")
    print(f"Estimated total tasks: {stats.get('estimated_total_tasks', 'Unknown')}")
    print(f"Estimated total execution time: {stats.get('estimated_total_execution_hours', 'Unknown')} hours")
    print("=" * 45)


def validate_configuration() -> bool:
    """Validate the current DAG configuration."""
    stats = generate_dag_statistics()
    
    if not stats:
        print("Error: Could not read configuration!")
        return False
    
    errors = []
    warnings = []
    
    # Validate required fields
    required_fields = ['num_dags', 'min_tasks_per_dag', 'max_tasks_per_dag', 
                      'min_execution_time', 'max_execution_time', 'max_sequential_depth']
    
    for field in required_fields:
        if field not in stats:
            errors.append(f"Missing required field: {field}")
    
    # Validate ranges
    if 'min_tasks_per_dag' in stats and 'max_tasks_per_dag' in stats:
        if stats['min_tasks_per_dag'] >= stats['max_tasks_per_dag']:
            errors.append("min_tasks_per_dag should be less than max_tasks_per_dag")
    
    if 'min_execution_time' in stats and 'max_execution_time' in stats:
        if stats['min_execution_time'] >= stats['max_execution_time']:
            errors.append("min_execution_time should be less than max_execution_time")
    
    # Validate reasonable values
    if stats.get('num_dags', 0) > 2000:
        warnings.append("Number of DAGs is very high (>2000), this may cause performance issues")
    
    if stats.get('max_tasks_per_dag', 0) > 100:
        warnings.append("Max tasks per DAG is very high (>100), this may cause memory issues")
    
    if stats.get('estimated_total_tasks', 0) > 50000:
        warnings.append("Estimated total tasks is very high (>50,000), this may cause system overload")
    
    # Print results
    if errors:
        print("Configuration Errors:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    if warnings:
        print("Configuration Warnings:")
        for warning in warnings:
            print(f"  - {warning}")
    
    if not errors and not warnings:
        print("Configuration is valid!")
    
    return True


def main():
    """Main function for the DAG management utility."""
    parser = argparse.ArgumentParser(description="Dynamic DAG Generator Management Utility")
    parser.add_argument("--set-count", type=int, metavar="N", 
                       help="Set the number of DAGs to generate (500, 1000, or 1500)")
    parser.add_argument("--stats", action="store_true", 
                       help="Show current DAG generation statistics")
    parser.add_argument("--validate", action="store_true", 
                       help="Validate the current configuration")
    parser.add_argument("--export-stats", type=str, metavar="FILE", 
                       help="Export statistics to a JSON file")
    
    args = parser.parse_args()
    
    if args.set_count:
        if args.set_count not in [500, 1000, 1500]:
            print("Warning: Recommended values are 500, 1000, or 1500 DAGs")
        update_dag_count(args.set_count)
    
    if args.stats:
        print_statistics()
    
    if args.validate:
        is_valid = validate_configuration()
        if not is_valid:
            sys.exit(1)
    
    if args.export_stats:
        stats = generate_dag_statistics()
        with open(args.export_stats, 'w') as f:
            json.dump(stats, f, indent=2)
        print(f"Statistics exported to {args.export_stats}")
    
    # If no arguments provided, show help
    if not any([args.set_count, args.stats, args.validate, args.export_stats]):
        parser.print_help()


if __name__ == "__main__":
    main()
