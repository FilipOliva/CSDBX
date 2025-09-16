#!/usr/bin/env python3
"""
Databricks Workflow Tasks Lister Script
Lists all tasks in a specified Databricks workflow

Usage:
1. Modify the configuration variables below
2. Run the script in Databricks notebook

Requirements:
pip install databricks-sdk
"""

import sys
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Job
import os

# ============================================================================
# CONFIGURATION - MODIFY THESE VALUES
# ============================================================================

# Option 1: Search by workflow name
workflow_name = "ADS_RDS"

# Option 2: Use specific Job ID (if known)
job_id = None  # Set to specific job ID like 12345, or None to search by name

# ============================================================================


def find_workflow_by_name(w: WorkspaceClient, name: str):
    """
    Find workflow by name
    
    Args:
        w: WorkspaceClient instance
        name: Name of the workflow to find
        
    Returns:
        Job object if found, None otherwise
    """
    try:
        print(f"Searching for workflow: {name}")
        
        # List all jobs
        jobs = w.jobs.list()
        
        for job in jobs:
            if job.settings and job.settings.name == name:
                print(f"✓ Found workflow: {name} (ID: {job.job_id})")
                return job
                
        print(f"✗ Workflow '{name}' not found")
        return None
        
    except Exception as e:
        print(f"✗ Error searching for workflow: {e}")
        return None


def get_workflow_details(w: WorkspaceClient, job_id: int):
    """
    Get detailed workflow information
    
    Args:
        w: WorkspaceClient instance
        job_id: Job ID of the workflow
        
    Returns:
        Job details
    """
    try:
        job_details = w.jobs.get(job_id)
        return job_details
        
    except Exception as e:
        print(f"✗ Error getting workflow details: {e}")
        return None


def list_workflow_tasks(job_details):
    """
    List all tasks in the workflow
    
    Args:
        job_details: Job details object
    """
    if not job_details or not job_details.settings:
        print("✗ No workflow details available")
        return
    
    settings = job_details.settings
    tasks = settings.tasks or []
    
    print(f"\n=== Workflow Details ===")
    print(f"Name: {settings.name}")
    print(f"Job ID: {job_details.job_id}")
    print(f"Created: {job_details.created_time}")
    print(f"Creator: {job_details.creator_user_name}")
    print(f"Max Concurrent Runs: {settings.max_concurrent_runs}")
    print(f"Timeout: {settings.timeout_seconds} seconds")
    
    if settings.tags:
        print(f"Tags: {dict(settings.tags)}")
    
    print(f"\n=== Tasks ({len(tasks)}) ===")
    
    if not tasks:
        print("No tasks found in this workflow")
        return
    
    # Sort tasks by task_key for better readability
    sorted_tasks = sorted(tasks, key=lambda x: x.task_key)
    
    for i, task in enumerate(sorted_tasks, 1):
        print(f"\n{i:2d}. Task: {task.task_key}")
        print(f"    Description: {task.description or 'No description'}")
        
        # Determine task type and details
        if task.notebook_task:
            print(f"    Type: Notebook")
            print(f"    Path: {task.notebook_task.notebook_path}")
            if task.notebook_task.base_parameters:
                print(f"    Parameters: {dict(task.notebook_task.base_parameters)}")
                
        elif task.spark_python_task:
            print(f"    Type: Python Script")
            print(f"    File: {task.spark_python_task.python_file}")
            if task.spark_python_task.parameters:
                print(f"    Parameters: {task.spark_python_task.parameters}")
                
        elif task.sql_task:
            print(f"    Type: SQL")
            if hasattr(task.sql_task, 'query') and task.sql_task.query:
                print(f"    Query: {task.sql_task.query.query_id if task.sql_task.query else 'N/A'}")
                
        elif task.python_wheel_task:
            print(f"    Type: Python Wheel")
            print(f"    Package: {task.python_wheel_task.package_name}")
            print(f"    Entry Point: {task.python_wheel_task.entry_point}")
            
        elif task.jar_task:
            print(f"    Type: JAR")
            print(f"    Main Class: {task.jar_task.main_class_name}")
            
        else:
            print(f"    Type: Unknown")
        
        # Show dependencies
        if task.depends_on:
            deps = [dep.task_key for dep in task.depends_on]
            print(f"    Depends on: {', '.join(deps)}")
        
        # Show cluster configuration
        if task.existing_cluster_id:
            print(f"    Cluster: {task.existing_cluster_id}")
        elif task.new_cluster:
            print(f"    New Cluster: {task.new_cluster.spark_version}")
        
        # Show libraries
        if task.libraries:
            lib_types = []
            for lib in task.libraries:
                if lib.pypi:
                    lib_types.append(f"PyPI: {lib.pypi.package}")
                elif lib.maven:
                    lib_types.append(f"Maven: {lib.maven.coordinates}")
                elif lib.jar:
                    lib_types.append(f"JAR: {lib.jar}")
            if lib_types:
                print(f"    Libraries: {', '.join(lib_types)}")


def get_workflow_runs(w: WorkspaceClient, job_id: int, limit: int = 5):
    """
    Get recent workflow runs
    
    Args:
        w: WorkspaceClient instance
        job_id: Job ID of the workflow
        limit: Number of recent runs to show
    """
    try:
        print(f"\n=== Recent Runs (Last {limit}) ===")
        
        runs = w.jobs.list_runs(job_id=job_id, limit=limit)
        
        if not runs:
            print("No runs found")
            return
        
        from datetime import datetime
        
        for i, run in enumerate(runs, 1):
            status = run.state.life_cycle_state if run.state else "Unknown"
            result = run.state.result_state if run.state and run.state.result_state else "N/A"
            
            # Handle timestamp conversion - Databricks returns milliseconds since epoch
            if run.start_time:
                if isinstance(run.start_time, int):
                    start_time = datetime.fromtimestamp(run.start_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    start_time = run.start_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                start_time = "N/A"
            
            if run.end_time:
                if isinstance(run.end_time, int):
                    end_time = datetime.fromtimestamp(run.end_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    end_time = run.end_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                end_time = "Running"
            
            print(f"{i:2d}. Run ID: {run.run_id}")
            print(f"    Status: {status} / {result}")
            print(f"    Started: {start_time}")
            print(f"    Ended: {end_time}")
            
            # Calculate duration
            if run.start_time and run.end_time:
                if isinstance(run.start_time, int) and isinstance(run.end_time, int):
                    duration_ms = run.end_time - run.start_time
                    duration_seconds = duration_ms / 1000
                    hours = int(duration_seconds // 3600)
                    minutes = int((duration_seconds % 3600) // 60)
                    seconds = int(duration_seconds % 60)
                    print(f"    Duration: {hours:02d}:{minutes:02d}:{seconds:02d}")
                else:
                    duration = run.end_time - run.start_time
                    print(f"    Duration: {duration}")
            print()
            
    except Exception as e:
        print(f"✗ Error getting workflow runs: {e}")


def list_workflow_tasks_main(workflow_name: str = None, job_id: int = None):
    """
    Main function to list workflow tasks
    
    Args:
        workflow_name: Name of the workflow (optional)
        job_id: Job ID of the workflow (optional)
    """
    
    # Initialize Databricks client
    try:
        w = WorkspaceClient()
        print("✓ Connected to Databricks workspace")
    except Exception as e:
        print(f"✗ Failed to connect to Databricks: {e}")
        return
    
    # Find workflow
    if job_id:
        print(f"Using Job ID: {job_id}")
        job_details = get_workflow_details(w, job_id)
    elif workflow_name:
        job = find_workflow_by_name(w, workflow_name)
        if not job:
            return
        job_details = get_workflow_details(w, job.job_id)
    else:
        print("✗ Please provide either workflow_name or job_id")
        return
    
    if not job_details:
        return
    
    # List tasks
    list_workflow_tasks(job_details)
    
    # Show recent runs
    get_workflow_runs(w, job_details.job_id)


# ============================================================================
# EXECUTE THE SCRIPT
# ============================================================================

print("=== Databricks Workflow Tasks Lister ===")

if job_id:
    print(f"Job ID: {job_id}")
else:
    print(f"Workflow Name: {workflow_name}")

print("=" * 40)

list_workflow_tasks_main(workflow_name=workflow_name, job_id=job_id)