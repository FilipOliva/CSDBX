#!/usr/bin/env python3
"""
Databricks Workflow Generator Script
Creates a workflow from all notebooks in a specified folder

Usage:
1. Modify the configuration variables below
2. Run the script in Databricks notebook

Requirements:
pip install databricks-sdk
"""

import sys
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, JobSettings
import os

# ============================================================================
# CONFIGURATION - MODIFY THESE VALUES
# ============================================================================

workflow_name = "ADS_RDS2"
folder_name = "ADS"
base_path = "/Workspace/Users/filip.oliva1@ext.csas.cz"

# ============================================================================


def get_notebooks_from_folder(w: WorkspaceClient, folder_path: str):
    """
    Get all notebook files from the specified folder
    
    Args:
        w: WorkspaceClient instance
        folder_path: Path to the folder containing notebooks
        
    Returns:
        List of notebook paths
    """
    try:
        notebooks = []
        items = w.workspace.list(folder_path)
        
        for item in items:
            if item.object_type.name == 'NOTEBOOK':
                notebooks.append(item.path)
                print(f"Found notebook: {item.path}")
            elif item.object_type.name == 'DIRECTORY':
                # Recursively search subdirectories if needed
                sub_notebooks = get_notebooks_from_folder(w, item.path)
                notebooks.extend(sub_notebooks)
                
        return notebooks
        
    except Exception as e:
        print(f"Error accessing folder {folder_path}: {e}")
        return []


def create_workflow_tasks(notebooks: list):
    """
    Create workflow tasks from notebook paths
    
    Args:
        notebooks: List of notebook paths
        
    Returns:
        List of Task objects
    """
    tasks = []
    
    for i, notebook_path in enumerate(notebooks):
        # Extract notebook name from path for task key
        notebook_name = os.path.basename(notebook_path)
        # Clean the name for use as task key (remove special characters)
        task_key = notebook_name.replace('.', '_').replace(' ', '_').replace('-', '_')
        
        task = Task(
            task_key=f"task_{i+1:03d}_{task_key}",
            description=f"Execute notebook: {notebook_name}",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                base_parameters={}  # Add default parameters if needed
            )
        )
        
        tasks.append(task)
        print(f"Created task: {task.task_key} -> {notebook_path}")
    
    return tasks


def create_workflow(workflow_name: str, folder_name: str, base_path: str):
    """
    Main function to create the workflow
    
    Args:
        workflow_name: Name of the workflow to create
        folder_name: Name of the folder containing notebooks
        base_path: Base path for the user workspace
    """
    
    # Initialize Databricks client
    try:
        w = WorkspaceClient()
        print("✓ Connected to Databricks workspace")
    except Exception as e:
        print(f"✗ Failed to connect to Databricks: {e}")
        return
    
    # Construct full folder path
    folder_path = f"{base_path}/{folder_name}"
    print(f"Scanning folder: {folder_path}")
    
    # Get all notebooks from the folder
    notebooks = get_notebooks_from_folder(w, folder_path)
    
    if not notebooks:
        print(f"✗ No notebooks found in folder: {folder_path}")
        return
    
    print(f"✓ Found {len(notebooks)} notebook(s)")
    
    # Create workflow tasks
    tasks = create_workflow_tasks(notebooks)
    
    if not tasks:
        print("✗ No tasks created")
        return
    
    # Create the workflow
    try:
        print(f"Creating workflow: {workflow_name}")
        job = w.jobs.create(
            name=workflow_name,
            tasks=tasks,
            max_concurrent_runs=1,
            timeout_seconds=3600,  # 1 hour timeout
            tags={
                "created_by": "Create_MAP_Worflow.py",
                "source_folder": folder_name
            }
        )
        
        print(f"✓ Workflow created successfully!")
        print(f"  - Workflow ID: {job.job_id}")
        print(f"  - Workflow Name: {workflow_name}")
        print(f"  - Tasks: {len(tasks)}")
        print(f"  - Source Folder: {folder_path}")
        
        return job.job_id
        
    except Exception as e:
        print(f"✗ Failed to create workflow: {e}")
        return


# ============================================================================
# EXECUTE THE SCRIPT
# ============================================================================

print("=== Databricks Workflow Generator ===")
print(f"Workflow Name: {workflow_name}")
print(f"Folder: {folder_name}")
print(f"Base Path: {base_path}")
print("=" * 40)

create_workflow(workflow_name, folder_name, base_path)