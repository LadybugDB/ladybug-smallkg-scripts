#!/usr/bin/env python3
"""
Create and upload the small-kgs dataset to Hugging Face with three versions:
- graph-std: Standard graph format
- duckdb: DuckDB database format
- lbdb: LadybugDB format
"""

import os
import subprocess
from huggingface_hub import HfApi, create_repo

# Configuration
ORG_NAME = "ladybugdb"
DATASET_NAME = "small-kg"
REPO_ID = f"{ORG_NAME}/{DATASET_NAME}"
LBBDB_FILE = "/tmp/local/kg_history.lbdb"

def check_huggingface_login():
    """Check if user is logged in to Hugging Face"""
    try:
        api = HfApi()
        whoami = api.whoami()
        print(f"✓ Logged in as: {whoami['name']}")
        return api
    except Exception as e:
        print(f"✗ Not logged in to Hugging Face: {e}")
        print("Please run: huggingface-cli login")
        return None

def create_dataset_repo(api):
    """Create the dataset repository"""
    try:
        create_repo(
            REPO_ID, 
            repo_type="dataset", 
            exist_ok=True,
            private=False
        )
        print(f"✓ Dataset repository created: {REPO_ID}")
    except Exception as e:
        print(f"✗ Error creating repository: {e}")
        return False
    return True

def create_dataset_card():
    """Create the dataset README.md with proper configuration"""
    
    card_content = f"""---
language:
- en
license: mit
library_name: ladybug
pretty_name: Small Knowledge Graphs
size_categories:
- n<1K
annotations_creators:
- no-annotation
source_datasets:
- original
task_categories:
- graph-analysis
task_ids:
- link-prediction
- node-classification
- knowledge-graph-completion
configs:
- graph-std
- duckdb
- lbdb
dataset_info:
- config_name: graph-std
  features:
  - name: edges
    dtype: list
  - name: nodes
    dtype: list
  - name: metadata
    dtype: dict
  splits:
  - name: train
    num_examples: 1
  download_size: 0
  dataset_size: 0
- config_name: duckdb
  features:
  - name: database
    dtype: binary
  splits:
  - name: train
    num_examples: 1
  download_size: 0
  dataset_size: 0
- config_name: lbdb
  features:
  - name: database
    dtype: binary
  splits:
  - name: train
    num_examples: 1
  download_size: 0
  dataset_size: 0
---

# Small Knowledge Graphs (small-kgs)

A collection of small knowledge graphs in multiple formats for graph ML research and development.

## Dataset Structure

This dataset contains knowledge graphs in three formats:

### graph-std
Standard graph format with edges and nodes as structured data.

### duckdb
DuckDB database format for efficient analytical queries.

### lbdb
LadybugDB format for graph database operations.

## Usage

### Load graph-std version
```python
from datasets import load_dataset
dataset = load_dataset("{ORG_NAME}/{DATASET_NAME}", name="graph-std")
```

### Load duckdb version
```python
from datasets import load_dataset
dataset = load_dataset("{ORG_NAME}/{DATASET_NAME}", name="duckdb")
```

### Load lbdb version
```python
from datasets import load_dataset
dataset = load_dataset("{ORG_NAME}/{DATASET_NAME}", name="lbdb")
```

## Dataset Contents

- **Knowledge Graph**: {os.path.basename(LBBDB_FILE)}
"""
    
    return card_content

def prepare_data_files():
    """Prepare data files for each version"""
    
    data_dir = "/tmp/local/small-kgs-data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Create version directories
    for version in ["graph-std", "duckdb", "lbdb"]:
        version_dir = os.path.join(data_dir, version)
        os.makedirs(version_dir, exist_ok=True)
    
    # Copy lbdb file
    lbdb_dest = os.path.join(data_dir, "lbdb", "kg_history.lbdb")
    import shutil
    shutil.copy2(LBBDB_FILE, lbdb_dest)
    print(f"✓ Copied lbdb file to {lbdb_dest}")
    
    return data_dir

def upload_files(api, data_dir):
    """Upload files to the repository"""
    
    # Upload dataset card
    card_path = "/tmp/local/small-kgs_README.md"
    with open(card_path, 'w') as f:
        f.write(create_dataset_card())
    
    api.upload_file(
        path_or_fileobj=card_path,
        path_in_repo="README.md",
        repo_id=REPO_ID,
        repo_type="dataset"
    )
    print("✓ Uploaded dataset card")
    
    # Upload each version
    for version in ["lbdb"]:  # Start with lbdb, others will be converted
        version_dir = os.path.join(data_dir, version)
        if os.path.exists(version_dir):
            api.upload_folder(
                folder_path=version_dir,
                repo_id=REPO_ID,
                repo_type="dataset",
                commit_message=f"Add {version} version"
            )
            print(f"✓ Uploaded {version} version")

def main():
    print("Creating small-kgs dataset for ladybugdb...")
    
    # Check login
    api = check_huggingface_login()
    if not api:
        return
    
    # Create repository
    if not create_dataset_repo(api):
        return
    
    # Prepare data files
    data_dir = prepare_data_files()
    
    # Upload files
    upload_files(api, data_dir)
    
    print(f"\n✓ Dataset created successfully!")
    print(f"  Repository: https://huggingface.co/datasets/{REPO_ID}")
    print(f"  Available versions: graph-std, duckdb, lbdb")

if __name__ == "__main__":
    main()
