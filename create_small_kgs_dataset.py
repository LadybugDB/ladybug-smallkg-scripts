#!/usr/bin/env python3
"""
Create and upload a small-kgs dataset variant to Hugging Face.
Takes a directory containing 3 variants (graph-std, duckdb, lbdb) and uploads to
a subdir within the dataset repository.

Usage:
    python create_small_kgs_dataset.py --input-dir ldbc_history
"""

import os
import argparse
import shutil
from huggingface_hub import HfApi, create_repo


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create and upload a small-kgs dataset variant to Hugging Face"
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Path to directory containing 3 variants (graph-std, duckdb, lbdb)",
    )
    parser.add_argument(
        "--org-name", default="ladybugdb", help="Hugging Face organization name"
    )
    parser.add_argument(
        "--base-dataset-name",
        default="small-kgs",
        help="Base dataset name on Hugging Face",
    )
    parser.add_argument(
        "--variant-name",
        help="Variant name (defaults to input directory name)",
    )
    parser.add_argument(
        "--private", action="store_true", help="Make repository private"
    )
    return parser.parse_args()


def check_huggingface_login():
    """Check if user is logged in to Hugging Face"""
    try:
        api = HfApi()
        whoami = api.whoami()
        print(f"Logged in as: {whoami['name']}")
        return api
    except Exception as e:
        print(f"Not logged in to Hugging Face: {e}")
        print("Please run: huggingface-cli login")
        return None


def get_repo_id(org_name, base_dataset_name):
    """Get the full repository ID"""
    return f"{org_name}/{base_dataset_name}"


def check_repo_exists(api, repo_id):
    """Check if repository already exists"""
    try:
        info = api.repo_info(repo_id, repo_type="dataset")
        return True
    except Exception:
        return False


def create_dataset_repo(api, repo_id, private=False):
    """Create the dataset repository"""
    try:
        create_repo(repo_id, repo_type="dataset", exist_ok=True, private=private)
        print(f"Dataset repository created: {repo_id}")
    except Exception as e:
        print(f"Error creating repository: {e}")
        return False
    return True


def prepare_variant_data(input_dir):
    """Prepare data by moving variants to a subdir named after the input directory"""

    variant_name = os.path.basename(input_dir)
    temp_dir = f"/tmp/local/small-kgs-{variant_name}"
    os.makedirs(temp_dir, exist_ok=True)

    target_dir = os.path.join(temp_dir, variant_name)
    os.makedirs(target_dir, exist_ok=True)

    for variant in ["graph-std", "duckdb", "lbdb"]:
        src_path = os.path.join(input_dir, variant)
        dst_path = os.path.join(target_dir, variant)
        if os.path.exists(src_path):
            shutil.copytree(src_path, dst_path)
            print(f"Copied {variant} to {dst_path}")

    return temp_dir, variant_name


def upload_files(api, temp_dir, repo_id, variant_name, is_first_upload):
    """Upload files to the repository"""

    if is_first_upload:
        card_path = os.path.join(temp_dir, "README.md")
        with open(card_path, "w") as f:
            f.write(create_dataset_card(variant_name))

        api.upload_file(
            path_or_fileobj=card_path,
            path_in_repo="README.md",
            repo_id=repo_id,
            repo_type="dataset",
        )
        print("Uploaded dataset card")

    version_dir = os.path.join(temp_dir, variant_name)
    if os.path.exists(version_dir):
        api.upload_folder(
            folder_path=version_dir,
            repo_id=repo_id,
            repo_type="dataset",
            path_in_repo=variant_name,
            commit_message=f"Add {variant_name} variant",
        )
        print(f"Uploaded {variant_name} variant")


def create_dataset_card(variant_name):
    """Create the dataset README.md with proper configuration"""

    card_content = f"""---
language:
- en
license: mit
library_name: ladybug
pretty_name: Small Knowledge Graphs - {variant_name}
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
- {variant_name}
dataset_info:
- config_name: {variant_name}
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
---

# Small Knowledge Graphs - {variant_name}

A knowledge graph variant stored in multiple formats for graph ML research and development.

## Dataset Structure

This dataset contains knowledge graphs in three formats under the `{variant_name}/` directory:

### graph-std
Standard graph format with edges and nodes as structured data.

### duckdb
DuckDB database format for efficient analytical queries.

### lbdb
LadybugDB format for graph database operations.

## Usage

### Load this variant
```python
from datasets import load_dataset
dataset = load_dataset("ladybugdb/small-kgs", name="{variant_name}")
```

## Variant Contents

- **Variant Name**: {variant_name}
- **Storage Path**: {variant_name}/
"""

    return card_content


def main():
    args = parse_args()

    input_dir = os.path.abspath(args.input_dir)
    if not os.path.isdir(input_dir):
        print(f"Error: {input_dir} is not a valid directory")
        return

    org_name = args.org_name
    base_dataset_name = args.base_dataset_name
    variant_name = args.variant_name or os.path.basename(input_dir)
    repo_id = get_repo_id(org_name, base_dataset_name)

    print(f"Creating small-kgs dataset variant: {variant_name}")
    print(f"Input directory: {input_dir}")
    print(f"Target repository: {repo_id}")

    api = check_huggingface_login()
    if not api:
        return

    repo_exists = check_repo_exists(api, repo_id)

    if not create_dataset_repo(api, repo_id, private=args.private):
        return

    temp_dir, variant_name = prepare_variant_data(input_dir)

    upload_files(
        api,
        temp_dir,
        repo_id,
        variant_name,
        is_first_upload=not repo_exists,
    )

    print(f"\nDataset updated successfully!")
    print(f"  Repository: https://huggingface.co/datasets/{repo_id}")
    print(f"  Variant: {variant_name}")
    print(f"  Path: {variant_name}/")


if __name__ == "__main__":
    main()
