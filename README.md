Use these scripts to upload knowlege graphs to 

https://huggingface.co/datasets/ladybugdb/small-kgs

Sample cmdlines:

uv run create_duckdb.py --input-db ldbc_history/lbdb/ldbc_history.lbdb --output ldbc_history/duckdb/ldbc_history.duckdb
uv run convert_csr.py --source-db ~/src/ladybug-smallkg-scripts/ldbc_history/duckdb/ldbc_history.duckdb --output-db  ~/src/ladybug-smallkg-scripts/ldbc_history/graph-std.duckdb --storage graph-std/ldbc --csr-table ldbc
