Use these scripts to upload knowlege graphs to 

https://huggingface.co/datasets/ladybugdb/small-kgs

Sample cmdlines:

```
# lbdb -> duckdb
uv run create_duckdb.py \
   --input-db ldbc_history/lbdb/ldbc_history.lbdb \
   --output ldbc_history/duckdb/ldbc_history.duckdb
# duckdb -> csr duckdb -> csr graph-std
uv run convert_csr.py \
  --source-db ldbc_history/duckdb/ldbc_history.duckdb \
  --output-db ldbc_history/graph-std.duckdb \
  --storage graph-std/ldbc \
  --csr-table ldbc
# cleanup csr duckdb
rm ldbc_history/graph-std.duckdb
# upload
uv run create_small_kgs_dataset.py --input-dir ldbc_history
```
