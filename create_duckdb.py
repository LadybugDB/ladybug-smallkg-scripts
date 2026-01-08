#!/usr/bin/env python3
"""Create DuckDB from parquet files according to schema.cypher."""

import re
from pathlib import Path
import duckdb

def parse_schema_cypher(schema_path: Path) -> dict:
    """Parse schema.cypher to extract node and edge table definitions."""
    content = schema_path.read_text()
    
    node_tables = {}
    edge_tables = {}
    
    # Parse NODE TABLE definitions
    node_pattern = r"CREATE NODE TABLE\s+`?(\w+)`?\s*\(([^)]+)\)"
    for match in re.finditer(node_pattern, content, re.IGNORECASE):
        table_name = match.group(1)
        columns_str = match.group(2)
        
        columns = []
        pk_col = None
        for col_def in columns_str.split(','):
            col_def = col_def.strip()
            col_match = re.match(r'`?(\w+)`?\s+(\w+)', col_def)
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2)
                columns.append((col_name, col_type))
                if 'PRIMARY KEY' in col_def.upper():
                    pk_col = col_name
        
        node_tables[table_name] = {
            'columns': columns,
            'pk_col': pk_col or (columns[0][0] if columns else 'id')
        }
    
    # Parse REL TABLE definitions
    rel_pattern = r"CREATE REL TABLE\s+`?(\w+)`?\s*\(\s*FROM\s+`?(\w+)`?\s+TO\s+`?(\w+)`?([^)]*)\)"
    for match in re.finditer(rel_pattern, content, re.IGNORECASE):
        rel_name = match.group(1)
        from_node = match.group(2)
        to_node = match.group(3)
        props_str = match.group(4)
        
        columns = []
        for col_def in props_str.split(','):
            col_def = col_def.strip()
            if not col_def:
                continue
            col_match = re.match(r'`?(\w+)`?\s+(\w+)', col_def)
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2)
                columns.append((col_name, col_type))
        
        edge_tables[rel_name] = {
            'from_node': from_node,
            'to_node': to_node,
            'columns': columns
        }
    
    return node_tables, edge_tables

def map_duckdb_type(cypher_type: str) -> str:
    """Map Cypher type to DuckDB type."""
    type_map = {
        'INT64': 'BIGINT',
        'INT32': 'INTEGER',
        'INT16': 'SMALLINT',
        'INT8': 'TINYINT',
        'UINT64': 'UBIGINT',
        'UINT32': 'UINTEGER',
        'UINT16': 'USMALLINT',
        'UINT8': 'UTINYINT',
        'DOUBLE': 'DOUBLE',
        'FLOAT': 'FLOAT',
        'BOOL': 'BOOLEAN',
        'STRING': 'VARCHAR',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP',
        'TIME': 'TIME',
        'BLOB': 'BLOB',
        'JSON': 'VARCHAR'
    }
    return type_map.get(cypher_type, 'VARCHAR')

def create_duckdb_from_parquet(schema_path: Path, parquet_dir: Path, output_db: Path):
    """Create DuckDB from parquet files according to schema."""
    node_tables, edge_tables = parse_schema_cypher(schema_path)
    
    con = duckdb.connect(str(output_db))
    
    # Drop any existing tables
    con.execute("DROP TABLE IF EXISTS nodes_Place")
    con.execute("DROP TABLE IF EXISTS nodes_Event")
    con.execute("DROP TABLE IF EXISTS nodes_Concept")
    con.execute("DROP TABLE IF EXISTS nodes_KnowledgeGraph")
    con.execute("DROP TABLE IF EXISTS nodes_Organization")
    con.execute("DROP TABLE IF EXISTS nodes_Person")
    
    # Drop existing edge tables
    result = con.execute("SHOW TABLES")
    for row in result.fetchall():
        table_name = row[0]
        if table_name.startswith('edges_'):
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Create node tables
    for table_name, schema in node_tables.items():
        parquet_file = parquet_dir / f"{table_name}.parquet"
        if not parquet_file.exists():
            print(f"Warning: Parquet file not found for node table {table_name}: {parquet_file}")
            continue
        
        # Get the actual column names from the parquet file
        result = con.execute(f"SELECT * FROM read_parquet('{parquet_file}') LIMIT 1")
        parquet_cols = [desc[0] for desc in result.description]
        
        # Build column definitions using the actual parquet column names
        col_defs = []
        col_names = []
        for col in parquet_cols:
            # Extract base column name (remove prefix like 'a.')
            base_name = col.split('.')[-1] if '.' in col else col
            col_names.append(base_name)
            
            # Try to find the type from schema
            col_type = 'VARCHAR'  # default
            for schema_col_name, schema_col_type in schema['columns']:
                if schema_col_name.lower() == base_name.lower():
                    col_type = map_duckdb_type(schema_col_type)
                    break
            
            col_defs.append(f'"{base_name}" {col_type}')
        
        columns_str = ', '.join(col_defs)
        
        # Create table
        table_create = f'CREATE TABLE nodes_{table_name} ({columns_str});'
        con.execute(table_create)
        
        # Insert data with proper column mapping
        select_parts = [f'"{col}"' for col in parquet_cols]
        insert_sql = f"INSERT INTO nodes_{table_name} ({', '.join(['\"' + c + '\"' for c in col_names])}) SELECT {', '.join(select_parts)} FROM read_parquet('{parquet_file}')"
        con.execute(insert_sql)
        
        print(f"Created node table: nodes_{table_name} ({parquet_file.name})")
    
    # Create edge tables
    for rel_name, schema in edge_tables.items():
        # Try to find the parquet file
        from_node = schema['from_node']
        to_node = schema['to_node']
        parquet_file = parquet_dir / f"{rel_name}_{from_node}_{to_node}.parquet"
        
        if not parquet_file.exists():
            # Try just the rel name
            parquet_file = parquet_dir / f"{rel_name}.parquet"
        
        if not parquet_file.exists():
            print(f"Warning: Parquet file not found for edge table {rel_name}")
            continue
        
        # Get the actual column names from the parquet file
        result = con.execute(f"SELECT * FROM read_parquet('{parquet_file}') LIMIT 1")
        parquet_cols = [desc[0] for desc in result.description]
        
        # Build column definitions (source, target + properties)
        # a.id is source, b.id is target, r.* are properties
        col_defs = ['"source" BIGINT', '"target" BIGINT']
        col_names = ['source', 'target']
        
        for col in parquet_cols:
            base_name = col.split('.')[-1] if '.' in col else col
            # Skip id columns since we already have source/target
            if base_name.lower() == 'id':
                continue
            col_names.append(base_name)
            # Find type from schema
            col_type = 'VARCHAR'
            for schema_col_name, schema_col_type in schema['columns']:
                if schema_col_name.lower() == base_name.lower():
                    col_type = map_duckdb_type(schema_col_type)
                    break
            col_defs.append(f'"{base_name}" {col_type}')
        
        columns_str = ', '.join(col_defs)
        
        # Create edges table
        table_create = f'CREATE TABLE edges_{rel_name} ({columns_str});'
        con.execute(table_create)
        
        # Build insert statement - map a.id -> source, b.id -> target, r.* -> property
        select_parts = []
        for col in parquet_cols:
            base_name = col.split('.')[-1] if '.' in col else col
            if base_name.lower() == 'id':
                # Determine if it's source or target
                prefix = col.split('.')[0] if '.' in col else ''
                if prefix == 'a':
                    select_parts.append(f'"{col}" AS source')
                elif prefix == 'b':
                    select_parts.append(f'"{col}" AS target')
                else:
                    select_parts.append(f'"{col}"')
            else:
                select_parts.append(f'"{col}"')
        
        insert_sql = f"INSERT INTO edges_{rel_name} ({', '.join(['\"' + c + '\"' for c in col_names])}) SELECT {', '.join(select_parts)} FROM read_parquet('{parquet_file}')"
        con.execute(insert_sql)
        
        print(f"Created edge table: edges_{rel_name} ({parquet_file.name})")
    
    con.close()
    print(f"\nDuckDB created: {output_db}")

if __name__ == "__main__":
    schema_path = Path("./kg_history/schema.cypher")
    parquet_dir = Path("./kg_history")
    output_db = Path("./kg_history.duckdb")
    
    create_duckdb_from_parquet(schema_path, parquet_dir, output_db)
