from pathlib import Path

import pandas as pd
import sqlalchemy
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import get_current_context

from airflow_operator import create_dag


SOURCE_SCHEMA = "sagerx_dev"
DEST_SCHEMA = "sagerx_dev"
DEFAULT_OUTPUT_PATH = "/opt/airflow/exports/sagerx.duckdb"
DEFAULT_CHUNK_SIZE = 100_000
POSTGRES_SOURCE_ALIAS = "pg_source"

RELATIONS_SQL = sqlalchemy.text(
    """
    select
        c.relname as relation_name,
        case c.relkind
            when 'r' then 'table'
            when 'p' then 'partitioned_table'
            when 'v' then 'view'
            when 'm' then 'materialized_view'
        end as relation_type
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n
        on n.oid = c.relnamespace
    where n.nspname = :schema_name
        and c.relkind in ('r', 'p', 'v', 'm')
        and not c.relispartition
    order by c.relname
    """
)

COLUMNS_SQL = sqlalchemy.text(
    """
    select
        a.attname as column_name,
        coalesce(
            isc.data_type,
            case
                when t.typcategory = 'A' then 'ARRAY'
                when t.typtype in ('e', 'd') then 'USER-DEFINED'
                else t.typname
            end
        ) as data_type,
        coalesce(isc.udt_name, t.typname) as udt_name,
        isc.numeric_precision,
        isc.numeric_scale,
        a.attnum as ordinal_position
    from pg_catalog.pg_attribute a
    join pg_catalog.pg_class c
        on c.oid = a.attrelid
    join pg_catalog.pg_namespace n
        on n.oid = c.relnamespace
    join pg_catalog.pg_type t
        on t.oid = a.atttypid
    left join information_schema.columns isc
        on isc.table_schema = n.nspname
        and isc.table_name = c.relname
        and isc.column_name = a.attname
    where n.nspname = :schema_name
        and c.relname = :relation_name
        and a.attnum > 0
        and not a.attisdropped
    order by a.attnum
    """
)


def _duckdb_identifier(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _duckdb_schema(schema_name, catalog_name=None):
    schema = _duckdb_identifier(schema_name)
    if catalog_name is None:
        return schema
    return f"{_duckdb_identifier(catalog_name)}.{schema}"


def _duckdb_relation(schema_name, relation_name, catalog_name=None):
    return (
        f"{_duckdb_schema(schema_name, catalog_name)}."
        f"{_duckdb_identifier(relation_name)}"
    )


def _duckdb_string_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def _duckdb_current_catalog(duckdb_connection):
    return duckdb_connection.execute("select current_database()").fetchone()[0]


def _postgres_identifier(engine, identifier):
    return engine.dialect.identifier_preparer.quote(identifier)


def _postgres_relation(engine, schema_name, relation_name):
    return (
        f"{_postgres_identifier(engine, schema_name)}."
        f"{_postgres_identifier(engine, relation_name)}"
    )


def _libpq_value(value):
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _postgres_connection_string(pg_hook):
    connection = pg_hook.get_connection("postgres_default")
    parts = [
        ("host", connection.host),
        ("port", connection.port or 5432),
        ("dbname", connection.schema),
        ("user", connection.login),
        ("password", connection.password),
    ]

    extra = connection.extra_dejson or {}
    for key in ("sslmode", "sslcert", "sslkey", "sslrootcert"):
        if extra.get(key):
            parts.append((key, extra[key]))

    return " ".join(
        f"{key}={_libpq_value(value)}"
        for key, value in parts
        if value not in (None, "")
    )


def _duckdb_type_and_cast(column):
    data_type = column["data_type"]
    udt_name = column["udt_name"]

    if data_type == "ARRAY" or udt_name.startswith("_"):
        return "VARCHAR", True

    if data_type in {"USER-DEFINED", "json", "jsonb", "xml"}:
        return "VARCHAR", True

    if udt_name == "numeric":
        precision = column["numeric_precision"]
        scale = column["numeric_scale"]
        if precision is not None and scale is not None and int(precision) <= 38:
            return f"DECIMAL({int(precision)}, {int(scale)})", False
        return "DOUBLE", False

    type_map = {
        "bool": "BOOLEAN",
        "int2": "SMALLINT",
        "int4": "INTEGER",
        "int8": "BIGINT",
        "float4": "REAL",
        "float8": "DOUBLE",
        "date": "DATE",
        "time": "TIME",
        "timetz": "VARCHAR",
        "timestamp": "TIMESTAMP",
        "timestamptz": "TIMESTAMPTZ",
        "interval": "INTERVAL",
        "text": "VARCHAR",
        "varchar": "VARCHAR",
        "bpchar": "VARCHAR",
        "uuid": "UUID",
        "bytea": "BLOB",
        "inet": "VARCHAR",
        "cidr": "VARCHAR",
        "macaddr": "VARCHAR",
        "macaddr8": "VARCHAR",
        "money": "VARCHAR",
    }
    duckdb_type = type_map.get(udt_name, "VARCHAR")
    cast_to_text = duckdb_type == "VARCHAR" and udt_name not in {
        "text",
        "varchar",
        "bpchar",
    }
    return duckdb_type, cast_to_text


def _load_postgres_extension(duckdb_connection):
    try:
        duckdb_connection.execute("install postgres")
    except Exception as install_error:
        print(f"Could not install DuckDB postgres extension: {install_error}")

    try:
        duckdb_connection.execute("load postgres")
        return True
    except Exception as load_error:
        print(f"Could not load DuckDB postgres extension: {load_error}")
        return False


def _export_relation_with_postgres_extension(
    duckdb_connection,
    source_schema,
    dest_schema,
    dest_catalog,
    relation_name,
):
    source_relation = (
        f"{_duckdb_identifier(POSTGRES_SOURCE_ALIAS)}."
        f"{_duckdb_identifier(source_schema)}."
        f"{_duckdb_identifier(relation_name)}"
    )
    dest_relation = _duckdb_relation(dest_schema, relation_name, dest_catalog)

    duckdb_connection.execute(
        f"create table {dest_relation} as select * from {source_relation}"
    )
    return duckdb_connection.execute(
        f"select count(*) from {dest_relation}"
    ).fetchone()[0]


def _drop_duckdb_relation(
    duckdb_connection, schema_name, relation_name, catalog_name=None
):
    relation = _duckdb_relation(schema_name, relation_name, catalog_name)
    duckdb_connection.execute(f"drop table if exists {relation}")
    duckdb_connection.execute(f"drop view if exists {relation}")


def _export_relation_with_pandas(
    duckdb_connection,
    pg_connection,
    engine,
    source_schema,
    dest_schema,
    dest_catalog,
    relation_name,
    chunk_size,
):
    columns = [
        dict(row)
        for row in pg_connection.execute(
            COLUMNS_SQL,
            {"schema_name": source_schema, "relation_name": relation_name},
        ).mappings()
    ]

    dest_relation = _duckdb_relation(dest_schema, relation_name, dest_catalog)
    column_definitions = []
    select_columns = []
    for column in columns:
        column_name = column["column_name"]
        duckdb_type, cast_to_text = _duckdb_type_and_cast(column)
        duckdb_column = _duckdb_identifier(column_name)
        postgres_column = _postgres_identifier(engine, column_name)
        column_definitions.append(f"{duckdb_column} {duckdb_type}")
        if cast_to_text:
            select_columns.append(f"{postgres_column}::text as {postgres_column}")
        else:
            select_columns.append(postgres_column)

    duckdb_connection.execute(
        f"create table {dest_relation} ({', '.join(column_definitions)})"
    )

    source_relation = _postgres_relation(engine, source_schema, relation_name)
    select_sql = f"select {', '.join(select_columns)} from {source_relation}"

    row_count = 0
    for chunk in pd.read_sql_query(
        select_sql, con=pg_connection, chunksize=chunk_size
    ):
        duckdb_connection.register("export_chunk", chunk)
        try:
            duckdb_connection.execute(
                f"insert into {dest_relation} select * from export_chunk"
            )
        finally:
            duckdb_connection.unregister("export_chunk")
        row_count += len(chunk)

    return row_count


def _positive_int(value, default):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


dag = create_dag(
    dag_id="export_duckdb",
    schedule=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    params={
        "source_schema": SOURCE_SCHEMA,
        "dest_schema": DEST_SCHEMA,
        "output_path": DEFAULT_OUTPUT_PATH,
        "chunk_size": DEFAULT_CHUNK_SIZE,
    },
)


with dag:

    @task
    def export_sagerx_dev_to_duckdb():
        import duckdb

        context = get_current_context()
        params = context["params"]
        source_schema = params.get("source_schema") or SOURCE_SCHEMA
        dest_schema = params.get("dest_schema") or source_schema
        output_path = Path(params.get("output_path") or DEFAULT_OUTPUT_PATH)
        chunk_size = _positive_int(params.get("chunk_size"), DEFAULT_CHUNK_SIZE)
        temp_path = output_path.with_name(f".{output_path.name}.tmp")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        if temp_path.exists():
            temp_path.unlink()

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.connect() as pg_connection:
            relations = [
                dict(row)
                for row in pg_connection.execute(
                    RELATIONS_SQL, {"schema_name": source_schema}
                ).mappings()
            ]

            if not relations:
                raise ValueError(
                    f"No tables, views, or materialized views found in {source_schema}"
                )

            duckdb_connection = duckdb.connect(str(temp_path))
            try:
                # DuckDB uses the database file as a catalog; qualify local
                # destinations so catalog/schema name collisions stay valid.
                dest_catalog = _duckdb_current_catalog(duckdb_connection)
                duckdb_connection.execute(
                    "create schema if not exists "
                    f"{_duckdb_schema(dest_schema, dest_catalog)}"
                )

                use_postgres_extension = _load_postgres_extension(duckdb_connection)
                if use_postgres_extension:
                    postgres_connection_string = _postgres_connection_string(pg_hook)
                    try:
                        duckdb_connection.execute(
                            "attach "
                            f"{_duckdb_string_literal(postgres_connection_string)} "
                            f"as {_duckdb_identifier(POSTGRES_SOURCE_ALIAS)} "
                            "(type postgres)"
                        )
                    except Exception as attach_error:
                        print(f"Could not attach Postgres in DuckDB: {attach_error}")
                        use_postgres_extension = False

                total_rows = 0
                for relation in relations:
                    relation_name = relation["relation_name"]
                    relation_type = relation["relation_type"]
                    print(f"Exporting {source_schema}.{relation_name} ({relation_type})")

                    if use_postgres_extension:
                        try:
                            row_count = _export_relation_with_postgres_extension(
                                duckdb_connection,
                                source_schema,
                                dest_schema,
                                dest_catalog,
                                relation_name,
                            )
                        except Exception as export_error:
                            print(
                                "DuckDB postgres extension export failed for "
                                f"{source_schema}.{relation_name}: {export_error}"
                            )
                            _drop_duckdb_relation(
                                duckdb_connection,
                                dest_schema,
                                relation_name,
                                dest_catalog,
                            )
                            row_count = _export_relation_with_pandas(
                                duckdb_connection,
                                pg_connection,
                                engine,
                                source_schema,
                                dest_schema,
                                dest_catalog,
                                relation_name,
                                chunk_size,
                            )
                    else:
                        row_count = _export_relation_with_pandas(
                            duckdb_connection,
                            pg_connection,
                            engine,
                            source_schema,
                            dest_schema,
                            dest_catalog,
                            relation_name,
                            chunk_size,
                        )

                    total_rows += row_count
                    print(f"Exported {row_count} rows from {source_schema}.{relation_name}")

                if use_postgres_extension:
                    duckdb_connection.execute(
                        f"detach {_duckdb_identifier(POSTGRES_SOURCE_ALIAS)}"
                    )
            finally:
                duckdb_connection.close()

        temp_path.replace(output_path)

        print(
            f"Exported {len(relations)} relations and {total_rows} rows "
            f"from {source_schema} to {output_path}"
        )
        return {
            "output_path": str(output_path),
            "relation_count": len(relations),
            "row_count": total_rows,
        }

    export_sagerx_dev_to_duckdb()
