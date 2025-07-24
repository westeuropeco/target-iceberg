"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
import os
from typing import cast, Any
from decimal import Decimal
from singer_sdk.sinks import BatchSink
import pyarrow as pa  # type: ignore
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    ValidationError,
    CommitFailedException,
)
from pyiceberg.types import StringType
from pyarrow import fs

from .iceberg import singer_to_pyarrow_schema, pyarrow_to_pyiceberg_schema


def coerce_decimals(obj):
    """Recursively convert decimal.Decimal objects to float for PyArrow compatibility."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: coerce_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [coerce_decimals(item) for item in obj]
    else:
        return obj


def _align_arrow_to_iceberg(
    arrow_tbl: pa.Table,
    iceberg_sch: Schema,         # pyiceberg.schema.Schema
) -> pa.Table:
    """Return *arrow_tbl* reordered & padded to exactly match iceberg_sch."""
    names = [f.name for f in iceberg_sch.fields]
    cols  = {}
    for n in names:
        if n in arrow_tbl.column_names:
            cols[n] = arrow_tbl[n]
        else:
            # create a null column of the right Arrow type
            pa_type = iceberg_sch.find_field(n).type.to_arrow()
            cols[n] = pa.scalar(None, type=pa_type).repeat(len(arrow_tbl))
    return pa.Table.from_pydict(cols)


class IcebergSink(BatchSink):
    """Iceberg target sink class."""

    # Disable strict JSONSchema record validation. The HERE Traffic API returns
    # highly-sparse objects (different properties appear in different records).
    # The default Singer-SDK behaviour marks *all* observed properties as
    # `required`, which causes validation failures whenever later records miss
    # any of them.  Setting `validate_records = False` tells the core SDK to
    # skip that per-record validation step and lets us load whatever the tap
    # emits.
    validate_records = False

    max_size = 10000  # Max records to write in one batch

    def __init__(
        self,
        target: Any,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        self.stream_name = stream_name
        self.schema = schema


    def process_batch(self, context: dict) -> None:
        if not context["records"]:
            return                                               # nothing to do ✔︎

        # 1. ----- catalogue ----------------------------------------------------
        catalog = load_catalog(
            self.config["iceberg_catalog_name"],
            uri=self.config["iceberg_rest_uri"],
            **{
                "s3.endpoint": self.config["s3_endpoint"],
                "s3.region": fs.resolve_s3_region(self.config["s3_bucket"]),
                "s3.access-key-id": self.config["aws_access_key_id"],
                "s3.secret-access-key": self.config["aws_secret_access_key"],
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            },
        )

        ns = self.config["iceberg_catalog_namespace_name"]
        try:
            catalog.create_namespace(ns)
        except (NamespaceAlreadyExistsError, NoSuchNamespaceError):
            pass

        # 2. ----- build Arrow table -------------------------------------------
        pa_schema  = singer_to_pyarrow_schema(self, self.schema)
        arrow_tbl  = pa.Table.from_pylist(
            [coerce_decimals(r) for r in context["records"]],
            schema=pa_schema,
        )

        # 3. ----- load / create Iceberg table ----------------------------------
        table_id = f"{ns}.{self.stream_name}"
        try:
            table = catalog.load_table(table_id)
            table_exists = True
        except NoSuchTableError:
            table_exists = False
            iceberg_schema = pyarrow_to_pyiceberg_schema(self, pa_schema)
            table = catalog.create_table(table_id, schema=iceberg_schema)

        # 4. ----- (optional) evolve schema first -------------------------------
        if table_exists:
            with table.update_schema() as upd:
                upd.union_by_name(arrow_tbl.schema)  # no‑op if nothing new
            table = catalog.load_table(table_id)     # reload snapshot

        # 5. ----- align & write -------------------------------------------------
        aligned_tbl = _align_arrow_to_iceberg(arrow_tbl, table.schema())
        try:
            table.append(aligned_tbl)
        except ValidationError as e:
            self.logger.error("Schema validation failed: %s", e)
            raise
        except CommitFailedException as e:
            self.logger.error("Iceberg commit failed: %s", e)
            raise

    def process_batch_old(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        # Load the Iceberg catalog
        region = fs.resolve_s3_region(self.config.get("s3_bucket"))
        self.logger.info(f"AWS Region: {region}")

        catalog_name = self.config.get("iceberg_catalog_name")
        self.logger.info(f"Catalog name: {catalog_name}")

        s3_endpoint = self.config.get("s3_endpoint")
        self.logger.info(f"S3 endpoint: {s3_endpoint}")

        iceberg_rest_uri = self.config.get("iceberg_rest_uri")
        self.logger.info(f"Iceberg REST URI: {iceberg_rest_uri}")

        catalog = load_catalog(
            catalog_name,
            **{
                "uri": iceberg_rest_uri,
                "s3.endpoint": s3_endpoint,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "s3.region": region,
                "s3.access-key-id": self.config.get("aws_access_key_id"),
                "s3.secret-access-key": self.config.get("aws_secret_access_key"),
            },
        )

        nss = catalog.list_namespaces()
        self.logger.info(f"Namespaces: {nss}")

        # Create a namespace if it doesn't exist
        ns_name: str = cast(str, self.config.get("iceberg_catalog_namespace_name"))
        try:
            catalog.create_namespace(ns_name)
            self.logger.info(f"Namespace '{ns_name}' created")
        except (NamespaceAlreadyExistsError, NoSuchNamespaceError):
            # NoSuchNamespaceError is also raised for some reason (probably a bug - but needs to be handled anyway)
            self.logger.info(f"Namespace '{ns_name}' already exists")

        # Create pyarrow df
        singer_schema = self.schema
        pa_schema = singer_to_pyarrow_schema(self, singer_schema)
        
        # Coerce decimal values to float for PyArrow compatibility
        coerced_records = [coerce_decimals(record) for record in context["records"]]
        df = pa.Table.from_pylist(coerced_records, schema=pa_schema)

        # Create a table if it doesn't exist
        table_name = self.stream_name
        table_id = f"{ns_name}.{table_name}"

        try:
            table = catalog.load_table(table_id)
            self.logger.info(f"Table '{table_id}' loaded")

        except NoSuchTableError as e:
            # Table doesn't exist, so create it
            pyiceberg_schema = pyarrow_to_pyiceberg_schema(self, pa_schema)
            table = catalog.create_table(table_id, schema=pyiceberg_schema)
            self.logger.info(f"Table '{table_id}' created")

        # Add data to the table with automatic schema evolution
        try:
            table.append(df)
            self.logger.info(f"Data appended to table '{table_id}' successfully")
        except ValueError as e:
            if "Mismatch in fields" in str(e):
                self.logger.info(f"Schema mismatch detected for table '{table_id}', evolving schema...")
                
                # Evolve schema to include new fields from the incoming data
                with table.update_schema() as update_schema:
                    update_schema.union_by_name(df.schema)
                self.logger.info(f"Schema evolved for table '{table_id}'")
                
                # Reload the table to get the updated schema
                table = catalog.load_table(table_id)
                self.logger.info(f"Reloaded table '{table_id}' with evolved schema")
                
                # Retry append with evolved schema
                table.append(df)
                self.logger.info(f"Data appended to table '{table_id}' after schema evolution")
            else:
                # Re-raise non-schema errors
                self.logger.error(f"Failed to append data to table '{table_id}': {e}")
                raise
