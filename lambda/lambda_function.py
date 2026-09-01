import boto3
import csv
import io
import urllib.parse
from datetime import datetime
import json
import logging
import os
import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

RAW_BUCKET = os.environ["RAW_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
TRACKING_KEY = os.environ["TRACKING_KEY"]


def log_event(event_name, **details):

    log_data = {
        "event": event_name,
        "timestamp": datetime.utcnow().isoformat(),
        **details
    }

    logger.info(json.dumps(log_data))


REQUIRED_COLUMNS = [
    "OrderID",
    "CustomerID",
    "Product",
    "Category",
    "Quantity",
    "UnitPrice",
    "OrderDate",
    "Country"
]


def load_processed_files():
    try:
        response = s3.get_object(
            Bucket=PROCESSED_BUCKET,
            Key=TRACKING_KEY
        )

        content = response["Body"].read().decode("utf-8")

        return json.loads(content)

    except s3.exceptions.NoSuchKey:
        return {}

    except Exception as e:
        print(f"Could not load tracking file: {e}")
        return {}


def save_processed_files(processed_files):

    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=TRACKING_KEY,
        Body=json.dumps(
            processed_files,
            indent=4
        ),
        ContentType="application/json"
    )


def lambda_handler(event, context):

    try:

        log_event(
            "etl_started"
        )

        # Get information from S3 event
        raw_bucket = event["Records"][0]["s3"]["bucket"]["name"]

        raw_key = event["Records"][0]["s3"]["object"]["key"]

        raw_key = urllib.parse.unquote_plus(raw_key)

        object_etag = event["Records"][0]["s3"]["object"].get("eTag")

        print(f"Object ETag: {object_etag}")

        processed_files = load_processed_files()

        previous_etag = processed_files.get(
            raw_key,
            {}
        ).get("etag")

        if previous_etag == object_etag:

            log_event(
                "duplicate_file_skipped",
                key=raw_key
            )

            return {
                "statusCode": 200,
                "message": "File already processed",
                "file": raw_key
            }

        log_event(
            "source_file_received",
            bucket=raw_bucket,
            key=raw_key
        )

        # Read source file
        response = s3.get_object(
            Bucket=raw_bucket,
            Key=raw_key
        )

        csv_content = response["Body"].read().decode("utf-8")

        csv_file = io.StringIO(csv_content)

        try:

            reader = csv.DictReader(csv_file)

            # Force the CSV reader to parse all rows.
            # This allows malformed CSV errors to be detected
            # before ETL processing continues.
            rows = list(reader)

        except csv.Error as e:

            error_reason = f"Malformed CSV file: {str(e)}"

            log_event(
                "file_rejected",
                reason=error_reason,
                key=raw_key
            )

            rejected_key = (
                "rejected/"
                + raw_key.split("/")[-1].replace(
                    ".csv",
                    "_rejected.csv"
                )
            )

            s3.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=rejected_key,
                Body=csv_content,
                ContentType="text/csv"
            )

            log_event(
                "rejected_file_written",
                key=rejected_key
            )

            return {
                "statusCode": 400,
                "message": error_reason
            }

        reader = csv.DictReader(
            io.StringIO(csv_content)
        )

        # Validate CSV structure

        if reader.fieldnames is None:

            error_reason = "CSV file has no header."

            log_event(
                "file_rejected",
                reason=error_reason,
                key=raw_key
            )

            rejected_key = (
                "rejected/"
                + raw_key.split("/")[-1].replace(
                    ".csv",
                    "_rejected.csv"
                )
            )

            s3.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=rejected_key,
                Body=csv_content,
                ContentType="text/csv"
            )

            log_event(
                "rejected_file_written",
                key=rejected_key
            )

            return {
                "statusCode": 400,
                "message": error_reason
            }

        missing_columns = [
            column
            for column in REQUIRED_COLUMNS
            if column not in reader.fieldnames
        ]

        if missing_columns:

            error_reason = (
                f"Missing required columns: {missing_columns}"
            )

            log_event(
                "file_rejected",
                reason=error_reason,
                key=raw_key
            )

            rejected_key = (
                "rejected/"
                + raw_key.split("/")[-1].replace(
                    ".csv",
                    "_rejected.csv"
                )
            )

            s3.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=rejected_key,
                Body=csv_content,
                ContentType="text/csv"
            )

            log_event(
                "rejected_file_written",
                key=rejected_key
            )

            return {
                "statusCode": 400,
                "message": error_reason
            }

        processed_rows = []
        rejected_rows = []

        seen_orders = set()

        records_received = 0
        duplicate_count = 0
        invalid_count = 0

        # Process each record
        for row in rows:

            records_received += 1

            order_id = row.get("OrderID", "").strip()

            # --------------------------------
            # Duplicate validation
            # --------------------------------

            if order_id in seen_orders:

                duplicate_count += 1

                row["RejectionReason"] = "Duplicate OrderID"

                rejected_rows.append(row)

                log_event(
                    "duplicate_record_rejected",
                    order_id=order_id
                )

                continue

            seen_orders.add(order_id)

            rejection_reason = None

            # --------------------------------
            # Required field validation
            # --------------------------------

            for column in REQUIRED_COLUMNS:

                value = row.get(column)

                if value is None or value.strip() == "":

                    rejection_reason = (
                        f"{column} is required"
                    )

                    break

            # --------------------------------
            # Quantity validation
            # --------------------------------

            if rejection_reason is None:

                try:

                    quantity = int(row["Quantity"])

                    if quantity <= 0:

                        rejection_reason = (
                            "Quantity must be greater than 0"
                        )

                except ValueError:

                    rejection_reason = (
                        "Quantity must be an integer"
                    )

            # --------------------------------
            # UnitPrice validation
            # --------------------------------

            if rejection_reason is None:

                try:

                    unit_price = float(row["UnitPrice"])

                    if unit_price < 0:

                        rejection_reason = (
                            "UnitPrice cannot be negative"
                        )

                except ValueError:

                    rejection_reason = (
                        "UnitPrice must be a number"
                    )

            # --------------------------------
            # OrderDate validation
            # --------------------------------

            if rejection_reason is None:

                try:

                    datetime.strptime(
                        row["OrderDate"],
                        "%Y-%m-%d"
                    )

                except ValueError:

                    rejection_reason = (
                        "OrderDate must use YYYY-MM-DD format"
                    )

            # --------------------------------
            # Handle rejected record
            # --------------------------------

            if rejection_reason:

                invalid_count += 1

                row["RejectionReason"] = rejection_reason

                rejected_rows.append(row)

                log_event(
                    "record_rejected",
                    order_id=order_id,
                    reason=rejection_reason
                )

                continue

            # --------------------------------
            # Valid record
            # --------------------------------

            row["Quantity"] = quantity
            row["UnitPrice"] = unit_price

            row["TotalPrice"] = round(
                quantity * unit_price,
                2
            )

            processed_rows.append(row)

        # ========================================
        # Write processed records as Parquet
        # ========================================

        file_name = raw_key.split("/")[-1]

        order_date = processed_rows[0]["OrderDate"]

        order_date_obj = datetime.strptime(
            order_date,
            "%Y-%m-%d"
        )

        year = order_date_obj.strftime("%Y")
        month = order_date_obj.strftime("%m")

        processed_key = (
            "processed/parquet/"
            f"year={year}/"
            f"month={month}/"
            + file_name.replace(
                ".csv",
                "_processed.parquet"
            )
        )

        if processed_rows:

            # Define the output columns
            fieldnames = [
                "OrderID",
                "CustomerID",
                "Product",
                "Category",
                "Quantity",
                "UnitPrice",
                "OrderDate",
                "Country",
                "TotalPrice"
            ]

            # Create a clean list containing only
            # the columns that belong in the processed dataset
            parquet_rows = []

            for row in processed_rows:

                parquet_rows.append({
                    "OrderID": row["OrderID"],
                    "CustomerID": row["CustomerID"],
                    "Product": row["Product"],
                    "Category": row["Category"],
                    "Quantity": row["Quantity"],
                    "UnitPrice": row["UnitPrice"],
                    "OrderDate": row["OrderDate"],
                    "Country": row["Country"],
                    "TotalPrice": row["TotalPrice"]
                })

            # Convert Python records into a PyArrow table
            table = pa.Table.from_pylist(
                parquet_rows
            )

            # Write Parquet into memory
            parquet_buffer = io.BytesIO()

            pq.write_table(
                table,
                parquet_buffer,
                compression="snappy"
            )

            parquet_buffer.seek(0)

            # Upload Parquet file to S3
            s3.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=processed_key,
                Body=parquet_buffer.getvalue(),
                ContentType="application/octet-stream"
            )

            log_event(
                "processed_file_written",
                key=processed_key,
                records=len(processed_rows),
                format="parquet"
            )

        # ========================================
        # Write rejected records
        # ========================================

        rejected_key = (
            "rejected/"
            + file_name.replace(
                ".csv",
                "_rejected.csv"
            )
        )

        if rejected_rows:

            rejected_output = io.StringIO()

            rejected_fieldnames = (
                REQUIRED_COLUMNS
                + ["RejectionReason"]
            )

            rejected_writer = csv.DictWriter(
                rejected_output,
                fieldnames=rejected_fieldnames,
                extrasaction="ignore"
            )

            rejected_writer.writeheader()

            rejected_writer.writerows(
                rejected_rows
            )

            s3.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=rejected_key,
                Body=rejected_output.getvalue(),
                ContentType="text/csv"
            )

            print(
                f"Rejected file created: {rejected_key}"
            )

        # ========================================
        # ETL statistics
        # ========================================

        log_event(
            "etl_summary",
            records_received=records_received,
            valid_records=len(processed_rows),
            invalid_records=invalid_count,
            duplicate_records=duplicate_count,
            records_written=len(processed_rows)
        )

        log_event(
            "etl_completed",
            status="success"
        )

        processed_files[raw_key] = {
            "etag": object_etag,
            "processed_at": datetime.utcnow().isoformat()
        }

        save_processed_files(processed_files)

        print(
            f"File marked as processed: {raw_key}"
        )

        return {
            "statusCode": 200,
            "records_received": records_received,
            "valid_records": len(processed_rows),
            "invalid_records": invalid_count,
            "duplicates": duplicate_count
        }

    except Exception as e:

        log_event(
            "etl_failed",
            error_type=type(e).__name__,
            error_message=str(e),
            bucket=raw_bucket if "raw_bucket" in locals() else None,
            key=raw_key if "raw_key" in locals() else None
        )

        raise
