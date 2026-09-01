import boto3
import csv
import io
import urllib.parse
from datetime import datetime
import json

s3 = boto3.client("s3")

PROCESSED_BUCKET = "rohal-sales-processed-2026"
TRACKING_KEY = "processed-files/processed_files.json"

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

    print("========================================")
    print("Starting Sales ETL Pipeline")
    print("========================================")

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

        print(
            f"FILE ALREADY PROCESSED: {raw_key}"
        )

        return {
            "statusCode": 200,
            "message": "File already processed",
            "file": raw_key
        }

    print(f"Source bucket: {raw_bucket}")
    print(f"Source file: {raw_key}")

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

        print(f"FILE REJECTED: {error_reason}")

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

        print(
            f"Rejected file saved to: {rejected_key}"
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

        print(f"FILE REJECTED: {error_reason}")

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

        print(
            f"Rejected file saved to: {rejected_key}"
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

        print(f"FILE REJECTED: {error_reason}")

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

        print(
            f"Rejected file saved to: {rejected_key}"
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
    for row in row:

        records_received += 1

        order_id = row.get("OrderID", "").strip()

        # --------------------------------
        # Duplicate validation
        # --------------------------------

        if order_id in seen_orders:

            duplicate_count += 1

            row["RejectionReason"] = "Duplicate OrderID"

            rejected_rows.append(row)

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
    # Write processed records
    # ========================================

    file_name = raw_key.split("/")[-1]

    processed_key = (
        "processed/"
        + file_name.replace(
            ".csv",
            "_processed.csv"
        )
    )

    if processed_rows:

        output = io.StringIO()

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

        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames
        )

        writer.writeheader()

        writer.writerows(processed_rows)

        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=processed_key,
            Body=output.getvalue(),
            ContentType="text/csv"
        )

        print(
            f"Processed file created: {processed_key}"
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

    print("========================================")
    print("ETL SUMMARY")
    print("========================================")

    print(
        f"Records received: {records_received}"
    )

    print(
        f"Valid records: {len(processed_rows)}"
    )

    print(
        f"Invalid records: {invalid_count}"
    )

    print(
        f"Duplicate records: {duplicate_count}"
    )

    print(
        f"Records written: {len(processed_rows)}"
    )

    print("ETL completed successfully.")

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