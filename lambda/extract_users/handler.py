import csv
from datetime import datetime
import json
import os
import urllib.request

import boto3


def main(event, context):
    bucket = os.environ["DATA_BUCKET"]
    prefix = os.environ.get("OUTPUT_PREFIX", "raw/jsonplaceholder/users/")

    url = "https://jsonplaceholder.typicode.com/users"
    with urllib.request.urlopen(url, timeout=20) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    fieldnames = [
        "id",
        "name",
        "username",
        "email",
        "phone",
        "website",
    ]

    rows = []
    for user in data:
        rows.append(
            {
                "id": user.get("id"),
                "name": user.get("name"),
                "username": user.get("username"),
                "email": user.get("email"),
                "phone": user.get("phone"),
                "website": user.get("website"),
            }
        )

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix}users_{timestamp}.csv"

    tmp_path = f"/tmp/{os.path.basename(key)}"
    with open(tmp_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    s3 = boto3.client("s3")
    s3.upload_file(tmp_path, bucket, key)

    return {
        "statusCode": 200,
        "body": json.dumps({"bucket": bucket, "key": key, "rows": len(rows)}),
    }
