#!/usr/bin/env python3

"""
Image Upload Flow — Raptor Maps API

Demonstrates the complete image upload flow using the Raptor Maps
public API. This script walks through every step needed to upload
imagery and trigger processing:

    1. Authenticate           — POST /oauth/token  → get a JWT access token
    2. Create Upload Session  — POST /v2/upload_session
    3. Get AWS Credentials    — GET  /v2/upload_session/{id}/aws_credentials
    4. Upload Images to S3    — concurrent uploads via asyncio + boto3
    5. Trigger Ingestion      — POST /v2/upload_sessions/{id}/ingest
    6. Poll Ingestion Status  — GET  /v2/upload_session/{id}/status (optional)

Prerequisites
─────────────
    • Python 3.10+
    • pip install requests boto3
    • Raptor Maps API credentials (client ID & secret)
      → Create at https://app.raptormaps.com/account  (see "API Credentials")
    • Your Organization ID (visible on the same Profile page)

Environment Variables
─────────────────────
    RM_API_CLIENT_ID      Your Raptor Maps API client ID
    RM_API_CLIENT_SECRET   Your Raptor Maps API client secret
    RM_ORG_ID              Your Raptor Maps organization ID

Reference Docs
──────────────
    Getting Started          https://docs.raptormaps.com/reference/reference-getting-started
    Authentication           https://docs.raptormaps.com/reference/get-api-access-token
    Upload Session Status    https://docs.raptormaps.com/reference/apiv2upload_sessionupload_session_idstatus

USAGE:
    # 1. Install dependencies:
    pip install requests boto3

    # 2. Set your credentials:
    export RM_API_CLIENT_ID="<your_client_id>"
    export RM_API_CLIENT_SECRET="<your_client_secret>"
    export RM_ORG_ID="<your_org_id>"

    # 3. Run the script:
    python upload_files_from_local.py --image-dir /path/to/images --order-id <your_order_id>

    # Or pass everything inline:
    RM_API_CLIENT_ID=<your_client_id> RM_API_CLIENT_SECRET=<your_client_secret> RM_ORG_ID=<your_org_id> \\
        python upload_files_from_local.py --image-dir ./my_images --order-id <your_order_id>

    # Additional options:
    python upload_files_from_local.py \\
        --image-dir /path/to/images \\
        --order-id <your_order_id> \\
        --poll-interval 30 \\
        --poll-timeout 1800
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import boto3
import requests

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://api.raptormaps.com"
AUTH_URL = f"{BASE_URL}/oauth/token"
AUTH_AUDIENCE = "api://customer-api"

# Image file extensions we'll upload (case-insensitive)
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png"}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _headers(token: str) -> dict[str, str]:
    """Return standard request headers with Bearer auth."""
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }


def _collect_image_files(directory: Path) -> list[Path]:
    """Return a sorted list of image files in *directory* (non-recursive)."""
    files = [
        f
        for f in sorted(directory.iterdir())
        if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS
    ]
    return files


def _raise_for_status(response: requests.Response, step_name: str) -> None:
    """Raise a clear error if the response is not 2xx."""
    if not response.ok:
        detail = response.text[:500] if response.text else "(no body)"
        raise RuntimeError(f"[{step_name}] HTTP {response.status_code}: {detail}")


# ──────────────────────────────────────────────────────────────────────────────
# Step 1: Get API JWT
# ──────────────────────────────────────────────────────────────────────────────


def get_api_token(client_id: str, client_secret: str) -> str:
    """Authenticate with Raptor Maps OAuth and return a JWT access token.

    Endpoint
    --------
    POST {BASE_URL}/oauth/token

    Body
    ----
    {
        "client_id":     "<your_client_id>",
        "client_secret": "<your_client_secret>",
        "audience":      "api://customer-api"
    }

    Returns
    -------
    str — The access token (JWT) used as a Bearer token for all subsequent calls.

    Reference: https://docs.raptormaps.com/reference/get-api-access-token
    """
    print("=== Step 1: Authenticate (Get API JWT) ===")

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": AUTH_AUDIENCE,
    }

    response = requests.post(
        AUTH_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=30,
    )
    _raise_for_status(response, "Authentication")

    data = response.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("Authentication succeeded but no access_token in response")

    print(f"Authenticated successfully (token starts with {token[:12]}...)")
    return token


# ──────────────────────────────────────────────────────────────────────────────
# Step 2: Create Upload Session
# ──────────────────────────────────────────────────────────────────────────────


def create_upload_session(
    token: str,
    org_id: int,
    file_total: int,
    order_id: int,
    name: str | None = None,
) -> dict:
    """Create an upload session for the given org.

    Endpoint
    --------
    POST {base_url}/v2/upload_session?org_id={org_id}

    Body (CreateUploadSessionRequest)
    ---------------------------------
    {
        "file_total":       <int>,    // number of files you intend to upload
        "is_image_upload":  true,     // signals that this is a drone-image upload
        "name":             <str>,    // optional human-readable label
        "order_id":         <int>     // link this upload to an existing order
    }

    The server validates that the authenticated user has permission on the
    order and resolves the organization from the order's channel.

    Returns
    -------
    dict — The full upload_session object, including ``id`` and ``url``.
    """
    print("\n=== Step 2: Create Upload Session ===")

    endpoint = f"{BASE_URL}/v2/upload_session"
    body: dict = {
        "file_total": file_total,
        "is_image_upload": True,
    }
    if name:
        body["name"] = name
    body["order_id"] = order_id

    response = requests.post(
        endpoint,
        headers=_headers(token),
        params={"org_id": org_id},
        data=json.dumps(body),
        timeout=30,
    )
    _raise_for_status(response, "Create Upload Session")

    upload_session = response.json().get("upload_session", {})
    session_id = upload_session.get("id")
    session_url = upload_session.get("url")

    print("Upload session created")
    print(f"   Session ID : {session_id}")
    print(f"   URL / Key  : {session_url}")
    print(f"   File Total : {file_total}")
    print(f"   Order ID   : {order_id}")

    return upload_session


# ──────────────────────────────────────────────────────────────────────────────
# Step 3: Get AWS Credentials
# ──────────────────────────────────────────────────────────────────────────────


def get_aws_credentials(
    token: str,
    org_id: int,
    upload_session_id: int,
) -> dict:
    """Fetch scoped, temporary AWS credentials for uploading to S3.

    Endpoint
    --------
    GET {base_url}/v2/upload_session/{upload_session_id}/aws_credentials?org_id={org_id}

    Response (AwsCredentialsResponse)
    ---------------------------------
    {
        "access_key_id":     "<temporary_access_key>",
        "secret_access_key": "<temporary_secret_key>",
        "session_token":     "<temporary_session_token>",
        "bucket":            "<s3_bucket_name>",
        "prefix":            "<upload_prefix>/",
        "expiration":        "<iso8601_expiration>"
    }

    Returns
    -------
    dict — The credentials payload.
    """
    print("\n=== Step 3: Get AWS Credentials ===")

    endpoint = f"{BASE_URL}/v2/upload_session/{upload_session_id}/aws_credentials"

    response = requests.get(
        endpoint,
        headers=_headers(token),
        params={"org_id": org_id},
        timeout=30,
    )
    _raise_for_status(response, "Get AWS Credentials")

    creds = response.json()

    print("Received scoped AWS credentials")
    print(f"   Bucket     : {creds['bucket']}")
    print(f"   Prefix     : {creds['prefix']}")
    print(f"   Expires    : {creds['expiration']}")

    return creds


# ──────────────────────────────────────────────────────────────────────────────
# Step 4: Upload Images to S3
# ──────────────────────────────────────────────────────────────────────────────


async def _upload_one(
    s3_client,
    file_path: Path,
    bucket: str,
    s3_key: str,
    idx: int,
    total: int,
    semaphore: asyncio.Semaphore,
) -> None:
    """Upload a single file inside a concurrency-limited semaphore."""
    async with semaphore:
        print(f"   Uploading {idx}/{total}: {file_path.name} -> s3://{bucket}/{s3_key}")
        await asyncio.to_thread(s3_client.upload_file, str(file_path), bucket, s3_key)


def upload_images(
    image_files: list[Path],
    bucket: str,
    prefix: str,
    access_key_id: str,
    secret_access_key: str,
    session_token: str,
    max_concurrency: int = 10,
) -> None:
    """Upload local image files to S3 concurrently using asyncio.

    Creates a boto3 S3 client from the temporary credentials returned by
    ``get_aws_credentials`` and uploads each file to::

        s3://{bucket}/{prefix}{filename}

    Files are uploaded concurrently (up to *max_concurrency* at a time)
    using ``asyncio.to_thread`` so that multiple S3 PutObject calls run
    in parallel.

    Parameters
    ----------
    image_files       : List of local file paths to upload.
    bucket            : S3 bucket name (from AwsCredentialsResponse).
    prefix            : S3 key prefix (from AwsCredentialsResponse).
    access_key_id     : Temporary AWS access key ID.
    secret_access_key : Temporary AWS secret access key.
    session_token     : Temporary AWS session token.
    max_concurrency   : Maximum number of parallel uploads (default 10).
    """
    print("\n=== Step 4: Upload Images to S3 ===")
    print(f"   Max concurrency: {max_concurrency}")

    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
    )
    s3_client = session.client("s3")

    total = len(image_files)
    semaphore = asyncio.Semaphore(max_concurrency)

    async def _upload_all() -> None:
        tasks = [
            _upload_one(s3_client, fp, bucket, f"{prefix}{fp.name}", idx, total, semaphore)
            for idx, fp in enumerate(image_files, start=1)
        ]
        await asyncio.gather(*tasks)

    asyncio.run(_upload_all())

    print(f"All {total} images uploaded successfully")


# ──────────────────────────────────────────────────────────────────────────────
# Step 5: Trigger Ingestion
# ──────────────────────────────────────────────────────────────────────────────


def trigger_ingestion(
    token: str,
    org_id: int,
    upload_session_id: int,
    order_id: int,
) -> str:
    """Begin processing the uploaded images.

    Endpoint
    --------
    POST {base_url}/v2/upload_sessions/{upload_session_id}/ingest?org_id={org_id}&order_id={order_id}

    Body (UploadSessionIngestRequest)
    ---------------------------------
    {}   (all fields are optional)

    Response (IngestResponse)
    -------------------------
    { "ingestion_start_date": "<iso8601_datetime>" }

    Returns
    -------
    str — The ingestion start date string.
    """
    print("\n=== Step 5: Trigger Ingestion ===")

    endpoint = f"{BASE_URL}/v2/upload_sessions/{upload_session_id}/ingest"

    response = requests.post(
        endpoint,
        headers=_headers(token),
        params={"org_id": org_id, "order_id": order_id},
        data=json.dumps({}),
        timeout=30,
    )
    _raise_for_status(response, "Trigger Ingestion")

    data = response.json()
    start_date = data.get("ingestion_start_date", "unknown")

    print(f"Ingestion started at {start_date}")
    return start_date


# ──────────────────────────────────────────────────────────────────────────────
# Step 6: Poll Ingestion Status
# ──────────────────────────────────────────────────────────────────────────────


def poll_status(
    token: str,
    org_id: int,
    upload_session_id: int,
    poll_interval: int = 30,
    poll_timeout: int = 1800,
) -> dict:
    """Poll the upload session status until ingestion is complete or timeout.

    Endpoint
    --------
    GET {base_url}/v2/upload_session/{upload_session_id}/status?org_id={org_id}

    Response Fields
    ───────────────
    upload_session_status : int — 0 = complete, 1 = in progress
    n_images              : int — number of RGB images processed
    n_thermal_images      : int — number of radiometric thermal images processed
    n_tile_maps           : int — number of tile maps generated
    errors                : list — any processing errors
    file_total            : int — total files expected

    Parameters
    ----------
    poll_interval : Seconds between status checks (default 30).
    poll_timeout  : Maximum seconds to wait before giving up (default 1800 = 30 min).

    Returns
    -------
    dict — The final status response.

    Reference: https://docs.raptormaps.com/reference/apiv2upload_sessionupload_session_idstatus
    """
    print("\n=== Step 6: Poll Ingestion Status ===")

    endpoint = f"{BASE_URL}/v2/upload_session/{upload_session_id}/status"
    elapsed = 0

    while elapsed < poll_timeout:
        response = requests.get(
            endpoint,
            headers=_headers(token),
            params={"org_id": org_id},
            timeout=30,
        )
        _raise_for_status(response, "Poll Status")

        data = response.json()
        status_code = data.get("upload_session_status", 1)
        n_images = data.get("n_images", 0)
        n_thermal = data.get("n_thermal_images", 0)
        n_tile_maps = data.get("n_tile_maps", 0)
        errors = data.get("errors", [])
        file_total = data.get("file_total", "?")

        processed = n_images + n_thermal + n_tile_maps
        print(
            f"   [{elapsed:>4}s] Status: {'COMPLETE' if status_code == 0 else 'IN PROGRESS'} "
            f"| Processed: {processed}/{file_total} "
            f"(RGB: {n_images}, Thermal: {n_thermal}, Tile Maps: {n_tile_maps}) "
            f"| Errors: {len(errors)}"
        )

        if status_code == 0:
            print("Ingestion complete!")
            if errors:
                print(f"   WARNING: {len(errors)} error(s) occurred during processing:")
                for err in errors[:5]:  # Show first 5
                    print(f"      - {err}")
            return data

        time.sleep(poll_interval)
        elapsed += poll_interval

    print(f"Timed out after {poll_timeout}s — ingestion is still in progress.")
    print(
        "   You can re-run the status check later or view progress in the Raptor App."
    )
    return data


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def main() -> int:
    """Orchestrate the full image upload flow."""

    parser = argparse.ArgumentParser(
        description="Raptor Maps — Full Image Upload Flow Demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Environment variables required:\n"
            "  RM_API_CLIENT_ID       Raptor Maps API client ID\n"
            "  RM_API_CLIENT_SECRET   Raptor Maps API client secret\n"
            "  RM_ORG_ID              Raptor Maps organization ID\n"
            "\n"
            "Example:\n"
            "  RM_API_CLIENT_ID=<your_client_id> RM_API_CLIENT_SECRET=<your_client_secret> \\\n"
            "    RM_ORG_ID=<your_org_id> python demo_upload_images.py --image-dir ./my_images\n"
        ),
    )
    parser.add_argument(
        "--image-dir",
        type=str,
        required=True,
        help="Path to directory containing images to upload",
    )
    parser.add_argument(
        "--session-name",
        type=str,
        default=None,
        help="Optional human-readable name for the upload session",
    )
    parser.add_argument(
        "--order-id",
        type=int,
        required=True,
        help="Order ID to associate this upload with an existing order",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=6,
        help="Maximum number of parallel S3 uploads (default: 10)",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between status polling requests (default: 30)",
    )
    parser.add_argument(
        "--poll-timeout",
        type=int,
        default=1800,
        help="Maximum seconds to poll before giving up (default: 1800 = 30 min)",
    )

    args = parser.parse_args()

    # ── Read environment variables ────────────────────────────────────────
    client_id = os.environ.get("RM_API_CLIENT_ID")
    client_secret = os.environ.get("RM_API_CLIENT_SECRET")
    org_id_str = os.environ.get("RM_ORG_ID")

    missing = []
    if not client_id:
        missing.append("RM_API_CLIENT_ID")
    if not client_secret:
        missing.append("RM_API_CLIENT_SECRET")
    if not org_id_str:
        missing.append("RM_ORG_ID")
    if missing:
        print(f"ERROR: Missing required environment variable(s): {', '.join(missing)}")
        print(
            "   See --help or https://docs.raptormaps.com/reference/reference-getting-started"
        )
        return 1

    org_id = int(org_id_str)  # type: ignore[arg-type]

    # ── Discover image files ──────────────────────────────────────────────
    image_dir = Path(args.image_dir).resolve()
    if not image_dir.is_dir():
        print(f"ERROR: Image directory does not exist: {image_dir}")
        return 1

    image_files = _collect_image_files(image_dir)
    if not image_files:
        print(f"ERROR: No image files found in {image_dir}")
        print(f"   Supported extensions: {', '.join(sorted(IMAGE_EXTENSIONS))}")
        return 1

    # ── Print run summary ─────────────────────────────────────────────────
    print("Raptor Maps — Image Upload Flow")
    print("=" * 55)
    print(f"   Org ID      : {org_id}")
    print(f"   Image Dir   : {image_dir}")
    print(f"   Images Found: {len(image_files)}")
    if args.session_name:
        print(f"   Session Name: {args.session_name}")
    print(f"   Order ID    : {args.order_id}")
    print("=" * 55)

    try:
        # Step 1: Authenticate
        token = get_api_token(client_id, client_secret)  # type: ignore[arg-type]

        # Step 2: Create Upload Session
        upload_session = create_upload_session(
            token=token,
            org_id=org_id,
            file_total=len(image_files),
            name=args.session_name,
            order_id=args.order_id,
        )
        upload_session_id = upload_session["id"]

        # Step 3: Get AWS Credentials (the new endpoint!)
        creds = get_aws_credentials(
            token=token,
            org_id=org_id,
            upload_session_id=upload_session_id,
        )

        # Step 4: Upload Images to S3 (concurrent via asyncio)
        upload_images(
            image_files=image_files,
            bucket=creds["bucket"],
            prefix=creds["prefix"],
            access_key_id=creds["access_key_id"],
            secret_access_key=creds["secret_access_key"],
            session_token=creds["session_token"],
            max_concurrency=args.max_concurrency,
        )

        # Step 5: Trigger Ingestion
        trigger_ingestion(
            token=token,
            org_id=org_id,
            upload_session_id=upload_session_id,
            order_id=args.order_id,
        )

        # Step 6: Poll Ingestion Status
        poll_status(
            token=token,
            org_id=org_id,
            upload_session_id=upload_session_id,
            poll_interval=args.poll_interval,
            poll_timeout=args.poll_timeout,
        )

        print("\n" + "=" * 55)
        print("Upload flow completed!")
        print("   View your data at https://app.raptormaps.com")

    except KeyboardInterrupt:
        print("\n\nWARNING: Interrupted by user")
        return 130
    except RuntimeError as e:
        print(f"\nERROR: {e}")
        return 1
    except Exception as e:
        print(f"\nERROR: Unexpected error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
