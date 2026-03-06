#!/usr/bin/env python3

"""
Ingestor Upload Flow Demo — Raptor Maps API

Demonstrates the customer-facing upload flow where images already reside in your
own cloud storage.  Instead of uploading files directly, you provide Raptor Maps
with a list of presigned/signed URLs and the platform pulls the images itself.

    1. Authenticate                   — POST /oauth/token  → get a JWT access token
    2. Create Ingestor Upload Session — POST /v2/ingestor/upload_sessions (sends the URL list)

You are responsible for generating the signed URLs yourself before running this
script.  Any cloud storage provider that supports signed/presigned URLs will
work (AWS S3, GCS, Azure Blob, etc.).  The URLs must allow Raptor Maps servers
to download the images via HTTP GET.

Prerequisites
─────────────
    • Python 3.10+
    • pip install requests
    • Raptor Maps API credentials (client ID & secret)
      → Create at https://app.raptormaps.com/account  (see "API Credentials")
    • Your Organization ID (visible on the same Profile page)
    • A list of signed/presigned URLs pointing to your image files.
      The URLs must be accessible by Raptor Maps for the duration of ingestion
      (we recommend a 24-hour expiry at minimum).

Environment Variables
─────────────────────
    RM_API_CLIENT_ID       Your Raptor Maps API client ID
    RM_API_CLIENT_SECRET   Your Raptor Maps API client secret
    RM_ORG_ID              Your Raptor Maps organization ID

Reference Docs
──────────────
    Getting Started          https://docs.raptormaps.com/reference/reference-getting-started
    Authentication           https://docs.raptormaps.com/reference/get-api-access-token
    Ingestor Upload Session  https://docs.raptormaps.com/reference/apiv2ingestorupload_sessions

USAGE:
    # 1. Install dependencies:
    pip install requests

    # 2. Set your Raptor Maps credentials:
    export RM_API_CLIENT_ID="<your_client_id>"
    export RM_API_CLIENT_SECRET="<your_client_secret>"
    export RM_ORG_ID="<your_org_id>"

    # 3. Prepare a text file with one signed URL per line:
    #    urls.txt:
    #        https://your-bucket.s3.amazonaws.com/image001.jpg?<signature_params>
    #        https://your-bucket.s3.amazonaws.com/image002.jpg?<signature_params>

    # 4. Run the script:
    python demo_ingestor_upload.py \\
        --urls-file urls.txt \\
        --order-id <your_order_id> \\
        --session-name "My Upload Session"

    # Or pass URLs directly on the command line:
    python demo_ingestor_upload.py \\
        --urls \\
            "https://your-bucket.s3.amazonaws.com/image001.jpg?<signature_params>" \\
            "https://your-bucket.s3.amazonaws.com/image002.jpg?<signature_params>" \\
        --order-id <your_order_id> \\
        --session-name "My Upload Session"
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import requests

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://api.raptormaps.com"
AUTH_URL = f"{BASE_URL}/oauth/token"
AUTH_AUDIENCE = "api://customer-api"

# Maximum URLs per request (docs recommend ≤ 1000)
MAX_URLS_PER_REQUEST = 1000


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _headers(token: str) -> dict[str, str]:
    """Return standard request headers with Bearer auth."""
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }


def _raise_for_status(response: requests.Response, step_name: str) -> None:
    """Raise a clear error if the response is not 2xx."""
    if not response.ok:
        detail = response.text[:500] if response.text else "(no body)"
        raise RuntimeError(f"[{step_name}] HTTP {response.status_code}: {detail}")


def load_urls_from_file(filepath: str) -> list[str]:
    """Read signed URLs from a text file (one URL per line).

    Blank lines and lines starting with ``#`` are ignored.
    """
    urls: list[str] = []
    with open(filepath) as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                urls.append(stripped)
    return urls


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

    print(f"✅ Authenticated successfully (token starts with {token[:12]}...)")
    return token


# ──────────────────────────────────────────────────────────────────────────────
# Step 2: Create Ingestor Upload Session
# ──────────────────────────────────────────────────────────────────────────────


def create_ingestor_upload_session(
    token: str,
    org_id: int,
    order_id: int,
    data_urls: list[str],
    session_name: str,
    pipeline: str = "om",
) -> dict:
    """Create an ingestor upload session with a list of signed URLs.

    Raptor Maps will pull each image from the provided URLs and begin
    processing.  For performance, the docs recommend sending no more than
    1 000 URLs per request.

    Endpoint
    --------
    POST {BASE_URL}/v2/ingestor/upload_sessions?org_id={org_id}

    Body (CreateIngestorUploadSessionRequest)
    ------------------------------------------
    {
        "upload_session_name": "<session_name>",
        "order_id":            <order_id>,
        "data_url":            ["<signed_url_1>", "<signed_url_2>"],
        "pipeline":            "om"      // "om" (Operations & Maintenance) or "epc"
    }

    Response (CreateIngestorUploadSessionResponse)
    -----------------------------------------------
    {
        "upload_session_id":   <int>,
        "upload_session_uuid": "<uuid>"
    }

    Parameters
    ----------
    token        : Bearer JWT.
    org_id       : Your organisation ID.
    order_id     : Order to associate this upload with.
    data_urls    : List of signed URLs for the images.
    session_name : Human-readable label for the upload session.
    pipeline     : ``"om"`` or ``"epc"`` (default ``"om"``).

    Returns
    -------
    dict — Response containing ``upload_session_id`` (and optionally ``upload_session_uuid``).

    Reference: https://docs.raptormaps.com/reference/apiv2ingestorupload_sessions
    """
    print("\n=== Step 2: Create Ingestor Upload Session ===")

    endpoint = f"{BASE_URL}/v2/ingestor/upload_sessions"

    if len(data_urls) > MAX_URLS_PER_REQUEST:
        print(
            f"   ⚠️  {len(data_urls)} URLs exceeds the recommended maximum of "
            f"{MAX_URLS_PER_REQUEST} per request."
        )
        print("   Sending in batches...")

    # Send in batches if needed
    result: dict = {}
    for batch_start in range(0, len(data_urls), MAX_URLS_PER_REQUEST):
        batch = data_urls[batch_start : batch_start + MAX_URLS_PER_REQUEST]
        batch_num = (batch_start // MAX_URLS_PER_REQUEST) + 1

        body = {
            "upload_session_name": session_name,
            "order_id": order_id,
            "data_url": batch,
            "pipeline": pipeline,
        }

        response = requests.post(
            endpoint,
            headers=_headers(token),
            params={"org_id": org_id},
            json=body,
            timeout=60,
        )
        _raise_for_status(response, f"Create Ingestor Upload Session (batch {batch_num})")

        result = response.json()
        session_id = result.get("upload_session_id")

        if len(data_urls) > MAX_URLS_PER_REQUEST:
            print(
                f"   Batch {batch_num}: {len(batch)} URLs → "
                f"session {session_id}"
            )

    print("✅ Ingestor upload session created")
    print(f"   Session ID   : {result.get('upload_session_id')}")
    print(f"   Session UUID : {result.get('upload_session_uuid', 'N/A')}")
    print(f"   Order ID     : {order_id}")
    print(f"   Total URLs   : {len(data_urls)}")
    print(f"   Pipeline     : {pipeline}")

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def main() -> int:
    """Orchestrate the ingestor upload flow."""

    parser = argparse.ArgumentParser(
        description="Raptor Maps — Ingestor Upload Flow Demo (signed URLs)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Environment variables required:\n"
            "  RM_API_CLIENT_ID       Raptor Maps API client ID\n"
            "  RM_API_CLIENT_SECRET   Raptor Maps API client secret\n"
            "  RM_ORG_ID              Raptor Maps organization ID\n"
            "\n"
            "You must generate signed/presigned URLs for your images BEFORE\n"
            "running this script.  Any cloud provider that supports signed URLs\n"
            "will work (AWS S3, GCS, Azure Blob, etc.).  Ensure the URLs allow\n"
            "HTTP GET access and have a long enough expiry (24 hours recommended).\n"
        ),
    )

    # ── Signed URL source (mutually exclusive: file vs inline)
    url_group = parser.add_argument_group("signed URLs")
    source = url_group.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--urls-file",
        type=str,
        help=(
            "Path to a text file containing one signed URL per line. "
            "Blank lines and lines starting with '#' are ignored."
        ),
    )
    source.add_argument(
        "--urls",
        type=str,
        nargs="+",
        help="Signed URLs passed directly on the command line",
    )

    # ── Raptor Maps options
    parser.add_argument(
        "--order-id",
        type=int,
        required=True,
        help="Order ID to associate this upload with",
    )
    parser.add_argument(
        "--session-name",
        type=str,
        required=True,
        help="Human-readable name for the upload session",
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        default="om",
        choices=["om", "epc"],
        help=(
            "Processing pipeline: 'om' (Operations & Maintenance) "
            "or 'epc' (Engineering, Procurement & Construction). Default: om"
        ),
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
        print(f"❌ Missing required environment variable(s): {', '.join(missing)}")
        print(
            "   See --help or https://docs.raptormaps.com/reference/reference-getting-started"
        )
        return 1

    org_id = int(org_id_str)  # type: ignore[arg-type]

    # ── Load signed URLs ─────────────────────────────────────────────────
    if args.urls_file:
        print(f"📂 Loading signed URLs from {args.urls_file} ...")
        signed_urls = load_urls_from_file(args.urls_file)
        if not signed_urls:
            print(f"❌ No URLs found in {args.urls_file}")
            print("   The file should contain one signed URL per line.")
            return 1
    else:
        signed_urls = args.urls

    print(f"   Loaded {len(signed_urls)} signed URL(s)")

    # ── Print run summary ─────────────────────────────────────────────────
    print("\n🚀 Raptor Maps — Ingestor Upload Flow")
    print("=" * 55)
    print(f"   Org ID        : {org_id}")
    print(f"   Signed URLs   : {len(signed_urls)}")
    print(f"   Order ID      : {args.order_id}")
    print(f"   Session Name  : {args.session_name}")
    print(f"   Pipeline      : {args.pipeline}")
    print("=" * 55)

    try:
        # Step 1: Authenticate
        token = get_api_token(client_id, client_secret)  # type: ignore[arg-type]

        # Step 2: Create Ingestor Upload Session (sends signed URLs to Raptor Maps)
        result = create_ingestor_upload_session(
            token=token,
            org_id=org_id,
            order_id=args.order_id,
            data_urls=signed_urls,
            session_name=args.session_name,
            pipeline=args.pipeline,
        )

        print("\n" + "=" * 55)
        print("🎉 Ingestor upload session created successfully!")
        print(f"   Session ID : {result.get('upload_session_id')}")

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        return 130
    except RuntimeError as e:
        print(f"\n❌ {e}")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
