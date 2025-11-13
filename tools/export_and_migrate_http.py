#!/usr/bin/env python
"""
Export existing ledger entities via HTTP and migrate them to v1.1.

It will:
  1. Read a list of entity IDs from a text file (one per line).
  2. For each entity:
       - GET /ledger?entity=<ID> from DS_BASE
       - store the raw JSON in --out-legacy
       - run migrate_payload() to wrap it as DSubstrateEntity v1.1
       - store migrated JSON in --out-v11

Usage:

  export DS_BASE="https://dualsubstrate-commercial.fly.dev"
  export DS_KEY="demo-key"

  python tools/export_and_migrate_http.py \
    --entities-file entities.txt \
    --out-legacy /tmp/ledger_legacy \
    --out-v11 /tmp/ledger_v11 \
    --ledger-id default

"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Optional

import requests
from pydantic import ValidationError

# ⬇️ adjust these imports to match where you put them
from app.models.dsubstrate_entity import DSubstrateEntity
from tools.migrate_ledger_v11 import migrate_payload  # from previous script


def read_entities(path: Path) -> List[str]:
    ids: List[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            ids.append(s)
    return ids


def fetch_ledger_entity(
    base_url: str,
    api_key: str,
    entity_id: str,
    ledger_id: str = "default",
) -> Optional[dict]:
    url = f"{base_url.rstrip('/')}/ledger"
    headers = {
        "x-api-key": api_key,
        "X-Ledger-ID": ledger_id,
    }
    try:
        resp = requests.get(url, headers=headers, params={"entity": entity_id}, timeout=10)
    except Exception as e:
        print(f"[ERROR] Request failed for entity '{entity_id}': {e}", file=sys.stderr)
        return None

    if resp.status_code != 200:
        print(
            f"[ERROR] Non-200 status for entity '{entity_id}': "
            f"{resp.status_code} {resp.text}",
            file=sys.stderr,
        )
        return None

    try:
        return resp.json()
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON for entity '{entity_id}': {e}", file=sys.stderr)
        return None


def export_and_migrate_entity(
    base_url: str,
    api_key: str,
    entity_id: str,
    ledger_id: str,
    legacy_dir: Path,
    v11_dir: Path,
    default_tier: str,
    default_lawfulness: int,
) -> bool:
    raw = fetch_ledger_entity(base_url, api_key, entity_id, ledger_id=ledger_id)
    if raw is None:
        return False

    # Save raw / legacy
    legacy_path = legacy_dir / f"{entity_id}.json"
    try:
        with legacy_path.open("w", encoding="utf-8") as f:
            json.dump(raw, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[ERROR] Failed to write legacy JSON for '{entity_id}': {e}", file=sys.stderr)
        return False

    # Migrate
    try:
        entity: DSubstrateEntity = migrate_payload(
            raw,
            default_tier=default_tier,
            default_lawfulness=default_lawfulness,
        )
    except ValidationError as e:
        print(
            f"[ERROR] Migration validation failed for '{entity_id}':\n{e}\n",
            file=sys.stderr,
        )
        return False

    v11_path = v11_dir / f"{entity_id}.json"
    try:
        with v11_path.open("w", encoding="utf-8") as f:
            json.dump(entity.dict(), f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[ERROR] Failed to write v1.1 JSON for '{entity_id}': {e}", file=sys.stderr)
        return False

    print(f"[OK] {entity_id} exported + migrated")
    return True


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Export ledger entries via HTTP and migrate to Dual-Substrate v1.1."
    )
    parser.add_argument(
        "--entities-file",
        type=Path,
        required=True,
        help="Text file with entity IDs (one per line)",
    )
    parser.add_argument(
        "--out-legacy",
        type=Path,
        required=True,
        help="Directory to store raw legacy JSON (as fetched from /ledger)",
    )
    parser.add_argument(
        "--out-v11",
        type=Path,
        required=True,
        help="Directory to store migrated v1.1 JSON entities",
    )
    parser.add_argument(
        "--ledger-id",
        type=str,
        default="default",
        help="X-Ledger-ID header value (default: default)",
    )
    parser.add_argument(
        "--tier",
        type=str,
        default="S1",
        help="Default tier for legacy entries (default: S1)",
    )
    parser.add_argument(
        "--lawfulness",
        type=int,
        default=1,
        help="Default lawfulness (0–3) for legacy entries (default: 1)",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=os.environ.get("DS_BASE", "http://localhost:8000"),
        help="Dual-substrate base URL (default from DS_BASE env or http://localhost:8000)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=os.environ.get("DS_KEY"),
        help="API key (default from DS_KEY env)",
    )

    args = parser.parse_args(argv)

    if not args.api_key:
        print("[FATAL] No API key provided (set DS_KEY or use --api-key)", file=sys.stderr)
        return 1

    entities_file: Path = args.entities_file
    legacy_dir: Path = args.out_legacy
    v11_dir: Path = args.out_v11

    if not entities_file.is_file():
        print(f"[FATAL] --entities-file not found: {entities_file}", file=sys.stderr)
        return 1

    legacy_dir.mkdir(parents=True, exist_ok=True)
    v11_dir.mkdir(parents=True, exist_ok=True)

    entities = read_entities(entities_file)
    if not entities:
        print(f"[WARN] No entities found in {entities_file}")
        return 0

    print(f"[INFO] Base URL: {args.base_url}")
    print(f"[INFO] Ledger ID: {args.ledger_id}")
    print(f"[INFO] Entities: {len(entities)}")

    ok = 0
    fail = 0
    for eid in entities:
        if export_and_migrate_entity(
            base_url=args.base_url,
            api_key=args.api_key,
            entity_id=eid,
            ledger_id=args.ledger_id,
            legacy_dir=legacy_dir,
            v11_dir=v11_dir,
            default_tier=args.tier,
            default_lawfulness=args.lawfulness,
        ):
            ok += 1
        else:
            fail += 1

    print(f"\nDone. Exported+Migrated: {ok}, Failed: {fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
