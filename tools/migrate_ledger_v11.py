#!/usr/bin/env python
"""Migrate legacy ledger JSON files to the Dual-Substrate v1.1 schema."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from pydantic import ValidationError

from app.models import DSubstrateEntity


def _now_iso() -> str:
    """Return the current UTC time in ISO-8601 format with ``Z`` suffix."""

    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _coalesce_slots(payload: Dict[str, Any]) -> Dict[str, Any]:
    slots = payload.get("slots")
    if isinstance(slots, dict):
        return slots
    return {"S1": {}, "S2": {}, "body": {}}


def _coalesce_metrics(payload: Dict[str, Any]) -> Dict[str, float]:
    metrics = payload.get("r_metrics")
    if isinstance(metrics, dict):
        return metrics
    return {"dE": 0.0, "dDrift": 0.0, "dRetention": 0.0, "K": 0.0}


def migrate_payload(
    raw: Dict[str, Any],
    *,
    default_tier: str = "S1",
    default_lawfulness: int = 1,
) -> DSubstrateEntity:
    """Coerce a legacy ledger payload into a validated ``DSubstrateEntity``."""

    entity_id = raw.get("entity") or raw.get("id") or "unknown-entity"
    created_at = raw.get("created_at") or _now_iso()
    updated_at = raw.get("updated_at") or created_at

    base_payload: Dict[str, Any] = {
        "entity": entity_id,
        "version": "1.1",
        "tier": raw.get("tier", default_tier),
        "lawfulness": raw.get("lawfulness", default_lawfulness),
        "created_at": created_at,
        "updated_at": updated_at,
        "factors": raw.get("factors", []),
        "meta": raw.get("meta", {}),
        "slots": _coalesce_slots(raw),
        "r_metrics": _coalesce_metrics(raw),
    }

    return DSubstrateEntity.parse_obj(base_payload)


def migrate_file(
    source: Path,
    destination_dir: Path,
    *,
    default_tier: str = "S1",
    default_lawfulness: int = 1,
) -> bool:
    """Migrate a single JSON ledger file. Returns ``True`` on success."""

    try:
        with source.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except Exception as exc:  # pragma: no cover - defensive I/O guard
        print(f"[ERROR] Failed to read {source}: {exc}", file=sys.stderr)
        return False

    try:
        entity = migrate_payload(
            raw,
            default_tier=default_tier,
            default_lawfulness=default_lawfulness,
        )
    except ValidationError as exc:
        print(f"[ERROR] Validation failed for {source}:\n{exc}\n", file=sys.stderr)
        return False

    destination = destination_dir / source.name
    try:
        with destination.open("w", encoding="utf-8") as handle:
            json.dump(entity.dict(), handle, indent=2, ensure_ascii=False)
    except Exception as exc:  # pragma: no cover - defensive I/O guard
        print(f"[ERROR] Failed to write {destination}: {exc}", file=sys.stderr)
        return False

    print(f"[OK] {source.name} -> {destination}")
    return True


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate legacy ledger JSON files to the Dual-Substrate v1.1 schema.",
    )
    parser.add_argument(
        "--in-dir",
        type=Path,
        required=True,
        help="Directory containing legacy ledger JSON files",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Directory to write migrated JSON files",
    )
    parser.add_argument(
        "--tier",
        type=str,
        default="S1",
        help="Default tier applied when legacy payloads omit the field",
    )
    parser.add_argument(
        "--lawfulness",
        type=int,
        default=1,
        help="Default lawfulness value (0-3) when absent in the legacy payload",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    in_dir: Path = args.in_dir
    out_dir: Path = args.out_dir
    default_tier: str = args.tier
    default_lawfulness: int = args.lawfulness

    if not in_dir.is_dir():
        print(f"[FATAL] --in-dir is not a directory: {in_dir}", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)

    json_files = sorted(path for path in in_dir.glob("*.json") if path.is_file())
    if not json_files:
        print(f"[WARN] No *.json files found in {in_dir}")
        return 0

    succeeded = 0
    failed = 0
    for source in json_files:
        if migrate_file(
            source,
            out_dir,
            default_tier=default_tier,
            default_lawfulness=default_lawfulness,
        ):
            succeeded += 1
        else:
            failed += 1

    print(f"\nDone. Migrated: {succeeded}, Failed: {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
