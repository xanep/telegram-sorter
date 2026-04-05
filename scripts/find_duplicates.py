#!/usr/bin/env python3
"""
scripts/find_duplicates.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Scans every configured destination channel for duplicate messages
(same text posted more than once) and reports when they appeared.

Cross-reference the timestamps with the git log to pinpoint which
commit introduced the duplicates.

Usage:
    python scripts/find_duplicates.py [--limit 500]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from telethon import TelegramClient
from telethon.sessions import StringSession
from src.config import AppConfig, CategoryConfig

SEP = "=" * 70


@dataclass
class MsgRecord:
    dest_msg_id: int
    date: object
    text: str


async def scan_channel(
    client: TelegramClient,
    cat: CategoryConfig,
    limit: int,
) -> list[tuple[str, list[MsgRecord]]]:
    """Return list of (text, [records]) for every text that appears >1 time."""
    try:
        dest = int(cat.channel_id)
    except (ValueError, TypeError):
        dest = cat.channel_id

    by_text: dict[str, list[MsgRecord]] = defaultdict(list)
    total = 0
    async for msg in client.iter_messages(dest, limit=limit):
        text = (msg.raw_text or "").strip()
        if not text:
            continue
        by_text[text].append(MsgRecord(msg.id, msg.date, text))
        total += 1

    duplicates = [
        (text, sorted(records, key=lambda r: r.date))
        for text, records in by_text.items()
        if len(records) > 1
    ]
    duplicates.sort(key=lambda x: x[1][0].date)  # oldest first

    print(f"  Scanned {total} text-bearing messages, found {len(duplicates)} duplicate text(s).")
    return duplicates


async def main(limit: int) -> None:
    config = AppConfig.load()
    client = TelegramClient(
        StringSession(config.telegram.session_string or None),
        config.telegram.api_id,
        config.telegram.api_hash,
    )

    print("Connecting to Telegram…")
    await client.start()
    me = await client.get_me()
    print(f"Logged in as  : {me.username or me.first_name}")
    print(f"Scanning last {limit} messages per destination channel.\n")

    any_found = False
    for cat in config.categories:
        if not cat.is_configured:
            print(f"⚠️  '{cat.name}': not configured, skipping.")
            continue

        print(f"\n{SEP}")
        print(f"  {cat.name}  ({cat.channel_id})")
        print(SEP)

        duplicates = await scan_channel(client, cat, limit)

        if not duplicates:
            print("  ✅  No duplicates found.")
            continue

        any_found = True
        print(f"\n  {'#':<4} {'dest_id':>7}  {'posted (destination)':^22}  text preview")
        print(f"  {'-'*4}  {'-'*7}  {'-'*22}  {'-'*35}")

        for text, records in duplicates:
            preview = text[:60].replace("\n", " ")
            print(f"\n  ── {len(records)}× ── \"{preview}{'…' if len(text) > 60 else ''}\"")
            for i, r in enumerate(records):
                marker = "1st" if i == 0 else f"{i+1}th" if i > 2 else ("2nd" if i == 1 else "3rd")
                print(f"  {marker:<4}  {r.dest_msg_id:>7}  {r.date:%Y-%m-%d %H:%M:%S %Z}")

    print(f"\n{SEP}")
    if any_found:
        print("  ❌  Duplicates found — compare timestamps above with `git log`:")
        print()
        print("       Commit times (UTC−7):")
        print("         aec248f  2026-04-03 19:33  feat: strict ordering (seq + priority queue)")
        print("         83b77c3  2026-04-03 16:24  feat: catch-up ordering + verify_order script")
        print("         893b702  2026-04-01 19:05  fix: album grouping")
        print("         f95e61b  2026-03-31 08:50  Initial commit")
    else:
        print("  ✅  No duplicates found in any channel.")
    print(f"{SEP}\n")

    await client.disconnect()
    sys.exit(1 if any_found else 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find duplicate messages in destination channels.")
    parser.add_argument(
        "--limit", type=int, default=500,
        help="Messages to scan per channel (default: 500)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.limit))
