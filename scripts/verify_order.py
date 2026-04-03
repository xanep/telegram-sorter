#!/usr/bin/env python3
"""
scripts/verify_order.py
~~~~~~~~~~~~~~~~~~~~~~~
Verifies that messages in each destination channel appear in the same
chronological order as they do in the source channel (@stranaua).

Usage:
    python scripts/verify_order.py [--limit 50] [--source-limit 500]

Because the router uses forward_messages(drop_author=True), destination
messages look like native posts (no fwd_from metadata).  The script
matches them back to the source by text content:

  1. Scans the last --source-limit messages from @stranaua and builds a
     text → source_msg_id index.
  2. For each destination channel, fetches the last --limit messages.
  3. For every destination message whose text appears in the source index,
     records the corresponding source message ID.
  4. Checks that those source IDs are strictly increasing — i.e. the
     destination channel preserved the original source ordering.
  5. Reports any out-of-order pairs with enough context to debug them.

Media-only messages (no caption/text) cannot be matched and are skipped.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

# Allow running as  `python scripts/verify_order.py`  from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from telethon import TelegramClient
from telethon.sessions import StringSession

from src.config import AppConfig, CategoryConfig

SEP = "=" * 62


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

@dataclass
class SourceEntry:
    msg_id: int
    text: str    # normalised caption/text used as the matching key
    date: object


@dataclass
class DestEntry:
    dest_msg_id: int
    text: str
    date: object
    source_msg_id: int | None = None   # filled in after matching


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalise(text: str | None) -> str:
    """Strip whitespace for reliable text matching."""
    return (text or "").strip()


async def build_source_index(
    client: TelegramClient,
    source_channel: str,
    source_limit: int,
) -> dict[str, SourceEntry]:
    """
    Fetch the last `source_limit` messages from the source channel and return
    a dict keyed by normalised text.  Only messages with non-empty text are
    indexed (media-only messages can't be matched by text).
    """
    print(f"Scanning last {source_limit} messages from @{source_channel}…", end=" ", flush=True)
    index: dict[str, SourceEntry] = {}
    async for msg in client.iter_messages(source_channel, limit=source_limit):
        text = _normalise(msg.raw_text)
        if text:
            # Keep the first occurrence (lowest msg_id) for a given text in
            # case of rare duplicates — oldest wins.
            index.setdefault(text, SourceEntry(msg.id, text, msg.date))
    print(f"{len(index)} text-bearing messages indexed.")
    return index


# ---------------------------------------------------------------------------
# Per-channel check
# ---------------------------------------------------------------------------

async def check_channel(
    client: TelegramClient,
    cat: CategoryConfig,
    source_index: dict[str, SourceEntry],
    limit: int,
) -> bool:
    """Return True when all matchable messages are in source order."""
    print(f"\n{SEP}")
    print(f"  {cat.name}  ({cat.channel_id})")
    print(SEP)

    try:
        dest = int(cat.channel_id)
    except (ValueError, TypeError):
        dest = cat.channel_id

    # Fetch newest → oldest, then reverse to chronological order
    raw: list[DestEntry] = []
    async for msg in client.iter_messages(dest, limit=limit):
        raw.append(DestEntry(msg.id, _normalise(msg.raw_text), msg.date))
    entries = list(reversed(raw))

    if not entries:
        print("  ⚠️   Channel is empty — nothing to verify.")
        return True

    # Match each destination message back to its source by text content
    matched: list[DestEntry] = []
    no_text = 0
    no_match = 0
    for e in entries:
        if not e.text:
            no_text += 1
            continue
        src = source_index.get(e.text)
        if src is None:
            no_match += 1
            continue
        e.source_msg_id = src.msg_id
        matched.append(e)

    print(
        f"  {len(entries)} messages fetched: "
        f"{len(matched)} matched to source, "
        f"{no_text} media-only (skipped), "
        f"{no_match} not found in source index."
    )

    if not matched:
        print("  ⚠️   No messages could be matched — increase --source-limit or --limit.")
        return True   # inconclusive, not a failure

    # Check that matched source IDs are strictly increasing
    out_of_order: list[tuple[int, DestEntry, DestEntry]] = []
    for i in range(1, len(matched)):
        prev, curr = matched[i - 1], matched[i]
        if curr.source_msg_id <= prev.source_msg_id:
            out_of_order.append((i, prev, curr))

    if out_of_order:
        print(f"\n  ❌  FAIL — {len(out_of_order)} out-of-order pair(s):\n")
        for pos, prev, curr in out_of_order:
            print(
                f"     position {pos:>3}:  "
                f"dest={prev.dest_msg_id} → src={prev.source_msg_id}"
                f"  ({prev.date:%Y-%m-%d %H:%M})"
                f"\n               followed by  "
                f"dest={curr.dest_msg_id} → src={curr.source_msg_id}"
                f"  ({curr.date:%Y-%m-%d %H:%M})"
            )
        return False

    first, last = matched[0], matched[-1]
    print(
        f"\n  ✅  PASS — {len(matched)} messages in correct order  "
        f"(src IDs {first.source_msg_id} … {last.source_msg_id})"
    )
    return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main(limit: int, source_limit: int) -> None:
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
    print(f"Source channel: @{config.telegram.source_channel}")
    print(f"Dest limit    : last {limit} messages per channel")
    print()

    source_index = await build_source_index(
        client, config.telegram.source_channel, source_limit
    )

    all_passed = True
    for cat in config.categories:
        if not cat.is_configured:
            print(f"\n⚠️  '{cat.name}': channel not configured, skipping.")
            continue
        passed = await check_channel(client, cat, source_index, limit)
        if not passed:
            all_passed = False

    print(f"\n{SEP}")
    if all_passed:
        print("  ✅  All channels passed order verification.")
    else:
        print("  ❌  One or more channels have ordering issues — see above.")
    print(f"{SEP}\n")

    await client.disconnect()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Verify that destination channels preserve source message order."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Recent messages to check per destination channel (default: 50)",
    )
    parser.add_argument(
        "--source-limit",
        type=int,
        default=500,
        help="Recent source messages to index for matching (default: 500)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.limit, args.source_limit))
