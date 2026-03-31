#!/usr/bin/env python3
"""
One-time Telethon session generator.

Run this script ONCE on your local machine before deploying to Kubernetes.
It will authenticate with Telegram interactively (phone number + SMS code),
then print a session string that you should store in:
  - .env  →  TELEGRAM_SESSION_STRING=<value>          (local dev)
  - K8s Secret  →  TELEGRAM_SESSION_STRING: <value>   (production)

Usage:
    python scripts/setup_session.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Make sure the repo root is on the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from telethon import TelegramClient
from telethon.sessions import StringSession

from src.config import AppConfig


async def generate() -> None:
    config = AppConfig.load()

    print()
    print("=== Telegram StringSession Generator ===")
    print(f"  API ID  : {config.telegram.api_id}")
    print()
    print("You will be asked for your phone number and the confirmation code")
    print("that Telegram sends via SMS or another Telegram client.")
    print()

    client = TelegramClient(
        StringSession(),
        config.telegram.api_id,
        config.telegram.api_hash,
    )

    await client.start()            # prompts: phone → code → (2FA password if set)
    session_string = client.session.save()
    await client.disconnect()

    print()
    print("=" * 60)
    print("SUCCESS — copy the line below into your .env file:")
    print("=" * 60)
    print(f"TELEGRAM_SESSION_STRING={session_string}")
    print("=" * 60)
    print()
    print("For Kubernetes, run:")
    print(
        "  kubectl create secret generic telegram-sorter-secrets \\\n"
        "    --from-literal=TELEGRAM_SESSION_STRING='<paste here>' \\\n"
        "    -n telegram-sorter"
    )


if __name__ == "__main__":
    asyncio.run(generate())

