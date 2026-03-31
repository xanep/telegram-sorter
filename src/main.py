"""
Entry point for all three services.

Select which component to run via the SERVICE environment variable:
  - listener   : Telethon client → reads @stranaua → pushes to Redis
  - classifier  : reads Redis → calls OpenAI → pushes classified result
  - router      : reads Redis → copies messages to category channels via Bot API
  - all         : runs all three in a single process (default, good for local dev)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

# Load .env before anything reads os.environ (config expansion, OpenAI key, …)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(name)s: %(message)s",
    stream=sys.stdout,
)

from src.config import AppConfig  # noqa: E402  (must come after load_dotenv)


async def _run_listener(config: AppConfig) -> None:
    from src.listener import ListenerService
    await ListenerService(config).run()


async def _run_classifier(config: AppConfig) -> None:
    from src.classifier import ClassifierService
    await ClassifierService(config).run()


async def _run_router(config: AppConfig) -> None:
    from src.router import RouterService
    svc = RouterService(config)
    try:
        await svc.run()
    finally:
        await svc.close()


_SERVICES: dict[str, list] = {
    "listener":   [_run_listener],
    "classifier": [_run_classifier],
    "router":     [_run_router],
    "all":        [_run_listener, _run_classifier, _run_router],
}


async def main() -> None:
    config = AppConfig.load()
    service = os.environ.get("SERVICE", "all").lower()

    if service not in _SERVICES:
        logging.error(
            "Unknown SERVICE=%r. Valid values: %s",
            service, ", ".join(_SERVICES),
        )
        sys.exit(1)

    runners = _SERVICES[service]
    logging.info("Starting service(s): %s", service)

    # Run all selected coroutines concurrently; any unhandled exception
    # propagates and will restart the pod in Kubernetes.
    await asyncio.gather(*[fn(config) for fn in runners])


if __name__ == "__main__":
    asyncio.run(main())

