import asyncio
import signal
from app.core.logger import logger
from app.streams.consumer import stream_consumer


def main():
    logger.info("Initializing standalone Redis Stream Consumer worker...")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    task = loop.create_task(stream_consumer.start())

    def shutdown(sig, frame):
        logger.info(f"Received shutdown signal ({sig}), initiating consumer stop...")
        task.cancel()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        logger.info("Redis Stream consumer event loop cancelled.")
    finally:
        loop.run_until_complete(stream_consumer.stop())
        loop.close()
        logger.info("Redis Stream consumer service cleanly terminated.")


if __name__ == "__main__":
    main()
