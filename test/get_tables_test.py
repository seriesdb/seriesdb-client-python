import asyncio
import traceback
import pycommons.logger
from seriesdb.client import Client

logger = pycommons.logger.get_instance(__name__)

count = 0


async def get_tables():
    loop = asyncio.get_event_loop()
    client = Client("localhost:8888", loop=loop)

    while True:
        try:
            names, ids = await client.get_tables()
            logger.info("Received names: %s, ids: %s", names, ids)
        except Exception:
            logger.error("Failed to check: %s", traceback.format_exc())

        await asyncio.sleep(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(get_tables())
    loop.run_forever()
