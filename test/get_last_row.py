import asyncio
import traceback
import pycommons.logger
from seriesdb.client import Client

logger = pycommons.logger.get_instance(__name__)

count = 0


async def get_last_row():
    loop = asyncio.get_event_loop()
    client = Client("localhost:8888", loop=loop)
    global count

    while True:
        try:
            table = "huobi.btc.usdt.1m"
            row = await client.get_last_row(table)
            logger.info("Received row: %s", row)
            count += 1
            if count % 1000 == 0:
                logger.info("count: %s", count)
        except Exception:
            logger.error("Failed to check: %s", traceback.format_exc())

        await asyncio.sleep(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(get_last_row())
    loop.run_forever()