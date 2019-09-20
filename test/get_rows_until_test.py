import asyncio
import traceback
import struct
import pycommons.logger
from seriesdb.client import Client

logger = pycommons.logger.get_instance(__name__)

count = 0


async def get_rows_until():
    loop = asyncio.get_event_loop()
    client = Client("localhost:8888", loop=loop)
    global count

    while True:
        try:
            table = "huobi.btc.usdt.1m"
            key = struct.pack('>I', 3)
            rows = await client.get_rows_until(table, key, 10)
            logger.info("Received rows: %s", rows)
            count += 1
            if count % 1000 == 0:
                logger.info("count: %s", count)
        except Exception:
            logger.error("Failed to check: %s", traceback.format_exc())

        await asyncio.sleep(1)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(get_rows_until())
    loop.run_forever()
