import asyncio
import traceback
import pycommons.logger
from seriesdb.client import Client

logger = pycommons.logger.get_instance(__name__)

count = 0


async def rename_table():
    loop = asyncio.get_event_loop()
    client = Client("localhost:8888", loop=loop)

    while True:
        try:
            old_table = "huobi.btc.usdt.1m"
            new_table = "huobi.btc.usdt.3m"
            await client.rename_table(old_table, new_table)
        except Exception:
            logger.error("Failed to check: %s", traceback.format_exc())

        await asyncio.sleep(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(rename_table())
    loop.run_forever()
