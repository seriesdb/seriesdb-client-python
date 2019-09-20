import asyncio
import traceback
import struct
import pycommons.logger
from seriesdb.client import Client

logger = pycommons.logger.get_instance(__name__)

initial = 1
step = 1
count = 0


def build_rows():
    keys = []
    values = []
    global initial
    for i in range(initial, initial+step):
        keys.append(struct.pack('>I', i))
        value = [i, 100.01, 100.02, 99.01, 99.02, 100, 0]
        values.append(struct.pack('%sf' % len(value), *value))
    initial += step
    # logger.info("%s", rows)
    return keys, values


async def set_rows():
    loop = asyncio.get_event_loop()
    client = Client("localhost:8888", loop=loop)
    global count

    while True:
        try:
            keys, values = build_rows()
            await client.set_rows("huobi.btc.usdt.1m", keys, values)
            count += 1
            if count % 1000 == 0:
                logger.info("count: %s", count)
        except Exception:
            logger.error("Failed to check: %s", traceback.format_exc())

        # await asyncio.sleep(1)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(set_rows())
    loop.run_forever()
