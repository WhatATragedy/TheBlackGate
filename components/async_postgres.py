from configparser import ConfigParser
import asyncpg
import asyncio
import time
from rib import RibConsumer
import logging
from logging.config import fileConfig
from aiologger import Logger
import uvloop
import os 
import aiofiles
import datetime

def read_config(filename='configs/database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db_params = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_params[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db_params
    
async def streamer(input_queue):
    #this is going to stream data into the input queue
    logger.debug("Streamer Spawned...")
    batch = []
    sql_id = 0
    async with aiofiles.open(f"/home/ec2-user/TheBlackGate/ribs/route-views.kixp/rib.20200819.0000", mode='r') as f:
        async for line in f:
            if len(batch) > 10000:
                logger.debug("Batch Ready")
                await input_queue.put(batch.copy())
                batch.clear()
            _, timestamp, _, collect_ip, collect_asn, prefix, path, _ = line.split('|')
            timestamp = datetime.datetime.strptime(timestamp.replace('/', '-'), "%m-%d-%y %H:%M:%S")
            batch.append((timestamp, collect_ip, int(collect_asn), prefix, path))
    await input_queue.put(None)
                    

async def consumer(pool, input_queue):
    #This is going to pull from input_queue and generate tasks
    logger.debug("Consumer Spawned...")
    while True:
        batch = await input_queue.get()
        if batch is None:
            break 
            logger.debug("Queue Empty")
        asyncio.create_task(send_batch(batch, input_queue, pool))

async def send_batch(batch, input_queue, pool):
    operation = \
        """
        INSERT INTO ribs (timestamp, collect_ip, collect_asn, prefix, path)
        VALUES ( $1, $2, $3, $4, $5);
        """
    try:
        async with pool.acquire() as conn:
            await conn.copy_records_to_table("ribs", records=batch)
            batch.clear()
    except Exception as e:
        logger.debug(e)
    finally:
        input_queue.task_done()
    
async def write_tables(conn):
    await conn.execute(
        """
        DROP TABLE IF EXISTS ribs;
        CREATE TABLE ribs (
            Collect_Date TIMESTAMP NOT NULL,
            Collect_IP INET NOT NULL,
            Collect_ASN bigint NOT NULL,
            Prefix INET NOT NULL,
            Path TEXT
        )
        """
    )
async def main():
    logger.debug("Starting Up...")
    rib_files = RibConsumer.current_rib_files()
    print(rib_files)

    # uvloop is a faster eventloop implenation
    uvloop.install()

    params = read_config()
    pg_pool = await asyncpg.create_pool(
        **params
    )
    async with pg_pool.acquire() as conn:
        await write_tables(conn)

    loop = asyncio.get_event_loop()
    queue = asyncio.Queue(loop = loop)
    consumer_func = consumer(pg_pool, queue)
    streamer_func = streamer(queue)
    #understand more about ensure futurue
    consumer_fut = asyncio.ensure_future(consumer_func)
    await streamer_func
    #what does queue  join do
    await queue.join()
    consumer_fut.cancel()
    
if __name__ == "__main__":

    loop = asyncio.get_event_loop()

    # Use an async logger
    logger = Logger.with_default_handlers(loop = loop)

    # uvloop is a faster eventloop implenation
    uvloop.install()

    # This try accept loop does not deal with shutting down coroutines properly. Needs fixing.
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        tasks = asyncio.all_tasks()
        for task in tasks:
            task.cancel()
    finally:
        loop.run_until_complete(logger.shutdown())
        loop.close()



