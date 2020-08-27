from configparser import ConfigParser
import asyncpg
import asyncio
import time
from rib import RibConsumer
import logging
from logging.config import fileConfig
from aiologger import Logger

class Async_Postgres():
    def  __init__(self,):
        params = self.read_config()
        self.dsn = 'dbname={database} user={user} host={host}'.format(**params)
    
    def read_config(self, filename='configs/database.ini', section='postgresql'):
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
    @staticmethod
    async def streamer(input_queue):
        #this is going to stream data into the input queue
        logger.debug("Streamer Spawned...")
        rib_instance = RibConsumer()
        batch = []
        for index, line in enumerate(rib_instance.reader("E:\Stuff\Code\TheBlackGate\\ribs")):
            if index == 5000:
                #batch ready
                await input_queue.put(batch.copy())
                batch.clear()
            _, timestamp, _, collect_ip, collect_asn, prefix, path, _ = line.split('|')
            batch.append((timestamp, collect_ip, collect_asn, prefix, path))

    @staticmethod
    async def consumer(pool, input_queue):
        #This is going to pull from input_queue and generate tasks
        logger.debug("Consumer Spawned...")
        while True:
            batch = await input_queue.get()
            asyncio.create_task(Async_Postgres.convert_and_send_batch(pool, batch, input_queue))

    @staticmethod
    async def convert_and_send_batch(pool, batch, input_queue):
        ##do some parsing of the batch here but we don't need to
        asyncio.create_task(Async_Postgres.send_batch(batch, input_queue, pool))
        

    @staticmethod
    async def send_batch(batch, input_queue, pool):
        operation = \
            """
            INSERT INTO ribs (timestamp, collect_ip, collect_asn, prefix, path)
            VALUES ( $1, $2, $3, $4, $5);
            """
        logger.debug("Batch Being Sent..")
        sem = asyncio.Semaphore(3)
        try:
            async with sem:
                async with pool.acquire() as conn:
                    await conn.copy_records_to_table("ribs", records=batch)
                    batch.clear()
        except Exception as e:
            print(e)
        finally:
            input_queue.task_done()

    @staticmethod       
    async def write_tables(conn):
        await conn.execute(
            """
            DROP TABLE IF EXISTS ribs;
            CREATE TABLE ribs (
                Route_ID SERIAL PRIMARY KEY,
                Collect_Date DATE NOT NULL,
                Collect_IP INET NOT NULL,
                Collect_ASN bigint NOT NULL,
                Prefix INET NOT NULL,
                Path TEXT
            )
            """
        )

    async def main(self):
        params = self.read_config()
        pg_pool = await asyncpg.create_pool(
            **params
        )
        async with pg_pool.acquire() as conn:
            await Async_Postgres.write_tables(conn)
        queue = asyncio.Queue()
        asyncio.create_task(Async_Postgres.consumer(pg_pool, queue))
        data_streams = asyncio.create_task(Async_Postgres.streamer(queue))
        await asyncio.gather(data_streams)
        await logger.shutdown()


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    logger = Logger.with_default_handlers (loop = loop )
    logger.debug("Starting Up...")
    pg = Async_Postgres()
    try:
        loop.run_until_complete(pg.main())
    except KeyboardInterrupt:
        tasks = asyncio.all_tasks()
        for task in tasks:
            task.cancel()
    finally:
        loop.close()



