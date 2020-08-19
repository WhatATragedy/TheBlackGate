from configparser import ConfigParser
import aiopg
import asyncio
import time

class Async_Postgres():
    def  __init__(self,):
        self.dsn = 'dbname={database} user={user} host={host}'.format(**CONN_INFO)
    
    def read_config(self, filename='components/configs/database.ini', section='postgresql'):
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
    CONN_INFO = {
    'host': 'POSTGRESQL_SERVER',
    'user': 'user_name',
    'port': 1234,
    'database': 'some_dabase',
}

dsn = 'dbname={database} user={user} host={host}'.format(**self.read_config())


async def insert_data(pool, line):
    start = time()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('SELECT * FROM some_table limit 100000')
            result = await cur.fetchall()
    print(f'there are {len(result)} records. Exec time: {time() - start}')
    return result


async def main():
    pool = await aiopg.create_pool(dsn)

    start = time()
    tasks = []

    for i in range(3):
        tasks.append(loop.create_task(get_data(pool)))

    tasks, stat = await asyncio.wait(tasks)

    for task in tasks:
        print(f'number of items: {len(task.result())}')

    print(f'total exec time: {time() - start} secs')

    print('exiting main')


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()

print('exiting program')