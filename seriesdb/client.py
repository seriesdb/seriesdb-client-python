import asyncio
import pycommons.logger
import seriesdb.protocol.seriesdb_protocol_pb2 as protocol_types
from seriesdb.connection import Connection

logger = pycommons.logger.get_instance(__name__)


class Client(object):

    # ===========================================
    # APIs
    # ===========================================
    def __init__(self, endpoint, options=None, loop=None):
        self.__endpoint = endpoint
        self.__init_options(options)
        self.__init_loop(loop)

        self.__init_connection()

    def close(self):
        self.__connection.close()

    async def set_rows(self, table, keys, values):
        await self.__connection.wait_until_open()
        await self.__connection.request(
            self.__build_set_rows_req(table, keys, values)
        )

    async def delete_rows_since(self, table, key, limit):
        await self.__connection.wait_until_open()
        await self.__connection.request(
            self.__build_delete_rows_since_req(table, key, limit)
        )

    async def get_first_row(self, table):
        await self.__connection.wait_until_open()
        get_first_row_rep = await self.__connection.request(
            self.__build_get_first_row_req(table)
        )
        if get_first_row_rep.key == b'':
            return None
        return (get_first_row_rep.key, get_first_row_rep.value)

    async def get_last_row(self, table):
        await self.__connection.wait_until_open()
        get_last_row_rep = await self.__connection.request(
            self.__build_get_last_row_req(table)
        )
        if get_last_row_rep.key == b'':
            return None
        return (get_last_row_rep.key, get_last_row_rep.value)

    async def get_boundary_rows(self, table):
        await self.__connection.wait_until_open()
        get_boundary_rows_rep = await self.__connection.request(
            self.__build_get_boundary_rows_req(table)
        )
        return (
            get_boundary_rows_rep.first_key,
            get_boundary_rows_rep.first_value,
            get_boundary_rows_rep.last_key,
            get_boundary_rows_rep.last_value
        )

    async def get_rows_since(self, table, key, limit):
        await self.__connection.wait_until_open()
        get_rows_since_rep = await self.__connection.request(
            self.__build_get_rows_since_req(table, key, limit)
        )
        return (get_rows_since_rep.keys, get_rows_since_rep.values)

    async def get_rows_until(self, table, key, limit):
        await self.__connection.wait_until_open()
        get_rows_until_rep = await self.__connection.request(
            self.__build_get_rows_until_req(table, key, limit)
        )
        return (get_rows_until_rep.keys, get_rows_until_rep.values)

    async def get_rows_until_last(self, table, limit):
        await self.__connection.wait_until_open()
        get_rows_until_last_rep = await self.__connection.request(
            self.__build_get_rows_until_last_req(table, limit)
        )
        return (get_rows_until_last_rep.keys, get_rows_until_last_rep.values)

    async def get_rows_between(self, table, begin_key, end_key, limit):
        await self.__connection.wait_until_open()
        get_rows_between_rep = await self.__connection.request(
            self.__build_get_rows_between_req(
                table, begin_key, end_key, limit)
        )
        return (get_rows_between_rep.keys, get_rows_between_rep.values)

    async def get_first_key(self, table):
        await self.__connection.wait_until_open()
        get_first_key_rep = await self.__connection.request(
            self.__build_get_first_key_req(table)
        )
        if get_first_key_rep.key == b'':
            return None
        return get_first_key_rep.key

    async def get_last_key(self, table):
        await self.__connection.wait_until_open()
        get_last_key_rep = await self.__connection.request(
            self.__build_get_last_key_req(table)
        )
        if get_last_key_rep.key == b'':
            return None
        return get_last_key_rep.key

    async def get_boundary_keys(self, table):
        await self.__connection.wait_until_open()
        get_boundary_keys_rep = await self.__connection.request(
            self.__build_get_boundary_keys_req(table)
        )
        if get_boundary_keys_rep.first_key == b'':
            return None
        return (
            get_boundary_keys_rep.first_key,
            get_boundary_keys_rep.last_key
        )

    async def get_value(self, table, key):
        await self.__connection.wait_until_open()
        get_value_rep = await self.__connection.request(
            self.__build_get_value_req(table, key)
        )
        if get_value_rep.value == b'':
            return None
        return get_value_rep.value

    async def get_nth_last_value(self, table, n):
        await self.__connection.wait_until_open()
        get_nth_last_value_rep = await self.__connection.request(
            self.__build_get_nth_last_value_req(table, n)
        )
        if get_nth_last_value_rep.value == b'':
            return None
        return get_nth_last_value_rep.value

    async def get_values_since(self, table, key, limit):
        await self.__connection.wait_until_open()
        get_values_since_rep = await self.__connection.request(
            self.__build_get_values_since_req(table, key, limit)
        )
        return get_values_since_rep.values

    async def get_values_until(self, table, key, limit):
        await self.__connection.wait_until_open()
        get_values_until_rep = await self.__connection.request(
            self.__build_get_values_until_req(table, key, limit)
        )
        return get_values_until_rep.values

    async def get_values_until_last(self, table, limit):
        await self.__connection.wait_until_open()
        get_values_until_last_rep = await self.__connection.request(
            self.__build_get_values_until_last_req(table, limit)
        )
        return get_values_until_last_rep.values

    async def get_values_between(self, table, begin_key, end_key, limit):
        await self.__connection.wait_until_open()
        get_values_between = await self.__connection.request(
            self.__build_get_values_between_req(
                table, begin_key, end_key, limit)
        )
        return get_values_between.values

    async def destroy_table(self, table):
        await self.__connection.wait_until_open()
        return await self.__connection.request(
            self.__build_destroy_table_req(table)
        )

    async def rename_table(self, old_table, new_table):
        await self.__connection.wait_until_open()
        return await self.__connection.request(
            self.__build_rename_table_req(old_table, new_table)
        )

    async def get_tables(self):
        await self.__connection.wait_until_open()
        get_tables_rep = await self.__connection.request(
            self.__build_get_tables_req()
        )
        return (get_tables_rep.names, get_tables_rep.ids)

    # ===========================================
    #  Init functions
    # ===========================================

    def __init_options(self, options):
        options = options if options else {}
        if options.get('check_interval') == None:
            options['check_interval'] = 1
        if options.get('ping_interval') == None:
            options['ping_interval'] = 10
        if options.get('max_idle_period') == None:
            options['max_idle_period'] = 15
        if options.get('default_round_timeout') == None:
            options['default_round_timeout'] = 5
        self.__options = options

    def __init_loop(self, loop):
        self.__loop = loop if loop else asyncio.get_event_loop()

    def __init_connection(self):
        self.__connection = Connection(
            self.__endpoint, self.__options, self.__loop
        )

    # ===========================================
    # req builders
    # ===========================================

    def __build_set_rows_req(self, table, keys, values):
        set_rows_req = protocol_types.SetRowsReq()
        set_rows_req.table = table
        set_rows_req.keys.extend(keys)
        set_rows_req.values.extend(values)
        return set_rows_req

    def __build_delete_rows_since_req(self, table, key, limit):
        delete_rows_since_req = protocol_types.DeleteRowsSinceReq()
        delete_rows_since_req.table = table
        delete_rows_since_req.key = key
        delete_rows_since_req.limit = limit
        return delete_rows_since_req

    def __build_get_first_row_req(self, table):
        get_first_row_req = protocol_types.GetFirstRowReq()
        get_first_row_req.table = table
        return get_first_row_req

    def __build_get_last_row_req(self, table):
        get_last_row_req = protocol_types.GetLastRowReq()
        get_last_row_req.table = table
        return get_last_row_req

    def __build_get_boundary_rows_req(self, table):
        get_boundary_rows_req = protocol_types.GetBoundaryRowsReq()
        get_boundary_rows_req.table = table
        return get_boundary_rows_req

    def __build_get_rows_since_req(self, table, key, limit):
        get_rows_since_req = protocol_types.GetRowsSinceReq()
        get_rows_since_req.table = table
        get_rows_since_req.key = key
        get_rows_since_req.limit = limit
        return get_rows_since_req

    def __build_get_rows_until_req(self, table, key, limit):
        get_rows_until_req = protocol_types.GetRowsUntilReq()
        get_rows_until_req.table = table
        get_rows_until_req.key = key
        get_rows_until_req.limit = limit
        return get_rows_until_req

    def __build_get_rows_until_last_req(self, table, limit):
        get_rows_until_last_req = protocol_types.GetRowsUntilLastReq()
        get_rows_until_last_req.table = table
        get_rows_until_last_req.limit = limit
        return get_rows_until_last_req

    def __build_get_rows_between_req(self, table, begin_key, end_key, limit):
        get_rows_between_req = protocol_types.GetRowsBetweenReq()
        get_rows_between_req.table = table
        get_rows_between_req.begin_key = begin_key
        get_rows_between_req.end_key = end_key
        get_rows_between_req.limit = limit
        return get_rows_between_req

    def __build_get_first_key_req(self, table):
        get_first_key_req = protocol_types.GetFirstKeyReq()
        get_first_key_req.table = table
        return get_first_key_req

    def __build_get_last_key_req(self, table):
        get_last_key_req = protocol_types.GetLastKeyReq()
        get_last_key_req.table = table
        return get_last_key_req

    def __build_get_boundary_keys_req(self, table):
        get_boundary_keys_req = protocol_types.GetBoundaryKeysReq()
        get_boundary_keys_req.table = table
        return get_boundary_keys_req

    def __build_get_value_req(self, table, key):
        get_value_req = protocol_types.GetValueReq()
        get_value_req.table = table
        get_value_req.key = key
        return get_value_req

    def __build_get_nth_last_value_req(self, table, n):
        get_nth_last_value_req = protocol_types.GetNthLastValueReq()
        get_nth_last_value_req.table = table
        get_nth_last_value_req.n = n
        return get_nth_last_value_req

    def __build_get_values_since_req(self, table, key, limit):
        get_values_since_req = protocol_types.GetValuesSinceReq()
        get_values_since_req.table = table
        get_values_since_req.key = key
        get_values_since_req.limit = limit
        return get_values_since_req

    def __build_get_values_until_req(self, table, key, limit):
        get_values_until_req = protocol_types.GetValuesUntilReq()
        get_values_until_req.table = table
        get_values_until_req.key = key
        get_values_until_req.limit = limit
        return get_values_until_req

    def __build_get_values_until_last_req(self, table, limit):
        get_values_until_last_req = protocol_types.GetValuesUntilLastReq()
        get_values_until_last_req.table = table
        get_values_until_last_req.limit = limit
        return get_values_until_last_req

    def __build_get_values_between_req(self, table, begin_key, end_key, limit):
        get_values_between_req = protocol_types.GetValuesBetweenReq()
        get_values_between_req.table = table
        get_values_between_req.begin_key = begin_key
        get_values_between_req.end_key = end_key
        get_values_between_req.limit = limit
        return get_values_between_req

    def __build_destroy_table_req(self, table):
        destroy_table_req = protocol_types.DestroyTableReq()
        destroy_table_req.table = table
        return destroy_table_req

    def __build_rename_table_req(self, old_table, new_table):
        rename_table_req = protocol_types.RenameTableReq()
        rename_table_req.old_table = old_table
        rename_table_req.new_table = new_table
        return rename_table_req

    def __build_get_tables_req(self):
        get_tables_req = protocol_types.GetTablesReq()
        return get_tables_req
