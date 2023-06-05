"""
HTTP Transport

EXPORT and IMPORT from Exasol to Polars DataFrames
"""

import pyexasol
import _config as config

# Connect with compression enabled
C = pyexasol.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
                     compression=True)

C.execute('TRUNCATE TABLE users_copy')

# Export from Exasol table into polars.DataFrame
pd = C.export_to_polars('users')
print(pd)

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Import from polars DataFrame into Exasol table
C.import_from_polars(pd, 'users_copy')

stmt = C.last_statement()
print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')

# Export from SQL query
pd = C.export_to_polars('SELECT user_id, user_name FROM users WHERE user_id >= 5000')
print(pd)

stmt = C.last_statement()
print(f'EXPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
