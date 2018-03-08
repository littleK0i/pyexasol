"""
Example 2
Fetching data as tuples
"""

import pyexasol as E
import _config as config

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)

# Basic connect (default mapper)
C = E.connect(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)

# Fetch tuples row-by-row as iterator
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")

for row in stmt:
    printer.pprint(row)

# Fetch tuples row-by-row with fetchone
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")

while True:
    row = stmt.fetchone()

    if row is None:
        break

    printer.pprint(row)

# Fetch many
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchmany(3))
printer.pprint(stmt.fetchmany(3))

# Fetch everything in one go
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchall())

# Fetch one column as list of values
stmt = C.execute("SELECT user_id FROM users ORDER BY user_id LIMIT 5")
printer.pprint(stmt.fetchcol())

# Fetch single value
stmt = C.execute("SELECT count(*) FROM users")
printer.pprint(stmt.fetchval())
