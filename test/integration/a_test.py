import pytest
import pyexasol


@pytest.mark.a01_examples
def test_sorted_select_and_limited_select(connection):
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        (0, 'Jessica Mccoy', '2018-07-12', '2018-04-03 18:36:40.553000', True, '0.7', None, 'ACTIVE'),
        (1, 'Beth James', '2018-05-24', '2018-03-24 08:08:46.251000', False, '0.53', 22.07, 'ACTIVE'),
        (2, 'Mrs. Teresa Ryan', '2018-08-21', '2018-11-07 01:53:08.727000', False, '0.03', 24.88, 'PENDING'),
        (3, 'Tommy Henderson', '2018-04-18', '2018-04-28 21:39:59.300000', True, '0.5', 27.43, 'DISABLED'),
        (4, 'Jessica Christian', '2018-12-18', '2018-11-29 14:11:55.450000', True, '0.1', 62.59, 'SUSPENDED')
    ]
    actual = result.fetchall()
    assert expected == actual


@pytest.mark.a02_examples
def test_fetch_tuples_using_fetchone(connection):
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        (0, 'Jessica Mccoy', '2018-07-12', '2018-04-03 18:36:40.553000', True, '0.7', None, 'ACTIVE'),
        (1, 'Beth James', '2018-05-24', '2018-03-24 08:08:46.251000', False, '0.53', 22.07, 'ACTIVE'),
        (2, 'Mrs. Teresa Ryan', '2018-08-21', '2018-11-07 01:53:08.727000', False, '0.03', 24.88, 'PENDING'),
        (3, 'Tommy Henderson', '2018-04-18', '2018-04-28 21:39:59.300000', True, '0.5', 27.43, 'DISABLED'),
        (4, 'Jessica Christian', '2018-12-18', '2018-11-29 14:11:55.450000', True, '0.1', 62.59, 'SUSPENDED')
    ]
    actual = [row for row in iter(result.fetchone, None)]
    assert expected == actual


@pytest.mark.a02_examples
def test_fetch_tuples_using_fetchmany(connection):
    count = 3
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)

    expected = [
        (0, 'Jessica Mccoy', '2018-07-12', '2018-04-03 18:36:40.553000', True, '0.7', None, 'ACTIVE'),
        (1, 'Beth James', '2018-05-24', '2018-03-24 08:08:46.251000', False, '0.53', 22.07, 'ACTIVE'),
        (2, 'Mrs. Teresa Ryan', '2018-08-21', '2018-11-07 01:53:08.727000', False, '0.03', 24.88, 'PENDING'),
    ]
    actual = result.fetchmany(count)
    assert expected == actual

    expected = [
        (3, 'Tommy Henderson', '2018-04-18', '2018-04-28 21:39:59.300000', True, '0.5', 27.43, 'DISABLED'),
        (4, 'Jessica Christian', '2018-12-18', '2018-11-29 14:11:55.450000', True, '0.1', 62.59, 'SUSPENDED')
    ]
    actual = result.fetchmany(count)
    assert expected == actual

    
@pytest.mark.a02_examples
def test_fetch_tuples_using_fetchall(connection):
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        (0, 'Jessica Mccoy', '2018-07-12', '2018-04-03 18:36:40.553000', True, '0.7', None, 'ACTIVE'),
        (1, 'Beth James', '2018-05-24', '2018-03-24 08:08:46.251000', False, '0.53', 22.07, 'ACTIVE'),
        (2, 'Mrs. Teresa Ryan', '2018-08-21', '2018-11-07 01:53:08.727000', False, '0.03', 24.88, 'PENDING'),
        (3, 'Tommy Henderson', '2018-04-18', '2018-04-28 21:39:59.300000', True, '0.5', 27.43, 'DISABLED'),
        (4, 'Jessica Christian', '2018-12-18', '2018-11-29 14:11:55.450000', True, '0.1', 62.59, 'SUSPENDED')
    ]
    actual = result.fetchall()
    assert expected == actual


@pytest.mark.a02_examples
def test_fetch_one_column_as_list_of_values(connection):
    statement = "SELECT user_name, user_id FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        'Jessica Mccoy',
        'Beth James',
        'Mrs. Teresa Ryan',
        'Tommy Henderson',
        'Jessica Christian'
    ]
    actual = result.fetchcol()
    assert expected == actual


@pytest.mark.a02_examples
def test_fetch_a_single_value(connection):
    statement = "SELECT user_name, user_id FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = 'Jessica Mccoy'
    actual = result.fetchval()
    assert expected == actual


@pytest.mark.a03_examples
def test_fetch_dict_using_fetchone(dsn, user, password, schema):
    connection = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        fetch_dict=True
    )
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        {
            'USER_ID': 0, 'USER_NAME': 'Jessica Mccoy', 'REGISTER_DT': '2018-07-12',
            'LAST_VISIT_TS': '2018-04-03 18:36:40.553000', 'IS_FEMALE': True,
            'USER_RATING': '0.7', 'USER_SCORE': None, 'STATUS': 'ACTIVE'
        },
        {
            'USER_ID': 1, 'USER_NAME': 'Beth James', 'REGISTER_DT':  '2018-05-24',
            'LAST_VISIT_TS': '2018-03-24 08:08:46.251000',
            'IS_FEMALE': False,  'USER_RATING': '0.53', 'USER_SCORE': 22.07, 'STATUS': 'ACTIVE'
        },
        {
            'USER_ID': 2, 'USER_NAME': 'Mrs. Teresa Ryan', 'REGISTER_DT':  '2018-08-21',
            'LAST_VISIT_TS': '2018-11-07 01:53:08.727000',
            'IS_FEMALE': False,  'USER_RATING': '0.03', 'USER_SCORE': 24.88, 'STATUS': 'PENDING'
        },
        {
            'USER_ID': 3, 'USER_NAME': 'Tommy Henderson', 'REGISTER_DT':  '2018-04-18',
            'LAST_VISIT_TS': '2018-04-28 21:39:59.300000',
            'IS_FEMALE': True, 'USER_RATING': '0.5', 'USER_SCORE': 27.43, 'STATUS': 'DISABLED'
        },
        {
            'USER_ID': 4, 'USER_NAME': 'Jessica Christian', 'REGISTER_DT':  '2018-12-18',
            'LAST_VISIT_TS': '2018-11-29 14:11:55.450000',
            'IS_FEMALE': True, 'USER_RATING': '0.1', 'USER_SCORE': 62.59, 'STATUS': 'SUSPENDED'
        }
    ]
    actual = [row for row in iter(result.fetchone, None)]
    assert expected == actual


@pytest.mark.a03_examples
def test_fetch_dict_using_fetchmany(dsn, user, password, schema):
    connection = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        fetch_dict=True
    )
    count = 3
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)

    expected = [
        {
            'USER_ID': 0, 'USER_NAME': 'Jessica Mccoy', 'REGISTER_DT': '2018-07-12',
            'LAST_VISIT_TS': '2018-04-03 18:36:40.553000', 'IS_FEMALE': True,
            'USER_RATING': '0.7', 'USER_SCORE': None, 'STATUS': 'ACTIVE'
        },
        {
            'USER_ID': 1, 'USER_NAME': 'Beth James', 'REGISTER_DT':  '2018-05-24',
            'LAST_VISIT_TS': '2018-03-24 08:08:46.251000',
            'IS_FEMALE': False,  'USER_RATING': '0.53', 'USER_SCORE': 22.07, 'STATUS': 'ACTIVE'
        },
        {
            'USER_ID': 2, 'USER_NAME': 'Mrs. Teresa Ryan', 'REGISTER_DT':  '2018-08-21',
            'LAST_VISIT_TS': '2018-11-07 01:53:08.727000',
            'IS_FEMALE': False,  'USER_RATING': '0.03', 'USER_SCORE': 24.88, 'STATUS': 'PENDING'
        }
    ]
    actual = result.fetchmany(count)
    assert expected == actual

    expected = [
        {
            'USER_ID': 3, 'USER_NAME': 'Tommy Henderson', 'REGISTER_DT':  '2018-04-18',
            'LAST_VISIT_TS': '2018-04-28 21:39:59.300000',
            'IS_FEMALE': True, 'USER_RATING': '0.5', 'USER_SCORE': 27.43, 'STATUS': 'DISABLED'
        },
        {
            'USER_ID': 4, 'USER_NAME': 'Jessica Christian', 'REGISTER_DT':  '2018-12-18',
            'LAST_VISIT_TS': '2018-11-29 14:11:55.450000',
            'IS_FEMALE': True, 'USER_RATING': '0.1', 'USER_SCORE': 62.59, 'STATUS': 'SUSPENDED'
        }
    ]
    actual = result.fetchmany(count)
    assert expected == actual

    
@pytest.mark.a03_examples
def test_fetch_dict_using_fetchall(dsn, user, password, schema):
    connection = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        fetch_dict=True
    )
    statement = "SELECT * FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        {
            'USER_ID': 0, 'USER_NAME': 'Jessica Mccoy', 'REGISTER_DT': '2018-07-12',
            'LAST_VISIT_TS': '2018-04-03 18:36:40.553000', 'IS_FEMALE': True,
            'USER_RATING': '0.7', 'USER_SCORE': None, 'STATUS': 'ACTIVE'
        },
        {
            'USER_ID': 1, 'USER_NAME': 'Beth James', 'REGISTER_DT':  '2018-05-24',
            'LAST_VISIT_TS': '2018-03-24 08:08:46.251000',
            'IS_FEMALE': False,  'USER_RATING': '0.53', 'USER_SCORE': 22.07, 'STATUS': 'ACTIVE'
        },
        {
            'USER_ID': 2, 'USER_NAME': 'Mrs. Teresa Ryan', 'REGISTER_DT':  '2018-08-21',
            'LAST_VISIT_TS': '2018-11-07 01:53:08.727000',
            'IS_FEMALE': False,  'USER_RATING': '0.03', 'USER_SCORE': 24.88, 'STATUS': 'PENDING'
        },
        {
            'USER_ID': 3, 'USER_NAME': 'Tommy Henderson', 'REGISTER_DT':  '2018-04-18',
            'LAST_VISIT_TS': '2018-04-28 21:39:59.300000',
            'IS_FEMALE': True, 'USER_RATING': '0.5', 'USER_SCORE': 27.43, 'STATUS': 'DISABLED'
        },
        {
            'USER_ID': 4, 'USER_NAME': 'Jessica Christian', 'REGISTER_DT':  '2018-12-18',
            'LAST_VISIT_TS': '2018-11-29 14:11:55.450000',
            'IS_FEMALE': True, 'USER_RATING': '0.1', 'USER_SCORE': 62.59, 'STATUS': 'SUSPENDED'
        }
    ]
    actual = result.fetchall()
    assert expected == actual


@pytest.mark.a03_examples
def test_fetch_one_column_as_list_of_values_with_dict_setting(dsn, user, password, schema):
    connection = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        fetch_dict=True
    )
    statement = "SELECT user_name, user_id FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = [
        'Jessica Mccoy',
        'Beth James',
        'Mrs. Teresa Ryan',
        'Tommy Henderson',
        'Jessica Christian'
    ]
    actual = result.fetchcol()
    assert expected == actual


@pytest.mark.a03_examples
def test_fetch_a_single_value_with_dict_setting(dsn, user, password, schema):
    connection = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        fetch_dict=True
    )
    statement = "SELECT user_name, user_id FROM USERS ORDER BY USER_ID LIMIT 5;"
    result = connection.execute(statement)
    expected = 'Jessica Mccoy'
    actual = result.fetchval()
    assert expected == actual

