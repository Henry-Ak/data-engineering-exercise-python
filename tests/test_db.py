import sys
import os

sys.path.append(os.path.join(os.path.dirname(sys.path[0]), 'src'))

import sqlite3
import unittest
from db import *
from main import *


def test_sqlite3_connection():
    with sqlite3.connect('warehouse.db') as con:
        cursor = con.cursor()
        assert list(cursor.execute('SELECT 1')) == [(1,)]


class TestDatabase(unittest.TestCase):
    def test_create_database(self):
        with sqlite3.Connection('warehouse.db') as con:
            # Test that warehouse.db is created
            cursor = con.cursor()
            create_database()
            table_check = cursor.execute("SELECT EXISTS (SELECT name FROM sqlite_master WHERE type='table' AND "
                                         "name='votes');")
            table_check = cursor.fetchone()[0]
            self.assertEqual(table_check, 1)
            cursor.execute('drop table votes')
            cursor.close()

    def test_drop_database(self):
        with sqlite3.Connection('warehouse.db') as con:
            # Test that all warehouse.db tables and views are dropped
            cursor = con.cursor()
            create_database()
            add_list_data_to_database()
            partition_by_week()
            get_vote_count_per_week()
            get_average_votes_per_week()
            find_outliers()
            drop_database()
            cursor.execute("SELECT name FROM sqlite_master")
            check_database = len(cursor.fetchall())
            self.assertEqual(check_database, 0)
            cursor.close()
