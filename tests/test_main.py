import sys
import os
import unittest
import sqlite3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(sys.path[0]), 'src'))

from main import *
from db import *

class TestFindOutliers(unittest.TestCase):
    test_values = [{"Id": "1", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2017-02-28T00:00:00.000"},
                   {"Id": "2", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2017-02-28T00:00:00.000"},
                   {"Id": "42", "PostId": "20", "VoteTypeId": "2", "CreationDate": "2017-02-28T00:00:00.000"}]

    def test_insert_data(self):
        with sqlite3.Connection('warehouse.db') as con:
            # Test that data is inserted into the database
            cursor = con.cursor()
            db.create_database()
            add_list_data_to_database()
            self.assertTrue(cursor.execute("SELECT EXISTS(SELECT 1 FROM votes WHERE id = 1);"))
            cursor.execute('drop table votes')
            cursor.close()

    def test_select_data(self):
        with sqlite3.Connection('warehouse.db') as con:
            # Test that all the data from the table is selected
            cursor = con.cursor()
            db.create_database()
            add_list_data_to_database()
            values_len = len(values)
            cursor.execute('select count (*) from votes;')
            row_count = cursor.fetchone()
            row_count = row_count[0]
            self.assertEqual(values_len, row_count)
            cursor.execute('drop table votes')
            cursor.close()

    def test_extract_weeks(self):
        with sqlite3.Connection('warehouse.db') as con:
            # Test that weeks are extracted successfully
            cursor = con.cursor()
            db.create_database()
            add_list_data_to_database()
            partition_by_week()
            res = list(cursor.execute('select * from weeks_view'))
            self.assertTrue(res)
            cursor.execute('drop table votes')
            cursor.execute('drop view weeks_view')
            cursor.close()

    def test_vote_counts(self):
        with sqlite3.Connection('warehouse.db') as con:
            # Test that vote counts per week view is created
            cursor = con.cursor()
            db.create_database()
            add_list_data_to_database()
            partition_by_week()
            get_vote_count_per_week()
            res = cursor.execute('select * from votes_per_week_view')
            self.assertTrue(list(res))
            cursor.execute('drop table votes')
            cursor.execute('drop view weeks_view')
            cursor.execute('drop view votes_per_week_view')
            cursor.close()

    def test_avg_votes(self):
        with sqlite3.Connection('warehouse.db') as con:
            # Test that avg vote counts per week are found
            cursor = con.cursor()
            db.create_database()
            add_list_data_to_database()
            partition_by_week()
            get_vote_count_per_week()
            avg = int(get_average_votes_per_week())
            self.assertTrue(avg)
            cursor.execute('drop table votes')
            cursor.execute('drop view weeks_view')
            cursor.execute('drop view votes_per_week_view')
            cursor.close()

    def test_find_outliers(self):
        with sqlite3.Connection('warehouse.db') as con:
            # Test  outliers functions outputs sqlite object
            cursor = con.cursor()
            db.create_database()
            add_list_data_to_database()
            partition_by_week()
            get_vote_count_per_week()
            get_average_votes_per_week()
            find_outliers()
            res = cursor.execute('select * from outliers_table')
            self.assertTrue(res)
            cursor.execute('drop table votes')
            cursor.execute('drop view weeks_view')
            cursor.execute('drop view votes_per_week_view')
            cursor.execute('drop table outliers_table')
            cursor.close()

    def test_select_outliers(self):
        with sqlite3.Connection('warehouse.db') as con:
            # Test that the select outliers view query is correct
            cursor = con.cursor()
            db.create_database()
            add_list_data_to_database()
            partition_by_week()
            get_vote_count_per_week()
            get_average_votes_per_week()
            find_outliers()
            res = cursor.execute('select * from outliers_table')
            self.assertTrue(list(res))
            cursor.execute('drop table votes')
            cursor.execute('drop view weeks_view')
            cursor.execute('drop view votes_per_week_view')
            cursor.close()
