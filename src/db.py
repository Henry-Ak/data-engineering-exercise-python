import sqlite3


def create_database():
    with sqlite3.Connection('warehouse.db') as con:
        cursor = con.cursor()
        cursor.execute(
            'create table if not exists votes (id integer, postid integer, votetypeid integer, creationdate text)')
        cursor.close()


def drop_database():
    with sqlite3.Connection('warehouse.db') as con:
        cursor = con.cursor()
        cursor.execute('drop table votes')
        cursor.execute('drop view weeks_view')
        cursor.execute('drop view votes_per_week_view')
        cursor.execute('drop table outliers_table')
        cursor.close()
