import db
import sqlite3
import json
import os
import sys


input_path = os.path.join(os.path.dirname(sys.path[0]), 'uncommitted')

with open(input_path + '/votes.jsonl') as jsonfile:

	jsonlist = list(jsonfile)
	values = []

	for json_str in jsonlist:
		json_dict = json.loads(json_str)
		value = []
		value.append(json_dict['Id'])
		value.append(json_dict['PostId'])
		value.append(json_dict['VoteTypeId'])
		value.append(json_dict['CreationDate'])
		values.append(value)

	def add_list_data_to_database():
		with sqlite3.Connection('warehouse.db') as con:
			cursor = con.cursor()
			cursor.executemany('insert into votes values (:id, :postid, :votetypeid, :creationdate)', values)
			cursor.close()

	def select_from_database():
		with sqlite3.Connection('warehouse.db') as con:
			cursor = con.cursor()
			res = cursor.execute('select * from votes')
			print(list(res))
			cursor.close()

	def partition_by_week():
		with sqlite3.Connection('warehouse.db') as con:
			cursor = con.cursor()
			cursor.execute("""
			create view if not exists weeks_view as select
			Id,
			PostId,
			VoteTypeId,
			CreationDate,
			strftime(\'%W\', CreationDate) Week_Number,
			strftime(\'%Y\', CreationDate) Year_Number
			from votes
			""")
			cursor.close()

	def get_vote_count_per_week():
		with sqlite3.Connection('warehouse.db') as con:
			cursor = con.cursor()
			cursor.execute("""
			create view if not exists votes_per_week_view as select
			count(Id)
			as vote_count,
			Id,
			PostId,
			VoteTypeId,
			CreationDate,
			Week_Number,
			Year_Number
			from weeks_view
			group by Week_Number""")
			cursor.close()

	def get_average_votes_per_week():
		with sqlite3.Connection('warehouse.db') as con:
			cursor = con.cursor()
			res = cursor.execute("""
			select
			avg(vote_count)
			from votes_per_week_view
			""")
			average_votes_per_week = list(res)[0][0]
			return average_votes_per_week

	def find_outliers():
		with sqlite3.Connection('warehouse.db') as con:
			cursor = con.cursor()

			average_votes_per_week = int(get_average_votes_per_week())

			high_mark = str(average_votes_per_week + (average_votes_per_week*.2))
			low_mark = str(average_votes_per_week - (average_votes_per_week*.2))

			cursor.execute("""
			create table if not exists outliers_table as select
			vote_count,
			case
				when
					(vote_count > """ + high_mark + """ or vote_count < """ + low_mark + """)
				and
					(lag(vote_count, 1, """ + low_mark + """) over (order by Week_Number) between """ + low_mark + """ and """ + high_mark + """)
				then \'outlier\'
				else \'not outlier\'
				end is_outlier,
			Week_Number,
			Year_Number,

			(vote_count > """ + high_mark + """ or vote_count < """ + low_mark + """),

			(lag(vote_count, 1, """ + low_mark + """) over (order by Week_Number) between """ + low_mark + """ and """ + high_mark + """)

			from votes_per_week_view
			order by Week_Number
			""")
			cursor.close()

	def select_outliers():
		with sqlite3.Connection('warehouse.db') as con:
			cursor = con.cursor()
			cursor.execute("select count(*) as number_of_outliers FROM outliers_table where is_outlier=\"outlier\"")
			res1 = cursor.fetchone()[0]
			print("Total number of outliers = ", res1)
			cursor.execute("select count(*) as normal_values FROM outliers_table where is_outlier=\"not outlier\"")
			res2 = cursor.fetchone()[0]
			print("Outlier to non-outlier ratio = ", res1/res2)
			cursor.execute('select vote_count, Week_Number, Year_Number from outliers_table where is_outlier=\"outlier\"')
			res3 = cursor.fetchall()
			print("List of outlier values: (Vote count, Week number, Year number)")
			for elem in res3:
				print(elem)
			cursor.close()


	db.create_database()
	add_list_data_to_database()
	partition_by_week()
	get_vote_count_per_week()
	get_average_votes_per_week()
	find_outliers()
	select_outliers()
	db.drop_database()
