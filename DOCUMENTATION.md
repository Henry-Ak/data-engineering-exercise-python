# Python Data Engineering Exercise Documentation
The vote data is ingested in the main.py file using the open function and is stored as a list of json data.
The list is then iterated through and each object is converted into a python dictionary and stored into a list
containing the dictionary values. With each function, an individual connection to the database is established. This
prevents the database from being used by multiple processes at the same time. The db.py file is responsible for the
creation and deletion of the tables and views within the database.

# What Does Each Function Do & How Do They Work? 
## create_database
This function executes the given query to create a table named 'votes' in the database with the given columns.

## add_list_data_to_database
This function takes the list of dictionary values and executes the given query to insert those queries into the votes
table.

## select_from_database
This function executes a query to return all the values from the votes table and then prints them to the terminal as a
list.

## partition_by_week
This function extracts the weeks from the votes table and stores this as a view in the database so that it can be
easily accessed by other functions.

## get_vote_count_per_week
This function takes the week view that was created in the partition_by_week function, and counts the identical Ids to
find the number votes per week. This query is stored as a view in the database so that it can be easily accessed by
other functions.

## get_average_votes_per_week
This function returns the average number of votes per week. This value is computed using the group by statement in the
given query. The result is stored as a list with the Id and Week_Number columns.

## find_outliers
With this function, I am using the data votes_per_week view that I created in the get_vote_count_per_week function.
In this function I create a column called is_outlier in the case that the total count of votes is either greater than
the high mark (higher than the average by more than 20%) or the less than low mark (lower than the average by more than
20%). I use the lag function to look at the previous row, and I include a default value of the low mark such that it
doesn't modify the system if the previous row wasn't an outlier. If the previous row is between the low mark and the
high mark, and the current row is an outlier numerically, then it is set as an outlier. Otherwise, it's set as not an
outlier.

**Explanatory note:**
The logic stipulates that a number that would otherwise have been marked as an outlier is marked as not an outlier if 
the number before it is high or low enough to have been an outlier, regardless of how it is labeled in the database. 
That should logically fulfill the requirement of the task.

## select_from_weeks_view
This function executes a query to return all the values from the weeks view and then prints them to the terminal as a
list.

## select_outliers
This function executes a query to return all the values from the outliers table and then prints them to the terminal as
a list.

## drop_database
This function executes a query that will drop all the tables and views that have added to the database.

## Tests
The tests need to be run from the 'tests' directory. 
