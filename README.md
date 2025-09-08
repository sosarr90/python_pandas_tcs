Write a Python script to load sample data into a data frame, perform transformations to the data,
and to display the data frame to the console as in the format shown below.
--------------------------------

Input data
----------------
year, month, day, hour, seconds, transaction_id, amount
2025,1,15,15,59,AA,1a
2025,2,14,23,55,B,2
2021,10,13,22,55,AA,1
2023,11,12,8,50,AA,2
2025,3,11,7,45,AA,1
1990,4,10,2,45,B,2
2001,4,9,11,45,B,1
1990,4,10,2,45,B,2
2021,10,13,22,55,AA,1


Transformations
-------------------------
1. change the date format as [mm-YYYY-dd hour:minute:second]
2. Calculate the total amount for each transaction_id
3. Choose most recent transaction date for each transaction_id



Output Data Frame
-----------------------------
transaction_id, total_amount, most_recent_transaction_date