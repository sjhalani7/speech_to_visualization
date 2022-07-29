import pandas as pd
import sqlite3 as sql
import pandasql as psql


# Creating sql lite database
def createDb():
    conn= sql.connect("your_database.db", check_same_thread=False)
    cursor=conn.cursor()
    cursor.execute("Drop table if exists SalesD")
    cursor.execute("Create Table SalesD(Year int, Sales int)")
    cursor.execute("insert into SalesD values(2001, 12000)")
    cursor.execute("insert into SalesD values(2002, 20000)")
    cursor.execute("insert into SalesD values(2003, 34000)")
    cursor.execute("insert into SalesD values(2004, 39000)")
    cursor.execute("insert into SalesD values(2005, 60000)")
    cursor.execute("insert into SalesD values(2006, 68000)")
    cursor.execute("insert into SalesD values(2007, 55000)")
    cursor.execute("insert into SalesD values(2008, 73000)")
    cursor.execute("insert into SalesD values(2009, 79000)")
    cursor.execute("insert into SalesD values(2010, 48000)")
    cursor.execute("insert into SalesD values(2011, 55000)")
    cursor.execute("insert into SalesD values(2012, 83000)")
    cursor.execute("insert into SalesD values(2013, 92000)")
    cursor.execute("insert into SalesD values(2014, 67000)")
    cursor.execute("insert into SalesD values(2015, 77000)")
    cursor.execute("insert into SalesD values(2016, 82000)")
    cursor.execute("insert into SalesD values(2017, 87000)")
    cursor.execute("insert into SalesD values(2018, 91000)")
    cursor.execute("insert into SalesD values(2019, 99000)")
    cursor.execute("insert into SalesD values(2020, 55000)")
    cursor.execute("insert into SalesD values(2021, 87000)")
    cursor.execute("insert into SalesD values(2022, 104000)")


    cursor.execute("SELECT * FROM SalesD")

    # printing the cursor data
    print(cursor.fetchall())

    # check the database creation data
    if cursor:
        print("Database Created Successfully !")
    else:
        print("Database Creation Failed !")

    # Commit the changes in database and Close the connection
    conn.commit()
    conn.close()

#create pandas dataframe with sqlite
conne = sql.connect("your_database.db", check_same_thread=False)
query=pd.read_sql_query('''SELECT * FROM SalesD''', conne)
df_sq=pd.DataFrame(query, columns = ['Year', 'Sales'])
#print(df_sq)

# query on pandas dataframe using pandasql
def sqlquery_1():
    q = "SELECT * FROM df_sq"
    psql.sqldf(q, globals())
    # globals() and locals() are built-in function in python where functions and variables are stored
    """
    globals() is a dictionary of all the variables created in the session (this python file)
    the key is the name of the variable and value is the actual value of the variable 
    "print(globals())" shows and prints all the functions and variables from this session. 

    therefore, to see the output of a query, we do 'globals()["{Variable/func name}"]'
    eg: globals()['sqldf']
    """

    # to make life easier, and avoid having to pass globals() everytime, create a lambda function
    # to do it

    sq = lambda q: psql.sqldf(q, globals())
    # sq is the new function name, q is the argument (in this case query) passed, and after the ':' is the
    # return of the lambda func that goes to sq.

    q1 = 'SELECT * from df_sq where Year=2014' #testing queries
    print(type(sq(q1)))
    return (sq(q1))