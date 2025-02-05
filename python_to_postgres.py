import psycopg2
import psycopg2.extras

hostname= 'localhost'
database= 'demo'
username= 'postgres'
pwd= 'password'
port_id= 5432
conn = None
# cur = None      # after with close no need of cur = None

try:

    # advantage of using with clause is that it automatically closes the connection when we are done with it
    # with clause make sure to commit the transaction and close the connection automatically
    with psycopg2.connect(
        host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id

    ) as conn:

    # define cursor
    # cur = conn.cursor()
    
        # define cursor to return data as dictionary
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            cur.execute('DROP TABLE IF EXISTS employee')

            # Create table
            create_script = ''' CREATE TABLE IF NOT EXISTS employee (
                                    id      int PRIMARY KEY,
                                    name    varchar(40) NOT NULL,
                                    salary  int,
                                    dept_id varchar(30))'''
            
            # in order to execute the script, we need to use the execute method of the cursor object
            cur.execute(create_script)

            insert_script = 'INSERT INTO employee (id, name, salary, dept_id) VALUES (%s, %s, %s, %s)'

            # pass single data
            insert_value = (1, 'Rahul', 12000, 'D1')

            # pass multiple data
            insert_values = [(1, 'Rahul', 12000, 'D1'), (2, 'Robin', 15000, 'D1'), (3, 'Ravi', 20000, 'D2')]

            # loop to insert multiple values
            for record in insert_values:
                cur.execute(insert_script, record)

            # in order to fetch the data, we need to use the fetch method of the cursor object
            cur.execute('SELECT * FROM EMPLOYEE')

            # for fetching all data in a list of tuples
            # print(cur.fetchall())

            # for fetching one row at a time
            # for record in cur.fetchall():
            #     print(record)

            # # for fetching specific column
            # for record in cur.fetchall():
            #     print(record[1], record[2])


            # update
            update_script = 'UPDATE employee SET salary = salary + (salary * 0.5)'
            cur.execute(update_script)

            # Delete
            delete_script = 'DELETE FROM employee WHERE name = %s'
            delete_record = ('Robin', )
            cur.execute(delete_script, delete_record)


            # after defining cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor), we can access data with column name
            cur.execute('SELECT * FROM EMPLOYEE')
            for record in cur.fetchall():
                print(record['name'], record['salary'])


            



            # in order to save the changes, we need to use the commit method of the connection object
            # after using with close no need to use commit, it will automatically commit
            # conn.commit()


except Exception as error:
    print(error)

finally:

    # after using with clause no need to close the cursor, it will close automatically
    # if cur is not None:
    #     cur.close()
    if conn is not None:
        conn.close()
