import io
import psycopg2

from sqlalchemy import create_engine


def create_table(df=None, table_name=None, URI=None):
    """
    Creates a table in the database

    Args:
        df (pd.Dataframe: a dataframe to be used to create the table
        table_name: the name of the table to be created
        URI: credentials for the database. defaults to URI
    """

    engine = create_engine(URI)
    print('connected to the db..')

    df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)


def populate_table(df=None, table_name=None, URI=None):
    """
    Populate the table in the db with data from the dataframe.

    The method here is quite fast. It uses the copy_from method from
    the psycopg2 library.
    """

    engine = create_engine(URI)
    conn = engine.raw_connection()
    print('connected to the database..')

    try:
        df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)

        with conn.cursor() as cur, io.StringIO() as output:
            print('creating the cursor..')
            print('writing the csv to file..')
            df.to_csv(output, sep='\t', header=False,
                      index=False, encoding='utf-8')
            output.seek(0)
            cur.copy_from(output, table_name, null="") 
        conn.commit()
        print('data inserted into table successfully!')
    
    except:
        print('Error: unable to populate the table.')
        conn.rollback()
    
    finally:
        conn.close()


def insert_into_table(df=None, table_name=None, URI=None):
    """
    Insert new data into the table.
    """

    if df is None:
        raise ValueError("Error: df is None")

    engine = create_engine(URI)

    conn = engine.raw_connection()
    cur = conn.cursor()

    try:
        output = io.StringIO()

        df.to_csv(output, sep='\t', header=False,
                  index=False, encoding='utf-8')
        output.seek(0)

        cur.copy_from(output, table_name, null="")

        conn.commit()
        cur.close()
        conn.close()

    except (psycopg2.OperationalError, psycopg2.ProgrammingError) as e:
        print(f"Error: unable to insert data into the table. {e}")
        print(df.index[-1])
        conn.rollback()

    except Exception as e:
        print(f"Error: {e}")
        print(df.index[-1])

    finally:
        cur.close()
        conn.close()


def drop_table(table_name=None, URI=None):
    """
    Drop a table from the database. 

    """

    psyco_conn = psycopg2.connect(URI)
    cursor = psyco_conn.cursor()
    psyco_conn.autocommit = True

    cursor.execute("""DROP TABLE %s;""" % table_name)
    cursor.close()
    psyco_conn.close()
