import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT,
        PRIMARY KEY (id));
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users("id", "first_name", "last_name", "gender", "address", 
                "post_code", "email", "username", "dob", "registered_date", "phone", "picture")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        print("Data inserted successfully")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

def create_cassandra_connection():

    try:
        # connecting to the cassandra cluster
        cas_session = Cluster(['localhost']).connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")

    return None


def create_spark_connection():
    s_conn = None

    try:
        s_conn = (SparkSession.builder
                  .appName('SparkDataStreaming')
                  .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,'
                                                 'com.datastax.spark:spark-cassandra-connector_2.12:3.5.0')
                  .config('spark.cassandra.connection.host', 'localhost')
                  .getOrCreate())

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(s_conn):
    spark_df = None
    try:
        spark_df = (s_conn.readStream.format('kafka')
                    .option('kafka.bootstrap.servers', 'localhost:9092')
                    .option('subscribe', 'users')
                    .option('startingOffsets', 'earliest')
                    .load())
        spark_df.printSchema()
        logging.info("kafka dataframe created successfully")

    except Exception as e:
        logging.warning(f"kafka dataframe could not be created due to: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("post_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])

    selection = (spark_df.selectExpr("CAST(value AS STRING) as value")
                 .select(from_json(col('value'), schema).alias('data'))
                 .select("data.*"))
    print(selection)
    return selection


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            insert_data(session)
            logging.info("Streaming is being started...")


            streaming_query = (selection_df.writeStream
                               .format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .option('failOnDataLoss', 'false')
                               .start())

            streaming_query.awaitTermination()