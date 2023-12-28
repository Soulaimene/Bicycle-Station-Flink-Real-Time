from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings 
from pyflink.table import expressions as expr
import logging


def create_kafka_source():
    kafka_sql = """
    CREATE TABLE IF NOT EXISTS KafkaConsumerTable (
      numbers BIGINT,
      contract_name STRING,
      banking BOOLEAN,
      bike_stands BIGINT,
      available_bike_stands BIGINT,
      available_bikes BIGINT,
      address STRING,
      status STRING,
      `position` MAP<STRING, DOUBLE>,
      timestamps STRING
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'bike',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset',
      'json.fail-on-missing-field' = 'false'
    )
    """
    
    return kafka_sql

def create_sink():
    sink_ddl = """ 
    CREATE TABLE ElasticsearchSinkTable (
    numbers BIGINT,
    contract_name STRING,
    banking BOOLEAN,
    bike_stands BIGINT,
    available_bike_stands BIGINT,
    available_bikes BIGINT,
    address STRING,
    status STRING,
    `position` MAP<STRING, DOUBLE>,
    timestamps STRING,
    PRIMARY KEY (address) NOT ENFORCED
    ) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://elasticsearch:9200',
    'index' = 'bike',
    'document-id.key-delimiter' = '$',
    'sink.bulk-flush.max-size' = '42mb',
    'sink.bulk-flush.max-actions' = '32',
    'sink.bulk-flush.interval' = '1000',
    'sink.bulk-flush.backoff.delay' = '1000',
    'format' = 'json'
)
    """
  
    return sink_ddl

def log_processing():
    logging.basicConfig(level=logging.INFO)
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.Builder().build()
    env_settings.to_configuration().set_string("parallelism.default", "4")
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=env_settings)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
    t_env.get_config().get_configuration().set_boolean("checkpointing.enabled", True)



    t_env.execute_sql(create_kafka_source())
    print("Successfully created source tables ")
    t_env.execute_sql(create_sink())
    print("Successfully created sink tables")


    # query = """
    # SELECT
    #   numbers,
    #   contract_name,
    #   banking,
    #   bike_stands,
    #   available_bike_stands,
    #   available_bikes,
    #   address,
    #   status,
    #   `position`,
    #   timestamps
    # FROM KafkaConsumerTable
    # """
    # result_table=t_env.execute_sql(query)
    # result_table.print()

    query1 = """
    INSERT INTO ElasticsearchSinkTable
    SELECT
      numbers,
      contract_name,
      banking,
      bike_stands,
      available_bike_stands,
      available_bikes,
      address,
      status,
      `position`,
      timestamps
    FROM KafkaConsumerTable
    """
    t_env.execute_sql(query1).wait()
  

    

if __name__ == '__main__':
    log_processing()
