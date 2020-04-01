from pyspark.sql import SparkSession


def create_spark_session():
    MASTER = 'local[3]'
    return SparkSession.builder.master(MASTER).appName('SQL app').getOrCreate()


if __name__ == '__main__':
    spark = create_spark_session()

    ##
    # data_frame = spark.read.json('personas.json')
    # data_frame.show()

    ##
    # data_frame.select(data_frame['nombre'], data_frame['edad'][:1] + '0').show()

    ##
    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true', inferschema='true') \
        .load('data/train.csv')

    df.show()

    spark.stop()
