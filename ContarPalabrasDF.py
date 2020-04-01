from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from WordsHelper import split_words, read_text_file, sanitize_text


def create_spark_session():
    MASTER = 'local[3]'
    return SparkSession.builder.master(MASTER).appName('Contador palabras spark').getOrCreate()


def remove_word(current_word):
    global words_to_remove
    return current_word not in words_to_remove


if __name__ == '__main__':
    words_to_remove = read_text_file('stopwords.txt')
    spark = create_spark_session()

    ##
    texto = read_text_file("cuento.txt")

    # Procesamos el texto con rdd
    rdd = spark.sparkContext.parallelize([texto])\
        .flatMap(split_words)\
        .map(sanitize_text)\
        .map(lambda palabra: palabra.upper())\
        .filter(remove_word)

    # Convertimos el rdd a df
    df = rdd.map(lambda palabra: Row(word=palabra)).toDF()
    df.show()

    # Ordenamos el df por el número de palabras que tiene
    df.groupBy('word').count().orderBy('count', ascending=False).show()

    spark.stop()