import re
import os
from operator import add

import unidecode
import pandas
from pyspark.sql import SparkSession


def create_spark_session ():
    MASTER = 'local[3]'
    return SparkSession.builder.master(MASTER).appName('Word counter').getOrCreate()


def read_text_file(filename):
    # Lee un archivo de texto
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        with open(current_dir + '/' + filename, 'r') as file:
            return file.read()
    except:
        return None


def split_words(text):
    # Cuenta las palabras de un texto
    try:
        text_split_regex = r'[\W]+'
        words_list = re.split(text_split_regex, text)
        words_list = list(filter(lambda word: word != '', words_list))

        return words_list
    except:
        return None


def sanitize_text(text):
    # Limpia caracteres raros de un texto
    return unidecode.unidecode(text)


def sanitize_text_array(text_array):
    # Limpia caracteres raros de un texto
    return list(map(lambda word: sanitize_text(word), text_array))


def group_words(words_array):
    # Cuenta las veces que aparece una palabra en un texto
    try:
        word_count = [(word, 1) for word in words_array]
        word_count = pandas.DataFrame(word_count)

        return word_count.groupby(0) \
            .sum() \
            .sort_values(1, ascending=False)
    except:
        return None


def remove_words(words_list):
    # Quita las palabras dadas de un dataframe
    words_to_remove = read_text_file('stopwords.txt')

    try:
        return list(
            filter(
                lambda current_word: current_word not in words_to_remove,
                words_list
            )
        )
    except:
        return None


def remove_word(current_word):
    global words_to_remove
    return current_word not in words_to_remove


if __name__ == '__main__':
    session = create_spark_session()
    text = [read_text_file('cuento.txt')]

    # Quita las palabras dadas de un dataframe
    words_to_remove = read_text_file('stopwords.txt')

    #
    rdd_texto_en_una_fila                          = session.sparkContext.parallelize(text)
    rdd_una_fila_por_palabra                       = rdd_texto_en_una_fila.flatmap(split_words)
    rdd_una_fila_por_palabra_sin_acentos           = rdd_una_fila_por_palabra.map(sanitize_text)
    rdd_una_fila_por_palabra_sin_acentos_en_mayus  = rdd_una_fila_por_palabra_sin_acentos.map(lambda word: word.upper())

    rdd_limpito = rdd_una_fila_por_palabra_sin_acentos_en_mayus.filter(remove_word)

    rdd_dataframe = rdd_limpito.map(lambda pal: (pal, 1))

    rdd = rdd_dataframe.reduceByKey(add)

    lista_puntuada = rdd.collect()

    for (palabra, ocurrencias) in lista_puntuada:
        print('Palabra ->', palabra, ' :: Ocurrencias ->', ocurrencias)

    session.stop()
