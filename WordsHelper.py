import re
import os
import unidecode
import pandas


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
        text_split_regex = '[\W]+'
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


def remove_words(words_to_remove, words_list):
    # Quita las palabras dadas de un dataframe
    try:
        return list(
            filter(
                lambda current_word: current_word not in words_to_remove,
                words_list
            )
        )
    except:
        return None
