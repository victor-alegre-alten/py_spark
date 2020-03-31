from unittest import TestCase

from WordsHelper import *


class Test(TestCase):
    def test_read_text_file(self):
        text = read_text_file('stopwords.txt')
        self.assertIsNotNone(text, 'Error reading or parsing text file')

    def test_split_words(self):
        words_list = split_words('This must be 4!!!--')

        self.assertEqual(len(words_list), 4, 'Word list length must be 4')

    def test_sanitize_text(self):
        sanitized = sanitize_text('ñáä')
        self.assertEqual(sanitized, 'naa')

    def test_sanitize_text_array(self):
        words_list = ('ñáá', 'éééé')
        sanitized_words = sanitize_text_array(words_list)

        self.assertEqual(sanitized_words[0], 'naa')
        self.assertEqual(sanitized_words[1], 'eeee')

    def test_group_words(self):
        grouped_words = group_words('Había una vez un cuento de una niña'.split(' '))
        # print(grouped_words.reset_index())
        # self.assert ?

    def test_remove_words(self):
        full_text = ('Esta', 'frase', 'es', 'un', 'test', 'con', 'palabras')
        filtered_words = remove_words(full_text)

        self.assertEqual(len(filtered_words), 4)
