# -*- coding: utf-8 -*-
"""
Created on Fri Oct 27 23:18:58 2023

@author: Ameni
"""
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# Expression régulière pour extraire les mots (y compris les apostrophes)
WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def steps(self):
        # Définit les étapes du job MapReduce
        return [
            MRStep(mapper=self.mapper_get_words, reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_make_counts_key, reducer=self.reducer_output_words),
        ]

    def mapper_get_words(self, _, line):
        # Utilise l'expression régulière pour extraire les mots de la ligne
        words = WORD_REGEXP.findall(line)

        # Émet chaque mot avec une occurrence de 1
        for word in words:
            # Convertit le mot en minuscules pour la normalisation
            yield word.lower(), 1

    def reducer_count_words(self, word, values):
        # Agrège les occurrences de chaque mot
        yield word, sum(values)

    def mapper_make_counts_key(self, word, count):
        # Émet le nombre formaté comme clé et le mot comme valeur
        yield '%04d' % int(count), word

    def reducer_output_words(self, count, words):
        # Émet les mots associés à leur occurrence
        for word in words:
            yield count, word

if __name__ == '__main__':
    MRWordFrequencyCount.run()
