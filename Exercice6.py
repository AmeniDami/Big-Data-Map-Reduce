# -*- coding: utf-8 -*-
"""
Created on Fri Oct 27 22:24:53 2023

@author: Ameni
"""


from mrjob.job import MRJob
import re

# Expression régulière pour extraire les mots (y compris les apostrophes)
WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):
  
    def mapper(self, _, line):
        # Utiliser l'expression régulière pour extraire les mots de la ligne
        words = WORD_REGEXP.findall(line)

        # Émettre chaque mot avec une occurrence de 1
        for word in words:
            # Convertir le mot en minuscules pour la normalisation
            yield word.lower(), 1

    def reducer(self, key, values):
        # Agréger les occurrences de chaque mot
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()
