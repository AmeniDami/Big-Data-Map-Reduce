# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 17:45:06 2023

@author: Ameni
"""

from mrjob.job import MRJob
class MRRatingCounter (MRJob):
    
    # Décomposer chaque ligne en colonnes en utilisant le délimiteur de tabulation
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp )= line.split('\t')
       
    # Émettre la note comme clé et la valeur 1 pour chaque enregistrement
        yield rating, 1
    def reducer(self, rating, occurences):
        
    # Agréger le nombre d'occurrences de chaque note
        yield rating, sum(occurences)
        
if __name__=='__main__':
    MRRatingCounter.run()