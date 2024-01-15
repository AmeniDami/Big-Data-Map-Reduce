# -*- coding: utf-8 -*-
"""
Created on Thu Nov  2 10:20:29 2023

@author: Ameni
"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):

    def steps(self):
        # Définit les étapes du job MapReduce
        return [
            MRStep(mapper=self.mapper_get_ratings, reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        # Séparer chaque ligne en colonnes en utilisant la tabulation comme délimiteur
        (userID, movieID, rating, timestamp) = line.split('\t')

        # Émettre le movieID comme clé et la valeur 1
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        # Agréger le nombre de notes pour chaque film
        yield None, (sum(values), key)

    def reducer_find_max(self, key, values):
        # Trouver le film le plus populaire en émettant celui avec le nombre de notes le plus élevé
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
