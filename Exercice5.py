# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 19:40:21 2023

@author: Ameni
"""

from mrjob.job import MRJob

class MRMoyAge(MRJob):

    def mapper(self, _, line):
        # Séparer chaque ligne en colonnes en utilisant la virgule comme délimiteur
        (UserId, Name, Age, NumberF) = line.split(',')

        # Émettre l'âge comme clé et le nombre d'amis (converti en entier) comme valeur
        yield Age, int(NumberF)

    def reducer(self, Age, values):
        # Convertir les valeurs (nombre d'amis) en une liste
        nb_friends = list(values)

        # Calculer la somme et la longueur de la liste
        s = sum(nb_friends)
        l = len(nb_friends)

        # Calculer la moyenne des amis pour cet âge
        moyenne = s / l

        # Émettre l'âge comme clé et la moyenne comme valeur
        yield Age, moyenne

if __name__ == '__main__':
    MRMoyAge.run()
