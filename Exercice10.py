# -*- coding: utf-8 -*-
"""
Created on Sat Oct 28 01:29:43 2023

@author: Ameni
"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class OrderAmounts(MRJob):

    def steps(self):
        # Définit les étapes du job MapReduce
        return [
            MRStep(mapper=self.mapper_get_order, reducer=self.reducer_count_order),
            MRStep(mapper=self.mapper_make_order, reducer=self.reducer_output_order)
        ]

    def mapper_get_order(self, _, line):
        # Séparer chaque ligne en colonnes en utilisant la virgule comme délimiteur
        (customer, item, order) = line.split(',')

        # Émettre le client comme clé et le montant de la commande (converti en float) comme valeur
        yield customer, float(order)

    def reducer_count_order(self, customer, orders):
        # Agréger le montant total des commandes pour chaque client
        yield customer, sum(orders)

    def mapper_make_order(self, customer, values):
        # Émettre le nombre formaté comme clé et le client comme valeur
        yield '%04d' % int(values), customer

    def reducer_output_order(self, count, customers):
        # Émettre les clients associés à leur montant total de commandes
        for customer in customers:
            yield count, customer

if __name__ == '__main__':
    OrderAmounts.run()
