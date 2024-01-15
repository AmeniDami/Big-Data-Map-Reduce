# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 19:37:52 2023

@author: Ameni
"""

from mrjob.job import MRJob

class MRMinFahrenheit(MRJob):

    def MakeFahrenheit(self, tenthsOfCelsius):
        # Convertir la température de dixièmes de degré Celsius à Fahrenheit
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit

    def mapper(self, key, line):
        # Séparer chaque ligne en colonnes en utilisant la virgule comme délimiteur
        (location, date, type, data, x, y, z, w) = line.split(',')

        # Vérifier si le type est "TMIN" (température minimale)
        if type == "TMIN":
            # Appeler la fonction MakeFahrenheit pour convertir la température
            temperature = self.MakeFahrenheit(data)

            # Émettre la location comme clé et la température convertie en Fahrenheit comme valeur
            yield location, temperature

    def reducer(self, location, temps):
        # Agréger la température minimale convertie en Fahrenheit pour chaque emplacement
        yield location, min(temps)

if __name__ == '__main__':
    MRMinFahrenheit.run()
