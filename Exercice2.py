# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 11:45:17 2023

@author: Ameni
"""

from mrjob.job import MRJob
class MRMaxTemperature (MRJob) :
    
    def mapper(self, key, line):
        
        # Séparer chaque ligne en colonnes en utilisant la virgule comme délimiteur
        (location, date, type, data, x, y, z, w) = line.split(',')
        
        # Vérifier si le type est "TMAX" (température maximale)
        if (type =="TMAX"): 
            
            # Émettre la location comme clé et la température (convertie en float) comme valeur
            yield location, float(data)
            
    def reducer (self, location, temps) :
        
        # Agréger la température maximale pour chaque emplacement
        yield location, max (temps)
    
if __name__=='__main__':
    MRMaxTemperature.run()
    