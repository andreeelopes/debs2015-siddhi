#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 29 10:27:31 2018

@author: coquenim, lopes
"""

from kafka import KafkaConsumer


def createConsumer(topic):
    return KafkaConsumer(topic)


#criacao dos consumidores para as varias queries

c_freq_routes = createConsumer('freq_routes')
c_profit_areas = createConsumer('profit_areas')
c_idle_taxis = createConsumer('idle_taxis')
c_cong_areas = createConsumer('cong_areas')
c_pleas_driver = createConsumer('pleas_driver')

#for msg in consumer:
    #print (msg)