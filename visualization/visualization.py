#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 29 10:27:31 2018

@author: coquenim, lopes
"""

from kafka import KafkaConsumer
import numpy as np

def createConsumer(topic):
    return KafkaConsumer(topic)

#ex: b'E0C54FD4238BC93F36CA25238F7E69C0-38,25'"
def getPayload(msg):
    payload = msg.decode('UTF-8') #converter bytes para str
    return np.asarray(payload.split('||'))
    

#criacao dos consumidores para as varias queries

c_freq_routes = createConsumer('freq_routes')
c_profit_areas = createConsumer('profit_areas')
c_idle_taxis = createConsumer('idle_taxis')
c_cong_areas = createConsumer('cong_areas')
c_pleas_driver = createConsumer('pleasant_driver')

for msg in c_pleas_driver:
    payload = getPayload(msg.value)
    print(payload[0])