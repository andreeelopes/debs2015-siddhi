#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 29 10:27:31 2018

@author: coquenim, lopes
"""

from kafka import KafkaConsumer
import numpy as np


START_GRID_LONGITUDE = -74.913585
GRID_STEP_LONGITUDE = 0.0059986
START_GRID_LATITUDE = 41.474937
GRID_STEP_LATITUDE = 0.004491556


def createConsumer(topic):
    return KafkaConsumer(topic)

#ex: b'E0C54FD4238BC93F36CA25238F7E69C0-38,25'"
def getPayload(msg):
    payload = msg.decode('UTF-8') #converter bytes para str
    return payload.split('||')
    
def getSorted(list_to_sort, column, limit):
    sorted_list = sorted(list_to_sort, key=lambda k: k[column], reverse = True) 
    return sorted_list[:limit]

#pre: grid values must be positive or zero
def getLongitude(grid_x):
    
    return START_GRID_LONGITUDE + grid_x * GRID_STEP_LONGITUDE
    
#pre: grid values must be positive or zero
def getLatitude(grid_y):
    return START_GRID_LATITUDE - grid_y * GRID_STEP_LATITUDE


#criacao dos consumidores para as varias queries


c_freq_routes = createConsumer('freq_routes')
c_profit_areas = createConsumer('profit_areas')
c_idle_taxis = createConsumer('idle_taxis')
c_cong_areas = createConsumer('cong_areas')
c_pleas_driver = createConsumer('pleasant_driver')
payload = []

for msg in c_profit_areas:
    payload.append(getPayload(msg.value))
    print("debug")
    events = getSorted(payload, 1, 10)
    events_np = np.array(events)
    profits = events_np[:,1]
    coords = #o que por aqui?!?
    print(coords)
    