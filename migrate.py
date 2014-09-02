#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#	   import.py
#	   
#	   Copyright 2012 Gaston Avila <avila.gas@gmail.com>
#	   
#	   This program is free software; you can redistribute it and/or modify
#	   it under the terms of the GNU General Public License as published by
#	   the Free Software Foundation; either version 2 of the License, or
#	   (at your option) any later version.
#	   
#	   This program is distributed in the hope that it will be useful,
#	   but WITHOUT ANY WARRANTY; without even the implied warranty of
#	   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#	   GNU General Public License for more details.
#	   
#	   You should have received a copy of the GNU General Public License
#	   along with this program; if not, write to the Free Software
#	   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#	   MA 02110-1301, USA.

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import csv
import codecs

FOLDER = 'tables/'

def migrate(inputFilename, outputFilename, mapFunction, fieldnames):
	with open(inputFilename, 'r') as infile:
		reader = csv.DictReader(infile)
		with open(outputFilename, 'w') as outfile:
			writer = csv.DictWriter(outfile, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
			writer.writeheader()
			for row in reader:
				d = mapFunction(row)
				if not d:
					continue
				writer.writerow(d)

def generateStops(inputFilename):
	logger.info("Importing stops from : %s", inputFilename)
	outputFilename = FOLDER + 'stops.csv'
	keyMap = {
		'stop_id': 'stop_id', 
		'stop_lat': 'stop_lat',
		'stop_lon': 'stop_lon',
		'entre': 'stop_entre', 
		'calle': 'stop_calle', 
		'numero': 'stop_numero', 
		'stop_name': 'stop_name'
		}
	def mapFunction(inDict):
		d = {v:codecs.decode(inDict[k],'utf8') for k, v in keyMap.items()}
		d = {k:v.encode('utf8') for k, v in d.items()}
		d.update({
			'stop_lat': float(d['stop_lat']),
			'stop_lon': float(d['stop_lon'])
			})
		return d
	migrate(inputFilename, outputFilename, mapFunction, keyMap.values())

def generateTrips(inputFilename):
	outputFilename = FOLDER + 'trips.csv'
	logger.info("Importing trips from : %s", inputFilename)
	keyMap = {
		'ID-recorrido': 'trip_id', 
		'ida-vuelta': 'direction_id',
		'descripcion': 'trip_headsign',
		'destino': 'trip_short_name',
		}
	def mapFunction(inDict):
		codigo = inDict['route-trip']
		route_id = codigo[:-3]
		if route_id.isdigit():
			route_id = route_id.zfill(3)
		d = {v:codecs.decode(inDict[k],'utf8') for k, v in keyMap.items()}
		d['route_id'] = route_id
		d['shape_id'] = d['trip_id']
		d = {k:v.encode('utf8') for k, v in d.items()}
		return d
	fieldnames = keyMap.values()
	fieldnames.extend(['route_id', 'shape_id'])
	migrate(inputFilename, outputFilename, mapFunction, fieldnames)

def generateRoutes(inputFilename):
	outputFilename = FOLDER + 'routes.csv'
	logger.info("Importing trips from : %s", inputFilename)

	def mapFunction(inDict):
		codigo = inDict['route-trip']
		route_id = codigo[:-3]
		if route_id.isdigit():
			route_id = route_id.zfill(3)
		d = {}
		d['route_id'] = route_id
		# d['route_short_name'] = codecs.decode(inDict['descripcion'],'utf8')
		d['route_short_name'] = route_id
		d['route_type'] = 'Bus'
		d['active'] = None
		d['route_desc'] = codecs.decode(inDict['descripcion'],'utf8')
		d = {k:v.encode('utf8') for k, v in d.items() if v}
		return d
	fieldnames = ['route_id', 'route_short_name', 'route_type', 'route_desc', 
		'active']
	migrate(inputFilename, outputFilename, mapFunction, fieldnames)

def generateShapes(inputFilename):
	outputFilename = FOLDER + 'shapes.csv'
	logger.info("Importing shapes from : %s", inputFilename)
	keyMap = {
		"shape_id": "shape_id",
		"shape_pt_lat": "shape_pt_lat",
		"shape_pt_lon": "shape_pt_lon",
		# "direction": "direction",
		"time": "shape_pt_time",
		}

	generateShapes.i = 0
	def mapFunction(inDict):
		d = {v:inDict[k] for k, v in keyMap.items()}
		lat, lon = d['shape_pt_lat'], d['shape_pt_lon']
		try:
			lat, lon = float(lat), float(lon)
		except Exception, e:
			logger.info("droped point", d)
			return False
		try:
			assert lat > -180
			assert lat < 180
			assert lon < 90
			assert lon > -90
		except Exception, e:
			logger.info("invalid coords")
			logger.info(d)
			return False
		d.update({'shape_pt_lat': lat, 'shape_pt_lon': lon, 
			'shape_pt_sequence': generateShapes.i + 300000})

		generateShapes.i += 1
		return d
	fieldnames = keyMap.values()
	fieldnames.append('shape_pt_sequence')
	migrate(inputFilename, outputFilename, mapFunction, fieldnames)
	logger.info("done importing shapes")

def generateStopTimes(inputFilename):
	outputFilename = FOLDER + 'stop_times.csv'
	logger.info("Importing stop_times from : %s", inputFilename)
	keyMap = {
		"stop_id": "stop_id",
		"trip_id": "trip_id",
		"time": "stop_time"
		}
	generateStopTimes.i = 0
	def mapFunction(inDict):
		d = {v:inDict[k] for k, v in keyMap.items()}
		d.update({'stop_sequence': generateStopTimes.i + 100000})
		generateStopTimes.i += 1
		return d

	fieldnames = keyMap.values()
	fieldnames.append('stop_sequence')
	migrate(inputFilename, outputFilename, mapFunction, fieldnames)

	logger.info("done importing stop times")

def generateTripStartTimes(filename):
  infile = open(filename)
  reader = csv.DictReader(infile)

  outfile = open(FOLDER + 'trips_start_times.csv', 'w')
  fieldnames = ['trip_id', 'service_id', 'start_time']
  writer = csv.DictWriter(outfile, fieldnames = fieldnames, quoting=csv.QUOTE_NONNUMERIC)
  writer.writeheader()
  for row in reader:
    for service_id in ['H', 'S', 'D']:
      if row[service_id]:
        d = {
          'trip_id': row['trip_id'],
          'service_id': service_id,
          'start_time': row[service_id]
        }
        writer.writerow(d)
  outfile.close()
  infile.close()

if __name__ == '__main__':
	generateStops('stops.csv')
	generateTrips('routes-trips-clean.csv')
	generateRoutes('routes-trips-clean.csv')
	generateShapes('shapes-raw.csv')
	generateStopTimes('stop_times.csv')
	generateTripStartTimes('salidas.csv')
