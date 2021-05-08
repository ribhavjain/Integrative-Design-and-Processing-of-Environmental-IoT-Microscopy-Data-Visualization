#!/usr/bin/env python
import logging
import subprocess
import json
import pyclowder
import sys
import datetime
import pyclowder.files
import pyclowder.datasets
from pyclowder.extractors import Extractor
from influxdb import client as influxdb


def fetchInfluxDBPayload(startTime=0, endTime=0, username='', password='', userkey=''):
    userkey = ''
    username = ''
    password = ''
    startTime *= 1000000000  # Converting to Billionth of Seconds
    endTime *= 1000000000  # Converting to Billionth of Seconds

    influx = influxdb.InfluxDBClient('senselet.4ceed.illinois.edu', 8086, username, password, 'senseletdb')
    influx.switch_database('senseletdb')
    query = 'select * from temp_humi_measurement WHERE time > ' + str(int(startTime)) + ' AND time < ' + str(int(endTime))
    # using str(int(x)) prevents the use of e in the large epoch time values
    payload = {}
    
    result = influx.query(query)
    if len(list(result)) == 0:  # no data for given time range
        return payload

    for i in list(result)[0]:
        key = i['time']
        del i['time']
        if key in payload:
            payload[key].append(i)
        else:
            payload[key] = [i]
    return payload


class Senselet(Extractor):
    """Count the number of characters, words and lines in a text file."""
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def process_message(self, connector, host, secret_key, resource, parameters):
        logger = logging.getLogger(__name__)

        logger.info("Getting influxDB data")

        startTime = (parameters['parameters'].split(',')[0][10:-1]).split('/')  # Processing Date Parameters
        endTime = (parameters['parameters'].split(',')[1][7:-2]).split('/')  # Processing Date Parameters

        start = (datetime.datetime(int(startTime[2]), int(startTime[0]), int(startTime[1]), 0, 0) - datetime.datetime(1970, 1, 1)).total_seconds()
        end = (datetime.datetime(int(endTime[2]), int(endTime[0]), int(endTime[1]), 0, 0) - datetime.datetime(1970, 1, 1)).total_seconds()
        # start and end are the respective start and end times in epoch that we will use for our InfluxDB query

        payload = fetchInfluxDBPayload(start, end)
        dataset_id = resource["id"]

        metadata = self.get_metadata(payload, 'dataset', dataset_id, host)
        logger.info("Uploading metadata : " + str(metadata))

        result = pyclowder.datasets.upload_metadata(connector, host, secret_key, dataset_id, metadata)


if __name__ == "__main__":
    extractor = Senselet()
    extractor.start()


# response = fetch(startTime, endTime, username, password, userkey, dataset_id)
