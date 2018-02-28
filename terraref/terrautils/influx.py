"""Influx

This module provides methods for logging to an InfluxDB instance.
"""

import os
from dateutil.parser import parse
from influxdb import InfluxDBClient, SeriesHelper


def add_arguments(parser):

    parser.add_argument('--influxHost', dest="influx_host", type=str, nargs='?',
                        default=os.getenv("INFLUXDB_HOST", "terra-logging.ncsa.illinois.edu"),
                        help="InfluxDB URL for logging")
    parser.add_argument('--influxPort', dest="influx_port", type=int, nargs='?',
                        default= os.getenv("INFLUXDB_PORT", 8086),
                        help="InfluxDB port")
    parser.add_argument('--influxUser', dest="influx_user", type=str, nargs='?',
                        default=os.getenv("INFLUXDB_USER", "terra"),
                        help="InfluxDB username")
    parser.add_argument('--influxPass', dest="influx_pass", type=str, nargs='?',
                        default=os.getenv("INFLUXDB_PASSWORD", ''),
                        help="InfluxDB password")
    parser.add_argument('--influxDB', dest="influx_db", type=str, nargs='?',
                        default=os.getenv("INFLUXDB_DB", "extractor_db"),
                        help="InfluxDB database")


class Influx():

    def __init__(self, host, port, db, user, pass_):

        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.pass_ = pass_


    def log(self, extractorname, starttime, endtime, filecount, bytecount):

        f_completed_ts = int(parse(endtime).strftime('%s'))*1000000000
        f_duration = f_completed_ts - int(parse(starttime).strftime('%s'))*1000000000

        if self.pass_:
            client = InfluxDBClient(self.host, self.port, self.user,
                                    self.pass_, self.db)

            client.write_points([{
                "measurement": "file_processed",
                "time": f_completed_ts,
                "fields": {"value": f_duration}
            }], tags={"extractor": extractorname, "type": "duration"})
            client.write_points([{
                "measurement": "file_processed",
                "time": f_completed_ts,
                "fields": {"value": int(filecount)}
            }], tags={"extractor": extractorname, "type": "filecount"})
            client.write_points([{
                "measurement": "file_processed",
                "time": f_completed_ts,
                "fields": {"value": int(bytecount)}
            }], tags={"extractor": extractorname, "type": "bytes"})


    def error(self):
        # TODO: Allow sending critical error notification, e.g. email or Slack?
        pass