#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import argparse
import json
import csv
import os
import socket
import logging
import datetime
import threading
import time
import Queue
import sys

from ssl import SSLError

sys.path.insert(1, os.path.abspath("/Users/cda/Research/telescope"))

import telescope.selector
import telescope.query
import telescope.utils
import telescope.metrics_math
import telescope.mlab
import telescope.filters
import telescope.external

def setup_logger(verbosity_level = 0):
  logger = logging.getLogger('telescope')
  console_handler = logging.StreamHandler()
  logger.addHandler(console_handler)

  if verbosity_level > 0:
    logger.setLevel(logging.DEBUG)
  else:
    logger.setLevel(logging.INFO)
  return logger

def create_directory_if_not_exists(passed_selector):
  """ extant_file
      'Type' for argparse - checks that file exists but does not open.

  """
  if not os.path.exists(passed_selector):
    try:
      os.mkdir(passed_selector)
    except OSError:
      raise argparse.ArgumentError(('{0} does not exist, is not readable or '
                                    'could not be created.').format(passed_selector))
  return passed_selector

def retrieve_data_upon_job_completion(job_id, metadata, query_object = None):
  logger = logging.getLogger('telescope')

  if query_object is not None:
    bq_query_returned_data = query_object.retrieve_job_data(job_id)
    write_metric_calculations_to_file(metadata['data_filepath'], bq_query_returned_data)
  return None

def write_metric_calculations_to_file(data_filepath, metric_calculations, should_write_header = False):
  logger = logging.getLogger('telescope')
  try:
    with open(data_filepath, 'w') as data_file_raw:
      if type(metric_calculations) is list and len(metric_calculations) > 0:
        data_file_csv = csv.DictWriter(data_file_raw,
                                        fieldnames = metric_calculations[0].keys(),
                                        delimiter=',',
                                        quotechar='"', quoting=csv.QUOTE_MINIMAL)
        data_file_csv.writeheader()
        for metric_calculation in metric_calculations:
          for m_key in metric_calculation.keys():
            if type(metric_calculation[m_key]) == str or type(metric_calculation[m_key]) == unicode:
              metric_calculation[m_key] = metric_calculation[m_key].encode("utf-8")
          data_file_csv.writerow(metric_calculation)
    return True
  except Exception as caught_error:
    logger.error("When writing raw output, caught {error}.".format(error = caught_error))
  return False

def build_filename(resource_type, outpath, date, duration, site, client_provider):
  extensions = { 'data': 'raw.csv', 'bigquery': 'bigquery.sql'}
  filename_format = "{date}+{duration}_{site}_{client_provider}-{extension}"

  filename = filename_format.format(date = date,
                                    duration = duration,
                                    site = site,
                                    client_provider = client_provider,
                                    extension = extensions[resource_type])
  filepath = os.path.join(outpath, filename)
  return filepath

def write_bigquery_to_file(bigquery_filepath, query_string):
  logger = logging.getLogger('telescope')
  try:
    with open(bigquery_filepath, 'w') as bigquery_file_raw:
      bigquery_file_raw.write(query_string)
    return True
  except Exception as caught_error:
    logger.error("When writing bigquery, caught {error}.".format(error = caught_error))

  return False

def main(args):

  selector_queue = Queue.Queue()
  concurrent_thread_limit = 18
  logger = setup_logger(args.verbosity)

  thread_metadata = {
                    'date': datetime.datetime.now(),
                    'duration': 'na',
                    'site': 'manual',
                    'client_provider': 'manual',
                  }

  try:
    google_auth_config = telescope.external.GoogleAPIAuth(args.credentials_filepath, is_headless = args.noauth_local_webserver)
    google_auth_config.project_id = "833893705802"
    bq_query_call = telescope.external.BigQueryCall(google_auth_config)
    bq_job_id = args.job_id

    data_fileformat = 'manual-{bq_job_id}.csv'.format(bq_job_id = bq_job_id)
    thread_metadata['data_filepath'] = os.path.join(args.output, data_fileformat)
    retrieve_data_upon_job_completion(bq_job_id, thread_metadata, bq_query_call)

  except telescope.external.APIConfigError:
    logger.error("Could not find developer project, please create one in " +
                 "Developer Console to continue. (See README.md)")
    return None
                                                      
if __name__ == "__main__":
  parser = argparse.ArgumentParser(
  prog='M-Lab Observatory Support and Data Production Tool',
  formatter_class=argparse.ArgumentDefaultsHelpFormatter)

  parser.add_argument('job_id', default=None,
                  help='Selector JSON datafile(s) to parse.')
  parser.add_argument('-v', '--verbosity', action="count",
                  help="variable output verbosity (e.g., -vv is more than -v)")
  parser.add_argument('-o', '--output', default='.',
                  help='Output file path. If the folder does not exist, it will be created.',
                  type=create_directory_if_not_exists)
  parser.add_argument('--credentialspath', dest='credentials_filepath', default='bigquery_credentials.dat',
                help='Google API Credentials. If it does not exist, will trigger Google auth.')
  parser.add_argument('--noauth_local_webserver', default=False, action='store_true',
                      help='Authenticate to Google using another method than a local webserver')
  
  args = parser.parse_args()

  main(args)
