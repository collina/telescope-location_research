import geoip2.database
import csv
import argparse

def main(args):
  geoip_reader = geoip2.database.Reader('support/GeoLite2-City.mmdb')
  expected_headers = [
                      'connection_spec_client_geolocation_latitude',
                      'connection_spec_client_geolocation_continent_code',
                      'connection_spec_client_geolocation_city',
                      'connection_spec_client_geolocation_country_code',
                      'connection_spec_client_geolocation_metro_code',
                      'connection_spec_client_ip',
                      'connection_spec_client_geolocation_longitude',
                      'connection_spec_client_geolocation_region'
                      ]
                      
  statistics = {}
  seen_ip_set = set()
  
  for result_file in args.results_in:
    result_file_header = result_file.readline().rstrip().split(',')
    result_csv = csv.DictReader(result_file, fieldnames = result_file_header)
    
    
    for result_line in result_csv:
      error_message = None
      seen_ip_set.add(result_line['web100_log_entry_connection_spec_remote_ip'])

      try:
        geoip_response = geoip_reader.city(result_line['web100_log_entry_connection_spec_remote_ip'])
      except geoip2.errors.AddressNotFoundError:
        error_message = "Missing Entry from Maxmind"
      
      if result_line['connection_spec_client_geolocation_country_code'] == '':
        error_message = "Missing Country Code from BigQuery"
      elif geoip_response.country.iso_code != result_line['connection_spec_client_geolocation_country_code']:
        error_message = "Incorrect Country Code"
      
      if error_message not in statistics:
        statistics[error_message] = set()
      
      if error_message != None:
        statistics[error_message].add(result_line['web100_log_entry_connection_spec_remote_ip'])

  print "Addresses Seen", len(seen_ip_set)
  erroneous_addresses = 0
  for error_message, error_ip_set in statistics.iteritems():
    print error_message, len(error_ip_set)
    erroneous_addresses += len(error_ip_set)
  print "Fine", (len(seen_ip_set) - erroneous_addresses)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(
                                   prog='M-Lab Telescope Location Experiment',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
  parser.add_argument('results_in', nargs='+', type=file, default=None, help='Selector CSV datafile(s) to parse.')
  args = parser.parse_args()
  main(args)
