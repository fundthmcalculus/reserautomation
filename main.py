import argparse
import json
import logging
import os
import sys
from typing import Dict, Union, Callable, List

from shippolink import ShippoConnection
from smartetailing.connection import SmartetailingConnection
import lightspeedconnection
import kmlutilities


def create_function_map() -> Dict[str, Callable]:
    return {'syncshippo': sync_shippo,
            'downloadschedule': download_lightspeed_schedule,
            'displayschedule': display_schedule_info,
            'traillength': trail_length
            }


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', help="Perform an action", choices=create_function_map().keys())
    parser.add_argument('--kmlfile', help="Get the KML file")
    parser.add_argument('--reportfile', help="Output csv file")
    parser.add_argument('--summarycolumns', help="Summary columns for report, separated by commas `,`")
    return parser.parse_args()


def parse_config() -> Dict[str, Dict[str, Union[str, int]]]:
    with open("config.json") as f:
        config = json.load(f)
    return config


def sync_shippo() -> None:
    config = parse_config()
    etailing_config: Dict[str, Union[str, int]] = config["smartetailing"]
    shippo_config: Dict[str, Union[str, int, List[str]]] = config["shippo"]
    api_key: str = shippo_config["apikey"]

    shippo_connection = ShippoConnection(api_key,
                                         shippo_config['skipshippingclassification'],
                                         shippo_config['skiporderstatus'])
    smartetailing_connection = SmartetailingConnection(etailing_config["baseurl"],
                                                       etailing_config["merchant_id"],
                                                       etailing_config["url_key"])

    sent_orders = shippo_connection.send_to_shippo(config["return_address"], smartetailing_connection.export_orders())
    # Assuming we made it this far, everything worked, update the smart-etailing orders to not download again.
    smartetailing_connection.confirm_order_receipts(sent_orders)


def download_lightspeed_schedule() -> None:
    logging.info("Downloading lightspeed work order schedule")
    config: Dict = parse_config()["lightspeed"]

    # Load the existing data file
    connection = lightspeedconnection.LightspeedConnection(config["cache_file"],
                                                           config['account_id'],
                                                           config["client_id"],
                                                           config["client_secret"],
                                                           config["token_info"]["refresh_token"])
    # TODO - Connect to lightspeed
    connection.get_workorder_items()
    # TODO - Pull workorder data
    # TODO - Append to data table
    # TODO - Store back to data file
    pass


def display_schedule_info() -> None:
    raise NotImplementedError


def trail_length() -> None:
    args = parse_arguments()
    summary_columns = args.summarycolumns.split(',')
    logging.info(f"Parsing KML file={args.kmlfile}, summary columns={summary_columns}")
    doc = kmlutilities.parse_file(args.kmlfile)
    kmlutilities.process_kml_data(doc, summary_columns, args.reportfile)


def main():
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    log_file = os.path.join(dir_path, 'lightspeedsync.log')
    logging.basicConfig(filename=log_file,
                        format='%(asctime)s:%(levelname)s:%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filemode='w',
                        level=logging.DEBUG)
    logging.debug('Started')
    try:
        args = parse_arguments()
        func = create_function_map()[args.command]
        func()
        logging.debug('Finished')

        sys.exit(0)
    except Exception as err:
        logging.exception('Fatal error in main:', exc_info=True)
        sys.exit(-1)


if __name__ == '__main__':
    main()
