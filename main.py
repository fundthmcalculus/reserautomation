import argparse
import datetime
import json
import logging
import os
import sys
import urllib.parse
from typing import Dict, Union, Callable, List

import boto3
import pandas

from shippolink import ShippoConnection
from smartetailing.connection import SmartetailingConnection
import lightspeedconnection


def create_function_map() -> Dict[str, Callable]:
    return {'syncshippo': sync_shippo,
            'downloadschedule': download_lightspeed_schedule,
            'displayschedule': display_schedule_info,
            'inventoryspreadsheet': inventory_spreadsheet,
            'democonfigupdate': update_sample_config
            }


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', help="Perform an action", choices=create_function_map().keys())
    return parser.parse_args()


def parse_config() -> Dict[str, Dict[str, Union[str, int]]]:
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    config_file = os.path.join(dir_path, 'config.json')
    with open(config_file) as f:
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
    # Connect to lightspeed
    connection.get_workorder_items()
    # TODO - Pull workorder data
    # TODO - Append to data table
    # TODO - Store back to data file
    pass


def display_schedule_info() -> None:
    raise NotImplementedError


def inventory_spreadsheet() -> None:
    logging.info("Updating inventory spreadsheet from lightspeed")
    lightspeed_config: Dict = parse_config()["lightspeed"]
    aws_config: Dict = parse_config()["aws"]

    connection = lightspeedconnection.LightspeedConnection(lightspeed_config["cache_file"],
                                                           lightspeed_config['account_id'],
                                                           lightspeed_config["client_id"],
                                                           lightspeed_config["client_secret"],
                                                           lightspeed_config["token_info"]["refresh_token"])
    inventory_items = connection.get_inventory()

    margin = lambda price, cost: (price - cost) / price if price > 0 else 0.0

    report_items = [{'System ID': item['systemSku'],
                     'UPC': int(item['upc'] or "0"),
                     'EAN': item['ean'],
                     'Custom SKU': item['customSku'],
                     'Manufact. SKU': item['manufacturerSku'],
                     'Item': item['description'],
                     'Remaining': int(item['ItemShops']['ItemShop'][0]['qoh']),  # TODO - Handle other shops?
                     'Total Cost': float(item['defaultCost']),
                     'Avg. Cost': float(item['avgCost']),  # TODO - Handle rewriting this with default if 0
                     'Sale Price': float(item['Prices']['ItemPrice'][0]['amount']),  # TODO - Handle finding the MSRP
                     'Margin': margin(float(item['Prices']['ItemPrice'][0]['amount']), float(item['defaultCost']))
                     } for item in inventory_items if int(item['ItemShops']['ItemShop'][0]['qoh']) > 0]

    # Reporting dataframe
    df = pandas.DataFrame(report_items)
    # Handle currency and percent columns.
    df['UPC'] = df['UPC'].apply(lambda x: f'{x:014.0f}')
    df['Total Cost'] = df['Total Cost'].apply(lambda x: f'${x:.2f}')
    df['Avg. Cost'] = df['Avg. Cost'].apply(lambda x: f'${x:.2f}')
    df['Sale Price'] = df['Sale Price'].apply(lambda x: f'${x:.2f}')
    df['Margin'] = df['Margin'].apply(lambda x: f'{x*100:4.2f}%')

    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    csv_file = os.path.join(dir_path, aws_config["export_file"])
    df.to_csv(csv_file, index=False)

    # Upload to s3
    client = boto3.client(
        's3',
        aws_access_key_id=aws_config['access_key_id'],
        aws_secret_access_key=aws_config['access_key_secret']
    )
    split_url = urllib.parse.urlsplit(aws_config['s3_file_uri'])
    with open(aws_config['export_file'], "rb") as f:
        client.upload_fileobj(f, f"{split_url.netloc}", f"{split_url.path[1:]}")


def update_sample_config() -> None:
    config = parse_config()
    secret_entries = ["smartetailing/baseurl", "smartetailing/merchant_id", "smartetailing/url_key",
                      "shippo/apikey",
                      "lightspeed/account_id", "lightspeed/client_id", "lightspeed/client_secret",
                      "lightspeed/password", "lightspeed/userid",
                      "lightspeed/token_info/access_token", "lightspeed/token_info/refresh_token"]

    # Search through the dictionary and rewrite the secrets.
    for secret_to_clean in secret_entries:
        my_config = config
        all_keys = secret_to_clean.split('/')
        for key in all_keys[:-1]:
            my_config = my_config[key]
        my_config[all_keys[-1]] = '***SECRET***'

    config_file = "sample_config.json"
    with open(config_file, 'w') as f:
        json.dump(config, f, sort_keys=True, indent=2)


def main():
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    log_file = os.path.join(dir_path, 'lightspeedsync.log')
    logging.basicConfig(filename=log_file,
                        format='%(asctime)s:%(levelname)s:%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filemode='w',
                        level=logging.DEBUG)
    logging.debug(f'Started argv={sys.argv}  path={os.getcwd()}')
    time_now = datetime.datetime.now()
    try:
        args = parse_arguments()
        func = create_function_map()[args.command]
        func()
        time_end = datetime.datetime.now()
        logging.debug(f'Finished {(time_end-time_now).seconds} sec')

        sys.exit(0)
    except Exception as err:
        logging.exception('Fatal error in main:', exc_info=True)
        time_end = datetime.datetime.now()
        logging.debug(f'Finished {(time_end - time_now).seconds} sec')
        sys.exit(-1)


if __name__ == '__main__':
    main()
