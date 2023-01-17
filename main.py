import argparse
import datetime
import logging
import os
import sys
from typing import Dict, Union, Callable, List

import requests
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

from config import ReserConfig
from datafeed import qoh, create_and_upload_inventory, create_and_upload_recent_sale, get_report_item
from shippolink import ShippoConnection
from smartetailing.connection import SmartetailingConnection
import lightspeedconnection


def create_function_map() -> Dict[str, Callable]:
    return {'syncshippo': sync_shippo,
            'downloadschedule': download_lightspeed_schedule,
            'displayschedule': display_schedule_info,
            'inventoryspreadsheet': inventory_spreadsheet,
            'getaccesstoken': get_access_token,
            'downloadreviews': download_reviews
            }


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', help="Perform an action", choices=create_function_map().keys())
    return parser.parse_args()


def download_reviews() -> None:
    config = ReserConfig.get_config()
    review_url = "https://display.powerreviews.com/m/2568/l/en_US/product/0_0_387915/reviews?apikey=51e5c335-f79d-43e9-9c41-f3095d711fdb&_noconfig=true"
    response = requests.get(review_url)
    print(response.content)


def sync_shippo() -> None:
    config = ReserConfig.get_config()
    etailing_config: Dict[str, Union[str, int]] = config["smartetailing"]
    shippo_config: Dict[str, Union[str, int, List[str]]] = config["shippo"]

    shippo_connection = ShippoConnection(shippo_config["apikey"],
                                         include_order_status=shippo_config['include_order_status'])
    smartetailing_connection = SmartetailingConnection(etailing_config["base_url"],
                                                       etailing_config["merchant_id"],
                                                       etailing_config["url_key"],
                                                       etailing_config["web_url"],
                                                       etailing_config["username"],
                                                       etailing_config["password"])

    shippo_connection.send_to_shippo(config["return_address"], smartetailing_connection.export_orders())


def download_lightspeed_schedule() -> None:
    logging.info("Downloading lightspeed work order schedule")
    config: Dict = ReserConfig.get_config()["lightspeed"]

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


def get_access_token() -> None:
    logging.info("Getting access token from lightspeed")
    lightspeed_config: Dict = ReserConfig.get_config()["lightspeed"]

    connection = lightspeedconnection.LightspeedConnection(lightspeed_config["cache_file"],
                                                           lightspeed_config['account_id'],
                                                           lightspeed_config["client_id"],
                                                           lightspeed_config["client_secret"],
                                                           lightspeed_config["token_info"]["refresh_token"])
    connection.get_access_token()


def inventory_spreadsheet() -> None:
    logging.info("Updating inventory spreadsheet from lightspeed")
    lightspeed_config: Dict = ReserConfig.get_config()["lightspeed"]
    aws_config: Dict = ReserConfig.get_config()["aws"]

    connection = lightspeedconnection.LightspeedConnection(lightspeed_config["cache_file"],
                                                           lightspeed_config['account_id'],
                                                           lightspeed_config["client_id"],
                                                           lightspeed_config["client_secret"],
                                                           lightspeed_config["token_info"]["refresh_token"])
    inventory_items = connection.get_inventory()
    sale_days = int(lightspeed_config['sale_history_days'])
    recent_sale_items = connection.get_recent_sales(sale_days)

    logging.info("Sorting inventory data")
    inventory_system_skus = dict([(item['systemSku'], get_report_item(item)) for item in inventory_items if qoh(item) > 0])
    recent_sale_system_skus = dict([(item['systemSku'], get_report_item(item)) for item in recent_sale_items])
    report_system_skus = dict()
    report_system_skus.update(inventory_system_skus)
    report_system_skus.update(recent_sale_system_skus)

    create_and_upload_recent_sale(aws_config, report_system_skus.values())
    create_and_upload_inventory(aws_config, inventory_system_skus.values())


def main():
    """
    Main entry point for the application
    """
    initialize_logging()
    time_now = datetime.datetime.now()
    exit_code = 0
    try:
        args = parse_arguments()
        func = create_function_map()[args.command]
        func()
    except Exception as err:
        logging.exception('Fatal error in main')
        exit_code = -1
    finally:
        time_end = datetime.datetime.now()
        logging.debug(f'Finished {(time_end - time_now).seconds} sec')
        sys.exit(exit_code)


def initialize_logging():
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    config = ReserConfig.get_config()

    log_file = os.path.join(dir_path, config["logging"]["log_file"])
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO,
                        handlers=[
                            logging.FileHandler(log_file, mode='w'),
                            logging.StreamHandler(sys.stdout)
                        ])
    logging.debug(f'Started argv={sys.argv}  path={os.getcwd()}')

    sentry_logging = LoggingIntegration(
        level=logging.INFO,
        event_level=logging.ERROR
    )

    sentry_sdk.init(
        config["logging"]["sentry_url"],
        integrations=[sentry_logging],
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0
    )
    sentry_sdk.debug.configure_logger()


if __name__ == '__main__':
    main()
