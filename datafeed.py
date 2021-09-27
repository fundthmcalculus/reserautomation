import logging
import os
from typing import Iterator, Dict, List, Union

import boto3
import urllib.parse

import pandas

from config import ReserConfig


def get_sale_items(recent_sales):
    return list(get_items_from_sale_lines(get_sale_lines(recent_sales)))


def get_sale_lines(all_sales: List[Union[List, Dict]]) -> Iterator[Dict]:
    for sale in all_sales:
        try:
            lines = sale['SaleLines']['SaleLine']
            if isinstance(lines, list):
                for line_item in lines:
                    yield line_item
            else:
                yield lines
        except KeyError:
            pass


def get_items_from_sale_lines(sale_lines: Iterator[Dict]) -> Iterator[Dict]:
    for line in sale_lines:
        try:
            line_items = line['Item']
            if isinstance(line, list):
                for item in line_items:
                    yield item
            else:
                yield line_items
        except KeyError:
            pass


def margin(price, cost) -> float:
    return (price - cost) / price if price > 0 else 0.0


def qoh(item):
    # TODO - Handle other shops?
    try:
        return int(item['ItemShops']['ItemShop'][0]['qoh'])
    except KeyError:
        return 0


def get_report_item(item) -> dict:
    return {'System ID': item['systemSku'],
            'UPC': int(item['upc'] or "0"),
            'EAN': item['ean'],
            'Custom SKU': item['customSku'],
            'Manufact. SKU': item['manufacturerSku'],
            'Item': item['description'],
            'Remaining': qoh(item),
            'Total Cost': float(item['defaultCost']),
            'Avg. Cost': float(item['avgCost']),  # TODO - Handle rewriting this with default if 0
            'Sale Price': float(item['Prices']['ItemPrice'][0]['amount']),  # TODO - Handle finding the MSRP
            'Margin': margin(float(item['Prices']['ItemPrice'][0]['amount']), float(item['defaultCost']))
            }


def create_and_upload_recent_sale(aws_config, report_items):
    report_df = create_dataframe(report_items)
    csv_file, mpn_csv_file = write_to_csv_file(report_df, aws_config['recent_sale_export_file'],
                                               aws_config['recent_sale_mpn_export_file'])
    upload_to_s3(csv_file, aws_config['s3_recent_sale_file_uri'])
    upload_to_s3(mpn_csv_file, aws_config['s3_recent_sale_mpn_file_uri'])


def create_and_upload_inventory(aws_config, inventory_items):
    inventory_df = create_dataframe(inventory_items)
    csv_file, mpn_csv_file = write_to_csv_file(inventory_df, aws_config['export_file'], aws_config['mpn_export_file'])
    upload_to_s3(csv_file, aws_config['s3_file_uri'])
    upload_to_s3(mpn_csv_file, aws_config['s3_mpn_file_uri'])


def create_dataframe(inventory_items):
    # Reporting dataframe
    inventory_df = pandas.DataFrame(inventory_items)
    # Handle currency and percent columns.
    inventory_df['UPC'] = inventory_df['UPC'].apply(lambda x: f'{x:014.0f}')
    inventory_df['Total Cost'] = inventory_df['Total Cost'].apply(lambda x: f'${x:.2f}')
    inventory_df['Avg. Cost'] = inventory_df['Avg. Cost'].apply(lambda x: f'${x:.2f}')
    inventory_df['Sale Price'] = inventory_df['Sale Price'].apply(lambda x: f'${x:.2f}')
    inventory_df['Margin'] = inventory_df['Margin'].apply(lambda x: f'{x * 100:4.2f}%')
    # Zero blank Manufacturing SKUs for datafeedwatch merge.
    inventory_df['Manufact. SKU'] = inventory_df['Manufact. SKU'].apply(lambda x: '0' if not x else x)
    return inventory_df


def write_to_csv_file(df, export_file, mpn_export_file):
    logging.info("Writing data frame to export csv")
    dir_path: str = os.path.dirname(os.path.realpath(__file__))
    csv_file = os.path.join(dir_path, export_file)
    df.to_csv(csv_file, index=False)
    logging.info("Writing MPN data to csv")
    # Secondary feed is solely MPN and quantity
    mpn_qty_df = df[['Manufact. SKU', 'Remaining']]
    mpn_qty_df = mpn_qty_df[mpn_qty_df['Manufact. SKU'] != '']
    mpn_csv_file = os.path.join(dir_path, mpn_export_file)
    mpn_qty_df.to_csv(mpn_csv_file, index=False)
    return csv_file, mpn_csv_file


def upload_to_s3(csv_file, s3_csv_uri):
    aws_config = ReserConfig.get_config()['aws']
    logging.info(f"Uploading {csv_file} to S3 {s3_csv_uri}")
    # Upload to s3
    client = boto3.client(
        's3',
        aws_access_key_id=aws_config['access_key_id'],
        aws_secret_access_key=aws_config['access_key_secret']
    )
    upload_csv_file(client, csv_file, s3_csv_uri)
    logging.info("File uploaded")


def upload_csv_file(client, csv_file, aws_file):
    split_url = urllib.parse.urlsplit(aws_file)
    with open(csv_file, "rb") as f:
        client.upload_fileobj(f, f"{split_url.netloc}", f"{split_url.path[1:]}")