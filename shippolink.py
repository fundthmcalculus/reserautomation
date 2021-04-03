import logging
import shippo
from datetime import datetime
from typing import List, Dict, Union, Set, Iterator

import requests
from smartetailing import objects
from httpconnection import HttpConnectionBase


class ShippoConstants:
    US_DOLLAR = "USD"
    US = "US"
    UNITED_STATES = "United States"
    POUND = "lb"
    INCH = "in"


class ShippoOrderStatus:
    PAID = "PAID"


class ShippoConnection(HttpConnectionBase):
    SHIPPO_BASE_URL = "https://api.goshippo.com/orders/"

    def __init__(self, api_key: str, skip_shipping_classification=None, skip_order_status=None):
        if skip_shipping_classification is None:
            skip_shipping_classifications = ["In-Store Pickup"]
        if skip_order_status is None:
            skip_order_statuses = ['received', 'being processed']

        shippo.config.api_key = api_key
        self.__api_key = api_key
        shipment_list: List[shippo.Shipment] = shippo.Shipment.all()
        id = shipment_list[0]['object_id']
        self.__existing_shippo_order_ids: Set[str] = None
        self.__skip_shipping_classification = skip_shipping_classification
        self.__skip_order_status = skip_order_status

    @property
    def existing_shippo_order_ids(self) -> Set[str]:
        if self.__existing_shippo_order_ids is None:
            self.__existing_shippo_order_ids = self.__get_existing_shippo_order_ids()
        return self.__existing_shippo_order_ids

    def send_to_shippo(self, return_address: Dict[str, str], orders: Iterator[objects.Order]) -> Iterator[str]:
        shippo_orders: Iterator[objects.Order] = self.skip_existing_orders(
            self.use_only_received_orders(
                self.skip_in_store_pickup(orders)))
        for order in shippo_orders:
            order_json = create_shippo_order(return_address, order)
            self.__create_order(order_json)
            yield order.id

    def skip_existing_orders(self, orders: Iterator[objects.Order]) -> Iterator[objects.Order]:
        for order in orders:
            if '#' + order.id in self.existing_shippo_order_ids:
                logging.info(f"SKIPPED: Order #{order.id} already in Shippo")
            else:
                yield order

    def skip_in_store_pickup(self, orders: Iterator[objects.Order]) -> Iterator[objects.Order]:
        for order in orders:
            if order.shipping.classification in self.__skip_shipping_classification:
                logging.info(f"SKIPPED: Order #{order.id} shipping={order.shipping.classification}")
            else:
                yield order

    def use_only_received_orders(self, orders: Iterator[objects.Order]) -> Iterator[objects.Order]:
        for order in orders:
            if order.status.lower() in self.__skip_order_status:
                yield order
            else:
                logging.info(f"SKIPPED: Order #{order.id} in status={order.status}")

    def __get_existing_shippo_order_ids(self) -> Set[str]:
        response = requests.get(ShippoConnection.SHIPPO_BASE_URL, headers={
            "Authorization": f"ShippoToken {self.__api_key}",
        })
        self._handle_response(response)
        response_json = response.json()
        if response_json["next"] is not None or response_json["previous"] is not None:
            # TODO - Handle next and previous
            raise NotImplementedError("Unhandled JSON pagination")
        else:
            return set([obj["order_number"] for obj in response_json["results"]])

    def __create_order(self, order_json: dict) -> None:
        response = requests.post(ShippoConnection.SHIPPO_BASE_URL, headers={
            "Authorization": f"ShippoToken {self.__api_key}",
        }, json=order_json)
        # Assert success
        self._handle_response(response)
        logging.info(f"Created shippo order {order_json['order_number']}")


def create_shippo_order(return_address: Dict[str, str], order: objects.Order):
    # Get the shipment, load the addresses
    ship_to_address = create_address(order.ship_address)
    # Load the items list
    line_items: List[Dict[str, str]] = [create_line_item(item) for item in order.items]

    order_json = create_order(order.id,
                              ship_to_address,
                              return_address,
                              line_items,
                              order.order_total,
                              order.shipping.method)
    return order_json


def create_address(address: objects.AddressInfo) -> Dict[str, str]:
    # TODO - Debug remove
    address_name = address.name.full
    if address.address1 == "648 Monmouth St":
        address_name = "Reser Bicycle"
    return {
        "name": address_name,
        "street1": address.address1,
        "street2": address.address2,
        "city": address.city,
        "state": address.state,
        "zip": address.zip,
        "country": address.country,
        "phone": address.phone
    }


def create_parcel(weight: float = 0, length: int = 1, width: int = 1, height: int = 1, line_items: List[dict] = None) \
        -> Dict[str, Union[str, List[dict]]]:
    """
    Create the parcel object with a defined weight and dimensions
    :param line_items: items in the parcel - optional
    :param height: inches
    :param width: inches
    :param length: inches
    :param weight: lbs
    :return:
    """
    weight = override_weight(line_items, weight)
    return {
        "length": f"{length:.1f}",
        "width": f"{width:.1f}",
        "height": f"{height:.1f}",
        "distance_unit": ShippoConstants.INCH,
        "weight": f"{weight:.1f}",
        "mass_unit": ShippoConstants.POUND,
        "line_items": line_items
    }


def override_weight(line_items, weight):
    if weight == 0:
        if len(line_items) == 0:
            raise ValueError("Define weight or line items!")
        weight = sum([float(x["weight"]) for x in line_items])
    return weight


def create_customs_item(item: objects.Item) -> dict:
    return {
        "description": item.description,
        "quantity": item.quantity,
        "net_weight": item.weight,
        "mass_unit": ShippoConstants.POUND,
        "value_amount": item.unit_price,
        "value_currency": ShippoConstants.US_DOLLAR,
        "origin_country": ShippoConstants.US,
        "tariff_number": ""
    }


def create_line_item(item: objects.Item) -> dict:
    return {
        "title": f"{item.description}",
        "sku": item.mpn,
        "quantity": item.quantity,
        "total_price": format_dollar(item.quantity * item.unit_price),
        "currency": ShippoConstants.US_DOLLAR,
        "weight": f"{item.weight:.2f}",
        "weight_unit": ShippoConstants.POUND,
        "manufacture_country": ShippoConstants.US
    }


def format_dollar(value: float) -> str:
    """
    Return a proper 2 decimal currency value
    :param value: Currency amount
    :return: currency value string
    """
    return f"{value:0.2f}"


def create_order(order_number: int, to_address: dict, from_address: dict, line_items: List[dict],
                 price_data: objects.OrderTotal, shipping_method: str) -> dict:
    weight = override_weight(line_items, 0)
    return {
        "order_number": f"#{order_number}",
        "order_status": ShippoOrderStatus.PAID,
        "to_address": to_address,
        "from_address": from_address,
        "line_items": line_items,
        "placed_at": datetime.now().isoformat(),
        "weight": f"{weight:.2f}",
        "weight_unit": ShippoConstants.POUND,
        "shipping_method": shipping_method,
        "shipping_cost": format_dollar(price_data.shipping),
        "shipping_cost_currency": ShippoConstants.US_DOLLAR,
        "subtotal_price": format_dollar(price_data.subtotal),
        "total_price": format_dollar(price_data.total),
        "total_tax": format_dollar(price_data.tax),
        "currency": ShippoConstants.US_DOLLAR
    }
