import logging
import os
import sqlite3
import lightspeed_api

from httpconnection import HttpConnectionBase


def assert_http_status(response, status_code, message):
    if response.status_code != status_code:
        raise Exception(message, response)


class LightspeedConnection(HttpConnectionBase):

    def __init__(self, cache_file: str, account_id: str, client_id: str, client_secret: str, refresh_token: str):
        self.cache_file = cache_file
        self.account_id = account_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.lightspeed = lightspeed_api.Lightspeed(self.__lightspeed_config)
        self.conn: sqlite3.Connection = None
        # Constructor actions
        self.__ensure_db_exists()
        self.__connect_to_db()

    def get_workorder_items(self):
        scott_customer = self.lightspeed.get('Customer/30175', {'load_relations': 'all'})
        employees = self.lightspeed.get('Employee')
        workorders = self.lightspeed.get('Workorder')
        # TODO - Get useful information from them
        a = 1

    @property
    def __lightspeed_config(self):
        return {
            'account_id': self.account_id,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }

    def __connect_to_db(self):
        try:
            self.conn = sqlite3.connect(self.cache_file)
            print(f"SQLite Version={sqlite3.version}")
        except sqlite3.Error as e:
            logging.exception("Could not connect to sqlite db")

    def __ensure_db_exists(self):
        # Ensure the file exists
        full_path: str = os.path.abspath(self.cache_file)
        folder, file = os.path.split(full_path)
        try:
            os.makedirs(folder)
        except FileExistsError:
            pass
        with open(full_path, 'a') as fp:
            # Create an empty file if it doesn't exist
            pass

    def __del__(self):
        try:
            if self.conn:
                self.conn.close()
        except sqlite3.Error as e:
            logging.exception("Could not connect to sqlite db")
