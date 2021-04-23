import logging
from datetime import datetime, timedelta, timezone
from functools import cached_property
from typing import Dict, List, Tuple, Callable, Any

import dateutil.parser
import lightspeed_api
import matplotlib.pyplot as plt
import numpy as np

from httpconnection import HttpConnectionBase


def assert_http_status(response, status_code, message):
    if response.status_code != status_code:
        raise Exception(message, response)


def assigned_employee(json) -> List[Tuple[int, int]]:
    return [(int(workorder['employeeID']), int(workorder['workorderStatusID'])) for workorder in json]


def print_assigned_employee_count(counter, employees, statuses):
    lines = [f'{employees[employee_id]}:{statuses[status_id]}:{count}'
             for ((employee_id, status_id), count) in counter.items()]
    print(*sorted(lines), sep='\n')


def plot_workorder_status(counter, employees, statuses):
    employee_ids = list(employees.keys())
    status_ids = list(statuses.keys())
    y = np.arange(0, len(employee_ids), 1)
    x = np.arange(0, len(status_ids), 1)
    z = np.zeros((len(y), len(x)))
    for ((employee_id, status_id), count) in counter.items():
        z[employee_ids.index(employee_id), status_ids.index(status_id)] = count

    plot_pmesh(x, y, z, list(statuses.values()), list(employees.values()), 'Status', 'Employee')


def plot_pmesh(x, y, z, xticks, yticks, xlabel, ylabel):
    fig = plt.figure(figsize=(10, 6))
    ax = plt.axes()
    pmesh = ax.pcolormesh(x, y, z, shading='auto')
    fig.colorbar(pmesh, ax=ax)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if xticks:
        plt.xticks(x, xticks)
    plt.xticks(rotation='vertical')
    plt.yticks(y, yticks)
    plt.subplots_adjust(left=0.17, bottom=0.25, right=1.05)
    plt.show()


def plot_workorder_allocation(workorders, employees):
    workorders_by_employee = group_by(workorders, lambda x: int(x['employeeID']))
    date_ranges, (date_min, date_max) = get_date_ranges(workorders)
    logging.debug(f'min-date:{date_min.date()} - max-date:{date_max.date()}')

    employee_ids = list(employees.keys())
    y = np.arange(0, len(employee_ids), 1)
    x = [(date_min+timedelta(days=ij)).date() for ij in range((date_max-date_min).days)]
    z = np.zeros((len(y), len(x)))
    for row, employee_id in enumerate(workorders_by_employee.keys()):
        for workorder in workorders_by_employee[employee_id]:
            z[row, :] += date_allocation_array(date_min, date_max, date_range(workorder))

    plot_pmesh(x, y, z, [], list(employees.values()), 'Date', 'Employee')
    plt.gcf().autofmt_xdate()


def date_range(workorder) -> Tuple[datetime, datetime]:
    return dateutil.parser.parse(workorder['timeIn']), dateutil.parser.parse(workorder['etaOut'])


def get_date_ranges(workorders) -> Tuple[List[Tuple[datetime, datetime]], Tuple[datetime, datetime]]:
    date_ranges = [date_range(workorder) for workorder in workorders]
    date_min = min([min(x) for x in date_ranges])
    date_max = max([max(x) for x in date_ranges])

    return date_ranges, (date_min, date_max)


def date_allocation_array(date_min: datetime, date_max: datetime, date_range: Tuple[datetime, datetime]):
    start_date = min(date_range)
    end_date = max(date_range)
    assigned_days = np.zeros((date_max - date_min).days)
    assigned_days[(start_date - date_min).days:(end_date - date_min).days] = 1
    return assigned_days


def group_by(items: List, grouper: Callable) -> Dict[Any, List[Any]]:
    keys = set(map(grouper, items))
    groups = dict()
    for key in keys:
        groups[key] = [item for item in items if grouper(item) == key]

    return groups


class LightspeedConnection(HttpConnectionBase):

    def __init__(self, cache_file: str, account_id: str, client_id: str, client_secret: str, refresh_token: str):
        self.cache_file = cache_file
        self.account_id = account_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.lightspeed = lightspeed_api.Lightspeed(self.__lightspeed_config)

    def get_workorder_items(self):
        # 1 week ago
        week_ago = datetime.now() - timedelta(days=21)
        est_time_week_ago = week_ago.replace(tzinfo=timezone(timedelta(hours=-5), name="EST"))
        workorders = self.lightspeed.get('Workorder', {'timeIn': f'>,{est_time_week_ago.isoformat("T", "seconds")}'})[
            'Workorder']

        # Remove uninteresting ones
        workorders = list(self.__filter_workorders(workorders, lambda x: self.__get_status(x) not in ['Done & Paid',
                                                                                                      'Finished']))

        # Get useful information from them
        # assignee_count = Counter(assigned_employee(workorders))
        # print_assigned_employee_count(assignee_count, self.employees, self.workorder_statuses)
        # plot_workorder_status(assignee_count, self.employees, self.workorder_statuses)
        plot_workorder_allocation(workorders, self.employees)

    def __filter_workorders(self, workorders: List[Dict], filter: Callable) -> List[Dict]:
        for workorder in workorders:
            if filter(workorder):
                yield workorder

    def __get_status(self, workorder) -> str:
        return self.workorder_statuses[int(workorder['workorderStatusID'])]

    @cached_property
    def workorder_statuses(self) -> Dict[int, str]:
        statuses = self.lightspeed.get('WorkorderStatus')['WorkorderStatus']
        # status_objects = [WorkorderStatus(**status) for status in statuses]
        return dict([(int(status['workorderStatusID']), status['name']) for status in statuses])

    @cached_property
    def employees(self) -> Dict[int, str]:
        employees = self.lightspeed.get('Employee')['Employee']
        return dict([(int(employee['employeeID']), f'{employee["firstName"]} {employee["lastName"]}')
                     for employee in employees])

    @cached_property
    def __lightspeed_config(self):
        return {
            'account_id': self.account_id,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }
