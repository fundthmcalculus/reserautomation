import dateutil.parser


class WorkorderStatus(object):
    def __init__(self, **kwargs):
        self.workorderStatusID = int(kwargs.get('workorderStatusID', 0))
        self.name = kwargs.get('name', '')
        self.sortOrder = int(kwargs.get('sortOrder', 0))
        self.htmlColor = kwargs.get('htmlColor', '')
        self.systemValue = kwargs.get('systemValue', '')


class Workorder(object):
    def __init__(self, **kwargs):
        self.workorderID = int(kwargs.get('workorderID', 0))
        self.timeIn = dateutil.parser.parse(kwargs.get('timeIn', '0000-01-01T00:00'))
        self.etaOut = dateutil.parser.parse(kwargs.get('etaOut', '0000-01-01T00:00'))
        self.note = kwargs.get('note', '')
        self.warranty = bool(kwargs.get('warranty', False))
        self.tax = bool(kwargs.get('tax', False))
        self.archived = bool(kwargs.get('archived', False))
        self.hookIn = kwargs.get('hookIn', '')
        self.hookOut = kwargs.get('hookOut', '')
        self.saveParts = bool(kwargs.get('saveParts', False))
        self.assignEmployeeToAll = bool(kwargs.get('assignEmployeeToAll', False))
        self.customerID = int(kwargs.get('customerID', 0))
        self.discountID = int(kwargs.get('discountID', 0))
        self.employeeID = int(kwargs.get('employeeID', 0))
        self.serializedID = int(kwargs.get('serializedID', 0))
        self.shopID = int(kwargs.get('shopID', 0))
        self.saleID = int(kwargs.get('saleID', 0))
        self.saleLineID = int(kwargs.get('saleLineID', 0))
        self.workorderStatusID = int(kwargs.get('workorderStatusID', 0))
        self.timeStamp = dateutil.parser.parse(kwargs.get('timeStamp', '0000-01-01T00:00'))

        # TODO - sub objects
        self.Customer = kwargs.get('Customer', 0)  # TODO
        self.Discount = kwargs.get('Discount', 0)  # TODO
        self.Employee = kwargs.get('Employee', 0)  # TODO
        self.Serialized = kwargs.get('Serialized', 0)  # TODO
        self.WorkorderStatus = kwargs.get('WorkorderStatus', 0)  # TODO
        self.WorkorderItems = kwargs.get('WorkorderItems', 0)  # TODO
        self.WorkorderLines = kwargs.get('WorkorderLines', 0)  # TODO
        self.CustomFieldValues = kwargs.get('CustomFieldValues', 0)  # TODO

