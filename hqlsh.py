from ishell.console import Console
from ishell.command import Command
from terminaltables import AsciiTable
import base64
import requests
import struct

BASE_URL = 'http://10.5.36.102:8080'


# Method for encoding ints with base64 encoding
def encode(n, type):
    data = struct.pack(type, n)
    s = base64.b64encode(data)
    return s


# Method for decoding ints with base64 encoding
def decode(s, type):
    data = base64.b64decode(s)
    n = struct.unpack(type, data)
    return n[0]


def try_decode(s):
    types = ['q', 'l', 'f', 'd', 'h']
    for t in types:
        try:
            value = decode(s, t)
            if t == 'l' or t == 'h':
                value = int(value)
                # print 'int value', value
            elif t == 'q':
                value = long(value)
                # print 'int64 value', value
            elif t == 'f' or t == 'd':
                value = float(value)
                # print 'float value', value
            return value
        except:
            pass

    # try to decode string
    try:
        value = base64.b64decode(s)
        value = str.decode(value, 'utf8')
        return value
    except:
        return s


def print_table(data):
    table = AsciiTable(data)
    table.outer_border = False

    print '\n'
    print table.table
    print (len(data) - 1), 'rows\n'


def parse_to_table(json):
    headings = {'pk': 0}
    data = [[]]

    for row in json['Row']:
        key = base64.b64decode(row['key'])
        cells = [''] * len(headings)
        cells[0] = key

        for cell in row['Cell']:
            column = base64.b64decode(cell['column'])
            value = try_decode(cell['$'])

            order = headings.get(column)
            if order is None:
                order = len(headings)
                headings[column] = order
                cells.append(value)
            else:
                cells[order] = value
        data.append(cells)

    h = [''] * len(headings)
    for k in headings:
        h[headings[k]] = k
    data[0] = h

    return data


def get(table, row):
    try:
        headers = {'Accept': 'application/json'}
        r = requests.get('%s/%s/%s' % (BASE_URL, table, row), headers=headers)
        if r.status_code != 200:
            raise ValueError('Failed with code', r.status_code)

        return parse_to_table(r.json())
    except (ValueError, TypeError) as e:
        print 'Error', e


def scan(table, row_prefix, limit=100):
    body = """
    <Scanner batch="%d">
    <filter> { "type": "PrefixFilter", "value": "%s" } </filter>
    </Scanner>
    """ % (limit, row_prefix)
    print body

    try:
        headers = {'Accept': 'application/json', 'Content-Type': 'text/xml'}
        r = requests.put('%s/%s/scanner' % (BASE_URL, table), headers=headers, data=body)
        if r.status_code != 201:
            raise ValueError('Get scanner id failed with code', r.status_code)

        url = r.headers['Location']
        r = requests.get(url=url, headers={'Accept': 'application/json'})

        if r.status_code == 204:
            return
        elif r.status_code != 200:
            raise ValueError('Scan failed with code', r.status_code, 'at url', url)

        return parse_to_table(r.json())
    except (ValueError, TypeError) as e:
        print 'Error', e


class GetCommand(Command):
    def run(self, line):
        params = line.split()
        data = get(table=params[1], row=params[2])
        if data:
            print_table(data)


class ScanCommand(Command):
    def run(self, line):
        params = line.split()
        prefix = params[2] if len(params) >= 3 else ''
        limit = params[3] if len(params) >= 4 else 100
        data = scan(table=params[1], row_prefix=prefix, limit=limit)
        if data:
            print_table(data)


get_command = GetCommand("get", help="Get a row")
scan_command = ScanCommand("scan", help="Scan rows")

console = Console(prompt="", prompt_delim="hqlsh>")
console.addChild(get_command)
console.addChild(scan_command)
console.loop()
