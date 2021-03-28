import re
import sqlite3
from datetime import datetime, timedelta

# --------------Couriers-------------- #
courier_pattern = ["courier_id",
                   "courier_type",
                   "regions",
                   "working_hours"]


def add_courier(info, cursor):
    cursor.execute("INSERT INTO couriers VALUES(?, ?, ?, ?);",
                   [
                       info['courier_id'],
                       info['courier_type'],
                       ' '.join(map(str, info['regions'])),
                       ' '.join(info['working_hours'])
                   ]
                   )
    for region in info['regions']:
        cursor.execute("INSERT INTO couriers_stats VALUES(?, ?, ?, ?, ?)",
                       [
                           info['courier_id'],
                           region,
                           0,
                           0,
                           None
                       ])
    cursor.execute('INSERT INTO courier_earnings VALUES(?, ?)',
                   [
                       info['courier_id'],
                       0
                   ])


def get_courier_info(courier_id, cursor):
    cursor.execute(
        'SELECT * FROM couriers WHERE courier_id == {}'.format(courier_id)
    )
    values = cursor.fetchone()
    return {
        'courier_id': values[0],
        'courier_type': values[1],
        'regions': list(map(int, values[2].split())),
        'working_hours': list(values[3].split())
    }


def reg_couriers(data):
    connection = sqlite3.connect('sweets.db')
    cursor = connection.cursor()

    valid = []
    invalid = []

    for obj in data:
        if (list(obj.keys()) == courier_pattern) and check_courier_data_validity(obj):
            add_courier(obj, cursor)
            valid.append({"id": obj['courier_id']})
        elif 'courier_id' in obj:
            invalid.append({"id": obj['courier_id']})

    connection.commit()
    connection.close()

    if invalid or not (invalid or valid):
        return False, invalid
    else:
        return True, valid


def edit_couriers(data, courier_id):
    connection = sqlite3.connect('sweets.db')
    cursor = connection.cursor()
    if not (all(i in courier_pattern for i in data) and
            (check_courier(courier_id, cursor)) and
            check_courier_data_validity(data)):
        return False, {}

    if "working_hours" in data:
        data["working_hours"] = ' '.join(data["working_hours"])

    if 'regions' in data:
        cursor.execute("SELECT region FROM couriers_stats WHERE courier_id == {}".format(courier_id))
        old_regions = [i[0] for i in cursor.fetchall()]
        new_regions = (set(data['regions']) - set(old_regions))
        for region in new_regions:
            cursor.execute("INSERT INTO couriers_stats VALUES(?, ?, ?, ?, ?)",
                           [
                               courier_id,
                               region,
                               0,
                               0,
                               None
                           ])
        connection.commit()
        data['regions'] = ' '.join(list(map(str, data['regions'])))

    for value in data:
        cursor.execute(
            'UPDATE couriers SET "{}" = "{}" WHERE courier_id = {}'.format(
                value, data[value], courier_id)

        )
    connection.commit()
    out = get_courier_info(courier_id, cursor)
    check_courier_ability(courier_id, cursor)
    connection.commit()
    connection.close()
    return True, out


def check_courier_ability(courier_id, cursor):
    cursor.execute("SELECT regions,"
                   " courier_type,"
                   " working_hours FROM couriers WHERE courier_id == {}".format(courier_id))
    info = cursor.fetchone()
    regions = '({})'.format(info[0].replace(' ', ','))
    weight = {'foot': 10, 'bike': 15, 'car': 50}[info[1]]
    working_hours = info[2].split()
    time_condition = make_checking_time_condition(working_hours)

    cursor.execute("SELECT order_id FROM orders WHERE courier_id == {} and completed == 0 and ( "
                   "(weight > {}) or "
                   "(not region in {} ) or "
                   "(not order_id in {}) )".format(courier_id,
                                                   weight,
                                                   regions,
                                                   time_condition))
    order_ids = '({})'.format(','.join([str(i[0]) for i in cursor.fetchall()]))
    cursor.execute("UPDATE orders SET"
                   " courier_id = NULL,"
                   " assign_time = NULL WHERE order_id in {}".format(order_ids))


def check_courier(courier_id, cursor):
    cursor.execute("SELECT courier_id FROM couriers WHERE courier_id = {}".format(courier_id))
    return bool(cursor.fetchone())


def get_courier_full_info(courier_id):
    connection = sqlite3.connect('sweets.db')
    cursor = connection.cursor()
    if not check_courier(courier_id, cursor):
        return False, {}
    out = get_courier_info(courier_id, cursor)
    cursor.execute("SELECT total_time, completed FROM couriers_stats "
                   "WHERE courier_id == {}".format(courier_id))
    av_times = []
    for total_time, number in cursor.fetchall():
        if total_time:
            av_times.append(total_time / number)
    print(av_times)
    if av_times:
        out['rating'] = round((60 - min(min(av_times), 60)) / 60 * 5, 2)
    cursor.execute(("SELECT sum FROM courier_earnings "
                    "WHERE courier_id == {}".format(courier_id)))
    out['earnings'] = cursor.fetchone()[0]
    return True, out


def check_courier_data_validity(data):
    validity = True
    if 'courier_id' in data:
        validity = (type(data['courier_id']) == int) and \
                   data['courier_id'] > 0

    if validity and ("courier_type" in data):
        validity = data["courier_type"] in ['foot', 'bike', 'car']

    if validity and ("regions" in data):
        validity = type(data['regions']) == list and \
                   all(((type(i) == int) and (i > 0)) for i in data['regions'])

    if validity and ("working_hours" in data):
        validity = type(data['working_hours']) == list and \
                   all(re.fullmatch(r'\d{2}:\d{2}-\d{2}:\d{2}', i) for i in data['working_hours']) and \
                   all((i.split('-')[0] < i.split('-')[1]) for i in data['working_hours']) and \
                   all(int(i[:-1]) < 24 for i in re.findall("\d{2}:", ' '.join(data["working_hours"]))) and \
                   all(int(i[1:]) < 60 for i in re.findall(":\d{2}", ' '.join(data["working_hours"])))

    return validity

# ---------------Orders--------------- #
orders_pattern = ["order_id",
                  "weight",
                  "region",
                  "delivery_hours"]


def add_order(info, cursor):
    cursor.execute(
        "INSERT INTO orders VALUES(?, ?, ?, ?, ?, ?, ?);",
        [
            info['order_id'],
            info['weight'],
            info['region'],
            None,
            None,
            None,
            0
        ]
    )
    for hours in info['delivery_hours']:
        cursor.execute(
            "INSERT INTO orders_delivery_hours VALUES(?, ?, ?);",
            [
                info['order_id'],
                int(hours.split('-')[0].replace(':', '')),
                int(hours.split('-')[1].replace(':', ''))
            ]
        )


def reg_orders(data):
    connection = sqlite3.connect('sweets.db')
    cursor = connection.cursor()

    valid = []
    invalid = []

    for obj in data:
        if (list(obj.keys()) == orders_pattern) and (check_order_data_validity(data)):
            add_order(obj, cursor)
            valid.append({"id": obj['order_id']})
        elif 'order_id' in obj:
            invalid.append({"id": obj['order_id']})

    connection.commit()
    connection.close()

    if invalid or not(invalid or valid):
        return False, invalid
    else:
        return True, valid


def check_order(order_id, cursor):
    cursor.execute("SELECT order_id FROM orders WHERE order_id = {}".format(order_id))
    return bool(cursor.fetchone())


def make_checking_time_condition(working_hours):
    time_condition = "(SELECT order_id FROM orders_delivery_hours WHERE( " + \
                     ("( start_hour >= {} and end_hour <= {} ) or " *
                      len(working_hours))[:-3] + ') )'

    hours = list(map(int, '-'.join(working_hours).replace(':', '').split('-')))

    time_condition = time_condition.format(*hours)
    return time_condition


def assign_orders(courier_id):
    connection = sqlite3.connect('sweets.db')
    cursor = connection.cursor()

    if not check_courier(courier_id, cursor):
        return False, {}

    info = get_courier_info(courier_id, cursor)
    weight = {'foot': 10, 'bike': 15, 'car': 50}[info['courier_type']]
    regions = "({})".format(','.join(list(map(str, info["regions"]))))
    time_condition = make_checking_time_condition(info["working_hours"])

    condition = ("SELECT order_id FROM orders WHERE "
                 "(weight < {}) "
                 "and (region in {}) "
                 "and (courier_id is NULL) "
                 "and (completed == 0) "
                 "and (order_id in {})"
                 ).format(weight,
                          regions,
                          time_condition)

    cursor.execute(condition)

    orders = [str(i[0]) for i in cursor.fetchall()]
    str_orders = '({})'.format(','.join(orders))

    if not orders:
        return True, {}

    cursor.execute("UPDATE orders SET courier_id = {},"
                   "assign_time = '{}',"
                   "courier_type = '{}' "
                   " WHERE order_id in {}".format(courier_id,
                                                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                  info["courier_type"],
                                                  str_orders))
    connection.commit()
    connection.close()
    return True, {
        "orders": [{"id": i} for i in orders],
        "assign_time": str(datetime.utcnow().isoformat("T")[:-4] + "Z")
    }


def complete_order(data):
    order_id = data["order_id"]
    courier_id = data["courier_id"]
    complete_time = datetime.strptime(data["complete_time"][:-1] + '0000', '%Y-%m-%dT%H:%M:%S.%f')

    connection = sqlite3.connect('sweets.db')
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM orders WHERE order_id == {} and courier_id == {}".format(order_id,
                                                                                           courier_id))
    order_info = cursor.fetchone()
    if not order_info or order_info[-1]:
        return False, {}
    earning = {'foot': 2, 'bike': 5, 'car': 9}[order_info[-2]] * 500
    cursor.execute("SELECT last_time FROM couriers_stats WHERE "
                   "courier_id = {} and "
                   "region = {}".format(courier_id, order_info[2]))

    last_time = cursor.fetchone()[0] or order_info[4]
    last_time = datetime.strptime(last_time, '%Y-%m-%d %H:%M:%S')

    time = int(timedelta.total_seconds(complete_time - last_time) // 60)
    cursor.execute('UPDATE couriers_stats SET '
                   'total_time = total_time + {}, '
                   'last_time = "{}", '
                   'completed = completed + 1 '
                   'WHERE courier_id == {} and '
                   'region == {}'.format(time,
                                         complete_time.strftime('%Y-%m-%d %H:%M:%S'),
                                         courier_id,
                                         order_info[2]))
    cursor.execute('UPDATE courier_earnings SET sum = sum + {} WHERE courier_id == {}'.format(earning,
                                                                                              courier_id))

    cursor.execute('UPDATE orders SET completed = 1 WHERE order_id == {}'.format(order_id))

    connection.commit()
    connection.close()
    return True, {"order_id": order_id}


def check_order_data_validity(data):
    validity = True
    if 'order_id' in data:
        validity = (type(data['order_id']) == int) and \
                   data['order_id'] > 0

    if validity and ('weight' in data):
        validity = (type(data['weight']) == int) and \
                   0.01 <= data['weight'] <= 50

    if validity and ('region' in data):
        validity = (type(data['region']) == int) and \
                   (data['region'] > 0)

    if validity and ("delivery_hours" in data):
        validity = type(data["delivery_hours"]) == list and \
                   all(re.fullmatch(r'\d{2}:\d{2}-\d{2}:\d{2}', i) for i in data["delivery_hours"]) and \
                   all((i.split('-')[0] < i.split('-')[1]) for i in data["delivery_hours"]) and \
                   all(int(i[:-1]) < 24 for i in re.findall("\d{2}:", ' '.join(data["delivery_hours"]))) and \
                   all(int(i[1:]) < 60 for i in re.findall(":\d{2}", ' '.join(data["delivery_hours"])))

    return validity
