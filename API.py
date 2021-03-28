from flask import Flask, request, make_response
import DB

app = Flask(__name__)


@app.route('/couriers', methods=['POST'])
def reg_couriers():
    data = request.json['data']
    validity, out = DB.reg_couriers(data)
    if validity:
        return make_response({"couriers": out},
                             'HTTP 201 Created')
    else:
        return make_response({"validation_error": {"couriers": out}},
                             'HTTP 400 Bad Request')


@app.route('/couriers/<int:courier_id>', methods=['PATCH'])
def edit_couriers(courier_id):
    data = request.json
    validity, out = DB.edit_couriers(data, courier_id)
    if validity:
        return make_response(out,
                             'HTTP 200 OK')
    else:
        return make_response('',
                             '400 Bad Request')


@app.route('/orders', methods=['POST'])
def reg_orders():
    data = request.json['data']
    validity, out = DB.reg_orders(data)
    if validity:
        return make_response({"orders": out},
                             'HTTP 201 Created')
    else:
        return make_response({"validation_error": {"orders": out}},
                             'HTTP 400 Bad Request')


@app.route('/orders/assign', methods=['POST'])
def assign_orders():
    courier_id = request.json["courier_id"]
    validity, out = DB.assign_orders(courier_id)
    if validity:
        return make_response(out,
                             'HTTP 200 OK')
    else:
        return make_response('',
                             'HTTP 400 Bad Request')


@app.route('/orders/complete', methods=['POST'])
def complete_order():
    validity, out = DB.complete_order(request.json)
    if validity:
        return make_response(out,
                             'HTTP 200 OK')
    else:
        return make_response('',
                             'HTTP 400 Bad Request')


@app.route('/couriers/<int:courier_id>', methods=['GET'])
def fet_courier_full_info(courier_id):
    validity, out = DB.get_courier_full_info(courier_id)
    if validity:
        return make_response(out,
                             'HTTP 200 OK')
    else:
        return make_response('',
                             'HTTP 400 Bad Request')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
