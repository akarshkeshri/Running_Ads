import flask
from flask import  jsonify
import request
import uuid
from flask import abort

app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.config['RESTFUL_JSON'] = {"ensure_ascii": False}

@app.route('/ping', methods=['GET'])
def ping():
    return "pong"
    
@app.route('/user/<user_id>', methods=['GET'])
def serve(user_id):
    query_params = request.args
    Parameter validation
    if "type" not in query_params:
        return abort(400)
# Generate the request identifier
    request_id = str(uuid.uuid1())
    return jsonify({
    "status": "success",
    "request_id": request_id
    })
@app.route('/user/<request_id>/feedback', methods=['POST'])
def create_task(request_id):
# Parameter validation
    if not request.json or 'status' not in request.json:
        return abort(400)
    return jsonify({
    "status": "success",
    "request_id": request_id
    })

#Start Flask application
app.run(host="localhost", port=5000)
