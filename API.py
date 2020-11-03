from flask import Flask, request
from api_logic import api_logic
import json

app = Flask(__name__)


def api_start():
    global api_logic_use
    api_logic_use = api_logic()


@app.route('/rating', methods=['POST'])
def rating_post():
    data = request.get_json()
    api_logic_use.add_rating(item=data)
    response = Flask.response_class(json.dumps(data), status=201)
    return response


@app.route('/ratings', methods=['GET'])
def rating_get():
    json_list = api_logic_use.get_ratings()
    response = Flask.response_class(json.dumps(json_list), status=200)
    return response


@app.route('/ratings', methods=['DELETE'])
def rating_delete():
    deleted_list = api_logic_use.delete_ratings()
    response = Flask.response_class(json.dumps(deleted_list), status=204)
    return response


@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def rating_get_all_ratings():
    all_ratings = api_logic_use.all_ratings()
    response = Flask.response_class(json.dumps(all_ratings), status=200)
    return response


@app.route('/avg-genre-ratings/<int:param>', methods=['GET'])
def rating_get_one_user_ratings(param):
    one_user_ratings = api_logic_use.get_one_user_average(param)
    response = Flask.response_class(json.dumps(one_user_ratings), status=200)
    return response


@app.route('/profile/<int:param>', methods=['GET'])
def rating_get_one_user_profile(param):
    one_user_ratings = api_logic_use.get_profile(param)
    response = Flask.response_class(json.dumps(one_user_ratings), status=200)
    return response


if __name__ == '__main__':
    api_start()
    app.run(host="localhost", port=9875)
