import json
from datetime import datetime

from bson import json_util
from flask import Blueprint, request, jsonify

from app.service.keyword_search import get_terror_events_by_keyword, get_terror_events_by_keyword_and_date


def parse_json(data):
    return json.loads(json_util.dumps(data))
keyword_blueprint = Blueprint("keyword_search", __name__)


@keyword_blueprint.route("/keyword", methods=["POST"])
def keyword_search():
    keyword = request.json.get("keyword")
    if keyword is None:
        return jsonify({"message": "No keyword provided"}), 400
    try:
        res = get_terror_events_by_keyword(keyword)
        return parse_json(res), 200
    except Exception as e:
        return str(e), 500


@keyword_blueprint.route("/historic", methods=["POST"])
def historic_keyword_search():
    keyword = request.json.get("keyword")
    if keyword is None:
        return jsonify({"message": "No keyword provided"}), 400
    try:
        res = get_terror_events_by_keyword(keyword)
        return parse_json(res), 200
    except Exception as e:
        return str(e), 500


@keyword_blueprint.route("/combined", methods=["POST"])
def keyword_search_by_date():
    keyword = request.json.get("keyword")
    start_date = request.json.get("start_date")
    end_date = request.json.get("end_date")
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        res = get_terror_events_by_keyword_and_date(keyword, start_date, end_date)
        return parse_json(res), 200
    except Exception as e:
        return str(e), 500
