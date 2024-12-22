from flask import Blueprint, json, jsonify, request
from returns.result import Success

from app.mongo_service import get_terror_list_by_date
from app.repository.mongo_repository import get_most_death_by_attack_type, get_average_death_by_region

statistics_blueprint = Blueprint("statistics", __name__)


@statistics_blueprint.route("/statistics", methods=["GET"])
def get_most_death_by_attack():
    top_5 = request.args.get("top5", "false").lower() == "true"
    res = get_most_death_by_attack_type(top_5)
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 400


@statistics_blueprint.route("/average-death-by-region", methods=["GET"])
def get_average_death():
    top_5 = request.args.get("top5", "false").lower() == "true"
    res = get_average_death_by_region(top_5)
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 400

@statistics_blueprint.route("/top-5-attacking-group", methods=["GET"])
def get_top_5_attacking_group():
    res = get_most_death_by_attack_type()
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 400

@statistics_blueprint.route("/terror-by-date", methods=["POST"])
def get_terror_by_date():
    time_range = request.json["time_range"]
    start_date = request.json["start_date"]
    date_type = request.json["date_type"]
    if date_type not in ["year", "month", "day"]:
        return jsonify({"Error": "Invalid date type"}), 400

    res = get_terror_list_by_date(start_date, date_type, time_range)
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 400