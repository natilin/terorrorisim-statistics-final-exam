from flask import Blueprint, json, jsonify, request
from returns.result import Success

from app.repository.neo4j_repository import get_groups_by_country, get_targets_by_group_and_year
from app.service.mongo_service import get_terror_list_by_date
from app.repository.mongo_repository import get_most_death_by_attack_type, get_average_death_by_region, \
    get_all_terror_events_with_location, get_groups_with_common_targets_type, get_most_active_groups_per_country, \
    get_most_common_attack_type_per_country
from app.utils import parse_json

statistics_blueprint = Blueprint("statistics", __name__)


@statistics_blueprint.route("/statistics", methods=["GET"])
def get_most_death_by_attack():
    top_5 = request.args.get("top5", "false").lower() == "true"
    res = get_most_death_by_attack_type(top_5)
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 500


@statistics_blueprint.route("/average-death-by-region", methods=["GET"])
def get_average_death():
    top_5 = request.args.get("top5", "false").lower() == "true"
    res = get_average_death_by_region(top_5)
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 500

@statistics_blueprint.route("/top-5-attacking-group", methods=["GET"])
def get_top_5_attacking_group():
    res = get_most_death_by_attack_type()
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 500


@statistics_blueprint.route("/terror-by-date", methods=["POST"])
def get_terror_by_date():
    time_range = request.json["time_range"]
    start_date = request.json["start_date"]
    date_type = request.json["date_type"]
    if date_type not in ["year", "month", "day"]:
        return jsonify({"Error": "Invalid date type"}), 400

    res = get_terror_list_by_date(start_date, date_type, time_range)
    if isinstance(res, Success):
         return parse_json(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 500


@statistics_blueprint.route("/all-terrors", methods=["GET"])
def get_all_terrors():
    res = get_all_terror_events_with_location()
    if isinstance(res, Success):
        return parse_json(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 500


@statistics_blueprint.route("/group-with-common-target", methods=["GET"])
def get_group_with_common_target():
    country = request.args.get("country")
    if country is None:
        res = get_groups_with_common_targets_type()
    else:
        res = get_groups_with_common_targets_type(country)
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 500


@statistics_blueprint.route("/most-active-group", methods=["GET"])
def get_most_active_group():
    country = request.args.get("country")
    if country is None:
        res = get_most_active_groups_per_country()
    else: res = get_most_active_groups_per_country(country)
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200

    return jsonify({"Error:": res.failure()}), 500


@statistics_blueprint.route("/group-attack-by-country", methods=["GET"])
def group_attack_by_country():
    res = get_groups_by_country()
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200
    return jsonify({"Error:": res.failure()}), 500


@statistics_blueprint.route("/targets-by-group-and-year", methods=["GET"])
def get_targets_by_group_year():
    country = request.args.get("country")
    if not country.isnumeric() :
        return jsonify({"Error country is not provided"}), 400
    res = get_targets_by_group_and_year(int(country))
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200
    return jsonify({"Error:": res.failure()}), 500


@statistics_blueprint.route("/most-active-targets", methods=["GET"])
def most_common_attack_type_per_country():
    country = request.args.get("country")
    res = get_most_common_attack_type_per_country(country)
    if isinstance(res, Success):
        return json.dumps(res.unwrap()), 200
    return jsonify({"Error:": res.failure()}), 500