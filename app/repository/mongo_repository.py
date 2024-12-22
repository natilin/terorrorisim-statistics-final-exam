from datetime import datetime, timedelta
from typing import List

from app.db.mongo_database import events_collection
from returns.result import Result, Success, Failure


def get_most_death_by_attack_type(top_5: bool=False) -> Result[List, str]:
    pipeline = [
        {
            "$group": {
                "_id": "$attack_type",
                "total_deadly_rating": {"$sum": "$deadly_rating"}
            }
        },
        {"$sort": {"total_deadly_rating": -1}},
    ]
    if top_5:
        pipeline.append({"$limit": 5})
    try:
        return Success(list(events_collection.aggregate(pipeline)))

    except Exception as e:
        Failure(f"An error occurred: {str(e)}")


def get_average_death_by_region(top_5: bool=False) -> Result[List, str]:
    pipeline = [
        {"$match": {
            "location.region": {
                "$ne": float("nan")
            }
        }},
        {
            "$group": {
                "_id": "$location.region",
                "total_deadly_rating": {"$sum": "$deadly_rating"},
                "num_of_attack": {"$sum": 1},
                "latitudes": {"$push": "$location.latitude"},
                "longitudes": {"$push": "$location.longitude"}
            }
        },
        {
            "$project": {
                "region": "$_id",
                "average_deadly_rating": {"$divide": ["$total_deadly_rating", "$num_of_attack"]},
                "num_of_attack": 1,
                "latitude": {
                    "$avg": {
                        "$filter": {
                            "input": "$latitudes",
                            "as": "latitude",
                            "cond": {"$ne": ["$$latitude", float("nan")]}}}
                },
                "longitude": {
                    "$avg": {
                        "$filter": {
                            "input": "$longitudes",
                            "as": "longitude",
                            "cond": {"$ne": ["$$longitude", float("nan")]}}}
                }
            }
        },
        {"$sort": {"average_deadly_rating": -1}},
    ]
    if top_5:
        pipeline.append({"$limit": 5})

    try:
        return Success(list(events_collection.aggregate(pipeline)))

    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")



def get_top_5_attacking_group() -> Result[List, str]:
    pipeline = [
        {"$match": {
            "group_name": {
                "$ne": float("nan")
            }
        }},
        {"$group": {
            "_id": "$group_name",
            "num_of_attack": {"$sum": 1}
        }},
        {"$sort": {"num_of_attack": -1}},
        {"$limit": 5}
    ]

    try:
        return Success(list(events_collection.aggregate(pipeline)))
    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")



def get_terror_events_by_time_range(start_date: datetime, end_date: datetime) -> Result[List, str]:
    query = {
        "date": {
            "$gte": start_date,
            "$lte": end_date
        }
    }
    try:
        results = list(events_collection.find(query))
        return Success(results)
    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")


