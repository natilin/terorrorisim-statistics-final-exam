from datetime import datetime
from typing import List

from app.db.mongo_database import events_collection
from returns.result import Result, Success, Failure

def get_terror_event_by_uuid(uuid: str) -> dict:
    try:
        res = events_collection.find_one({"uuid": uuid})
        return res
    except:
        return None




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
        },
        "$and": [
            {"location.latitude": {"$ne": float("nan")}},
            {"location.longitude": {"$ne": float("nan")}}
        ]
    }

    projection = {
        "location.latitude": 1,
        "location.longitude": 1
    }

    try:
        results = list(events_collection.find(query, projection))
        return Success(results)
    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")


def get_all_terror_events_with_location() -> Result[List, str]:
    query = {
        "$and":[
            {"location.latitude": {"$ne": float("nan")}},
            {"location.longitude": {"$ne": float("nan")}}
        ]
    }

    projection = {
        "location.latitude": 1,
        "location.longitude": 1
    }
    try:
        results = list(events_collection.find(query, projection))
        return Success(results)
    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")


def get_groups_with_common_targets_type(country: str = None) -> Result[List, str]:
    pipline = [
        {
        "$match":
            {"target_type": {"$ne": float("nan")},
            "location.latitude": {"$nin": [None, "nan"]},
            "location.longitude": {"$nin": [None, "nan"]}}

        },
        {
            "$group": {
                "_id": {
                    "country": "$location.country",  # קיבוץ לפי מדינה
                    "target_type": "$target_type"  # קיבוץ לפי סוג המטרה
                },
                "groups_name": {"$addToSet": "$group_name"},
                 "latitude": {"$addToSet": "$location.latitude"},
                "longitude":  {"$addToSet": "$location.longitude"},
                "count": {"$sum": 1}  # מספר הפגיעות במטרה
            }
        },
        {
            "$sort": {"count": -1}  # מיין את המטרות לפי מספר הפגיעות (הכי הרבה קודם)
        },
        {
            "$group": {
                "_id": "$_id.country",  # קיבוץ לפי מדינה בלבד
                "target_type": {"$first": "$_id.target_type"},  # מטרה עם הכי הרבה פגיעות
                "groups_name": {"$first": "$groups_name"},  # קבוצות שפגעו במטרה
                "latitude": {"$first": "$latitude"},
                "longitude": {"$first": "$longitude"}# קואורדינטות
            }
        },
        {
            "$project": {
                "country": "$_id",  # הצגת שם המדינה
                "target_type": 1,  # הצגת המטרה
                "groups_name": 1,  # הצגת רשימת הקבוצות
                "latitude": 1 ,
                "longitude": 1
            }
        }
    ]

    if country:
        pipline[0]["$match"]["location.country"] = country  # אם המדינה נבחרה, הוסף מסנן לפי מדינה

    try:
        result = list(events_collection.aggregate(pipline))

        # עיבוד התוצאה להחזיר את המבנה הנדרש
        final_result = []
        for doc in result:
            final_result.append({
                "country": doc["country"],
                "target_type": doc["target_type"],
                "groups_name": doc["groups_name"],
                "latitude": doc["latitude"][0],
                "longitude": doc["longitude"][0]
            })

        return Success(final_result)

    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")




def get_groups_with_common_targets_type2(country: str = None) -> Result[List, str]:
    pipeline = [
        {
            "$match": {
                "target_type": {"$ne": float("nan")},
                "location.latitude": {"$nin": [None, "nan"]},
                "location.longitude": {"$nin": [None, "nan"]}
            }
        },
        {
            "$group": {
                "_id": {
                    "country": "$location.country",  # קיבוץ לפי מדינה
                    "target_type": "$target_type"  # קיבוץ לפי סוג מטרה
                },
                "groups": {"$addToSet": "$group_name"},  # רשימת קבוצות שתוקפות
                "latitudes": {"$addToSet": "$location.latitude"},  # רשימת קווי רוחב
                "longitudes": {"$addToSet": "$location.longitude"},  # רשימת קווי אורך
                "attack_count": {"$sum": 1}  # סך התקיפות
            }
        },
        {
            "$sort": {"attack_count": -1}  # סידור לפי מספר התקיפות בסדר יורד
        },
        {
            "$group": {
                "_id": "$_id.country",  # קיבוץ לפי מדינה בלבד
                "target_type": {"$first": "$_id.target_type"},  # סוג המטרה עם המספר הגבוה ביותר של תקיפות
                "groups": {"$first": "$groups"},  # רשימת קבוצות שתוקפות את המטרה
                "latitudes": {"$first": "$latitudes"},  # רשימת קווי רוחב
                "longitudes": {"$first": "$longitudes"},  # רשימת קווי אורך
                "attack_count": {"$first": "$attack_count"}  # המספר הגבוה ביותר של תקיפות
            }
        },
        {
            "$project": {
                "country": "$_id",  # שם המדינה
                "target_type": 1,  # סוג המטרה
                "groups": 1,  # רשימת קבוצות שתוקפות
                "most_attacked_group": {"$arrayElemAt": ["$groups", 0]},  # הקבוצה הראשונה ברשימה (כדוגמה)
                "latitudes": 1,  # רשימת קווי רוחב
                "longitudes": 1,  # רשימת קווי אורך
            }
        }
    ]

    if country:
        pipeline[0]["$match"]["location.country"] = country  # סינון לפי מדינה אם נבחרה


    try:
        result = list(events_collection.aggregate(pipeline))

        # עיבוד מבנה התוצאה להחזרת המבנה הרצוי
        final_result = []
        for doc in result:
            final_result.append({
                "country": doc["country"],
                "target_type": doc["target_type"],
                "most_attacked_group": max(doc["groups"], key=len),  # מציאת הקבוצה עם הכי הרבה תוקפים
                "groups": doc["groups"],  # רשימת הקבוצות שתוקפות
                "latitude": doc["latitudes"][0] if doc["latitudes"] else None,  # קו רוחב ראשון
                "longitude": doc["longitudes"][0] if doc["longitudes"] else None  # קו אורך ראשון
            })

        return Success(final_result)
    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")






def get_most_active_groups_per_country(country: str = None) -> Result[List, str]:
    pipeline = [
        {
            "$match": {
                "group_name": {"$ne": float("nan")},
                "location.latitude": {"$nin": [None, "nan"]},
                "location.longitude": {"$nin": [None, "nan"]}
            }
        },
        {
            "$group": {
                "_id": {
                    "country": "$location.country",
                    "group": "$group_name"
                },
                "sum": {"$sum": 1},
                "latitude": {"$first": "$location.latitude"},
                "longitude": {"$first": "$location.longitude"}
            }
        },
        {
            "$sort": {
                "_id.country": 1,
                "sum": -1
            }
        },
        {
            "$group": {
                "_id": "$_id.country",
                "top_groups": {
                    "$push": {
                        "group": "$_id.group",
                        "sum": "$sum",
                        "latitude": "$latitude",
                        "longitude": "$longitude"
                    }
                }
            }
        },
        {
            "$project": {
                "top_groups": {"$slice": ["$top_groups", 5]}
            }
        }
    ]
    if country is not None:
        pipeline.insert(1, {"$match": {"location.country": country}})
    try:
        return Success(list(events_collection.aggregate(pipeline)))
    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")


def get_most_common_attack_type_per_country(country: str = None) -> Result[List, str]:
    pipeline = [

        {
            "$match": {
                "location.country": country if country else {"$exists": True}
            }
        },
        {
            "$group": {
                "_id": {
                    "country": "$location.country",
                    "attack_type": "$attack_type"
                },
                "groups": {
                    "$addToSet": "$group_name"
                }
            }
        },

        {
            "$project": {
                "country": "$_id.country",
                "attack_type": "$_id.attack_type",
                "groups_count": {"$size": "$groups"},
                "groups": 1,
                "_id": 0
            }
        },

        {
            "$sort": {
                "country": 1,
                "groups_count": -1
            }
        },

        {
            "$group": {
                "_id": "$country",
                "most_common_attack": {
                    "$first": {
                        "attack_type": "$attack_type",
                        "groups_count": "$groups_count"
                    }
                }
            }
        },
        {
            "$project": {
                "country": "$_id",
                "attack_type": "$most_common_attack.attack_type",
                "groups_count": "$most_common_attack.groups_count",
                "_id": 0
            }
        }
    ]

    try:

        result = list(events_collection.aggregate(pipeline))
        return Success(result)
    except Exception as e:
        return Failure(f"An error occurred: {str(e)}")


