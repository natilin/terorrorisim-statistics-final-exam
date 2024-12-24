from typing import List

from app.repository.elastic_repository import get_terror_events_id_by_keyword, get_terror_events_id_by_keyword_and_date
from app.repository.mongo_repository import get_terror_event_by_uuid


def get_terror_events_by_keyword(keyword) -> List:
    id_list = get_terror_events_id_by_keyword(keyword)
    if not id_list:
        return []
    res =  [get_terror_event_by_uuid(uuid) for uuid in id_list]
    return  list(filter(None, res))


def get_terror_events_by_keyword_and_date(keyword: str, start_date, end_date) -> List:
    id_list = get_terror_events_id_by_keyword_and_date(keyword, start_date, end_date)
    if not id_list:
        return []
    res = [get_terror_event_by_uuid(uuid) for uuid in id_list]
    return list(filter(None, res))

