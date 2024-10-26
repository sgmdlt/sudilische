from itertools import product
from courts.generator.query_builder import QueryParams, get_url
from datetime import datetime, timedelta


def generate_urls(search_params: dict):
    courts = search_params.get("court_url")
    instance = search_params.get("instance")
    case_type = search_params.get("type")
    date_range = generate_monthly_dates(search_params.get("entry_date_from"), search_params.get("entry_date_to"))

    for params in product(courts, instance, case_type, date_range):
        p = {
            "court_url": params[0],
            "instance": params[1],
            "type": params[2],
            "entry_date_from": params[3][0],
            "entry_date_to": params[3][1],
        }
        yield get_url(QueryParams(**p))


def generate_monthly_dates(start_date, end_date):
    current_date = datetime.strptime(start_date, "%d.%m.%Y")
    end_date = datetime.strptime(end_date, "%d.%m.%Y")
    while current_date < end_date:
        next_month = current_date+ timedelta(days=32)
        yield current_date.strftime("01.%m.%Y"), next_month.strftime("01.%m.%Y")
        current_date = next_month.replace(day=1)
