import json
import requests

from config import prometheus_url


def prometheus_get_metrics_by_series(serie: str):
    """
    Returns data about results from Prometheus
    :param serie: name of serie
    :return: [{'__name__':str, 'athlete_name':str, 'event':str, 'instance':str, 'job':str, 'value':str, 'year':str}]
    """
    url = prometheus_url + f"series?match[]={serie}"
    response = requests.get(url)
    body = response.text
    json_data = json.loads(body)
    return json_data
