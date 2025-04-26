import json

import requests

from config import prometheusUrl


# func prometheusGetMetricsBySeries(serie string) (times []athletesTime, err error) {
# 	url := prometheusUrl + "series?match[]=" + serie
# 	resp, err := http.Get(url)
# 	if err != nil {
# 		return nil, err
# 	}
# 	defer resp.Body.Close()
#
# 	body, err := io.ReadAll(resp.Body)
# 	if err != nil {
# 		panic(err)
# 	}
#
# 	var response = Response{}
# 	err = json.Unmarshal(body, &response)
# 	if err != nil {
# 		panic(err)
# 	}
#
# 	return response.Data, nil
# }

def prometheus_get_metrics_by_series(serie):
    url = prometheusUrl + f"series?match[]={serie}"
    response = requests.get(url)
    body = response.text
    json_data = json.loads(body)
    return json_data