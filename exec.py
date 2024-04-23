import json
from utils import generate_payload
from multiprocessing import Pool
import requests
import base64
import pprint
import pandas as pd

HOSTS = ["http://192.168.5.36:11000", "http://192.168.5.37:11000"]
burst_size = 192
granularity = 96

init = json.loads(open('init.json').read())
run = json.loads(open('run.json').read())

init["value"]["code"] = base64.b64encode(open("terasort-burst.zip", "rb").read()).decode("utf-8")
json.dump(init, open("foo.json", "w"))

run_params = generate_payload(endpoint="http://192.168.5.24:9000", 
                                partitions=192, 
                                bucket="terasort", 
                                key="terasort-6g", 
                                sort_column=0)

run0 = run.copy()
run0["value"] = run_params[0:granularity]
run0["invoker_id"] = "0"
run1 = run.copy()
run1["value"] = run_params[granularity:burst_size]
run1["invoker_id"] = "1"

payload_dict = {
    HOSTS[0]: run0,
    HOSTS[1]: run1
}

def send_request(host, payload):
    init_result = requests.post(host + "/init", json=init)
    run_result = requests.post(host + "/run", json=payload)
    return init_result, run_result


with Pool(2) as p:
    result = p.starmap(send_request, payload_dict.items())
    p.close()
    p.join()

results = []
for r in result:
    results.extend(json.loads(r[1].text))

stats = pd.DataFrame({"fn_id": i["part_number"],
                          # "host_submit": host_submit,
                          "init_fn": i["init_fn"],
                          "post_download": i["post_download"],
                          "pre_shuffle": i["pre_shuffle"],
                          "post_shuffle": i["post_shuffle"],
                          "pre_upload": i["pre_upload"],
                          "end_fn": i["end_fn"],
                          # "finished": finished
                          } for i in results)

stats.to_csv("terasort-burst.csv", index=False)



