import json
from utils import generate_payload
from multiprocessing import Pool
import requests

HOSTS = ["http://192.168.5.36:11000", "http://192.168.5.37:11000"]
burst_size = 256
granularity = 128

init = json.loads(open('init.json').read())
run = json.loads(open('run.json').read())

init["code"] = open('terasort-burst.zip', 'rb').read().encode('base64')
run_params = generate_payload(endpoint="http://192.168.5.24:9000", 
                                partitions=256, 
                                bucket="terasort", 
                                key="terasort-6g", 
                                sort_column=0)

run0 = run.copy()
run0["value"] = run_params[0:granularity]
run1 = run.copy()
run1["value"] = run_params[granularity:burst_size]

payload_dict = {
    HOSTS[0]: run0,
    HOSTS[1]: run1
}

def send_request(host, payload):
    init_result = requests.post(host + "/init", json=init)
    run_result = requests.post(host + "/run", json=payload)
    return init_result, run_result


with Pool(2) as p:
    result = p.map(send_request, payload_dict.keys(), payload_dict.values())

print(result)




