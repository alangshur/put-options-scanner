# import json
# import requests

# headers = {
#   'Accept': 'application/json'
# }

# payload = { 
#   'sessionid': 'SESSION_ID',
#   'symbols': 'SPY',
#   'linebreak': True
# }

# r = requests.get('https://stream.tradier.com/v1/markets/events', stream=True, params=payload, headers=headers)
# for line in r.iter_lines():
#     if line:
#         print(json.loads(line))


import requests

response = requests.post('https://sandbox.tradier.com/v1/markets/events/session',
    data={},
    headers={'Authorization': 'Bearer IU8G1tns4ii3PxjIAIC7s2pwsgXk', 'Accept': 'application/json'}
)
# json_response = response.json()
print(response)
# print(json_response)