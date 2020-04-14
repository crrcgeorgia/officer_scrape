import requests
import json
import re
import pandas as pd
from datetime import datetime, date

#%%
with open("DATA\POLICE\post_query.json", "r") as f:
    data = f.read()

with open("DATA\POLICE\headers_post.json", "r") as f:
    head_post = json.loads(f.read())["Request Headers (456 B)"]["headers"]

with open("DATA\POLICE\headers_get.json", "r") as f:
    head_get = json.loads(f.read())["Request Headers (456 B)"]["headers"]

h_post = {}
for h in head_post:
    v = list(h.values())
    h_post.update({v[0]: v[1]})

h_get = {}
for h in head_get:
    v = list(h.values())
    h_get.update({v[0]: v[1]})

with open("DATA\POLICE\cats.json", "r", encoding="utf-8") as f:
    CATS = json.loads(f.read())
    cats = [i["id"] for i in CATS["JSON"]]

#%%


def pull_query(dt, cat, data):

    data = re.sub('"(.+)"\n', f'"{cat}"\n', data)
    data = re.sub("\d{4}-\d{2}-\d{2}", f"{dt}", data)

    r = requests.post(
        "https://maps.officer.ge/api/search", data=data, headers=h_post
    )
    return r.json()["query"]


def pull_get(query, cat, dt):

    params = {
        "q": query,
        "layer[]": cat,
        "east": "49.28466796875",
        "west": "38.72680664062501",
        "south": "40.11588965267845",
        "north": "44.044167353572185",
        "zoom": "18",
        "precision": "12",
        "selectedId": "",
        "selectedType": "",
    }

    r = requests.get(
        "https://maps.officer.ge/api/aggregate", params=params, headers=h_get
    )

    good = []
    bad = []
    out = r.json()["userObjects"][0]["feature"]["features"]
    for i in r.json()["userObjects"][0]["feature"]["features"]:
        if i["properties"].get("id"):
            good.append(i)
        else:
            bad.append(i)

    if len(bad) > 0:
        print(f"Warning, some bad ones: {cat}, {dt}")
    if len(out) == 999:
        print(f"Warning, too many pulled: {cat}, {dt}")

    return r.json()["userObjects"][0]["feature"]["features"]


#%%

dates = pd.date_range(date(2019, 1, 1), datetime.now())

out = []
for d in dates:
    dt = d.strftime("%Y-%m-%d")
    for cat in cats:
        q = pull_query(dt, cat, data)
        x = pull_get(q, cat, dt)

        lab = [i["name"] for i in CATS["JSON"] if i["id"] == cat][0]
        for i in x:
            i["source"] = cat
            i["lab"] = lab

        for item in x:
            if type(item) == list:
                for i in item:
                    out.append(i)
            if type(item) == dict:
                out.append(i)

with open("DATA/POLICE/map.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(out))

#%%
