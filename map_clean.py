import re
import pandas as pd
import json
from geopy.geocoders import Nominatim


with open("JSON\map.json", "r") as f:
    d = json.loads(f.read())

out = []
for item in d:
    if type(item) == list:
        for i in item:
            out.append(i)
    if type(item) == dict:
        out.append(i)


df = pd.io.json.json_normalize(out)

#%%

col_new = {
    "properties.id": "id",
    "properties.@timestamp": "timestamp",
    "properties._createDate": "create_date",
    "properties.form.searchDateNew": "search_date",
    "properties.update_date": "update_date",
    "properties.form.fact_date_name": "fact_date",
    "properties.send_date": "send_date",
    "properties.location_point.lon": "lon",
    "properties.location_point.lat": "lat",
    "properties.address": "address",
    "properties.form.org_structure_name": "org_structure",
    "properties.form.persons": "persons",
    "properties.form.qualification_type_name": "qualification_type",
    "properties.form.crime_damage_type_name": "damage_type",
    "properties.form.vehicles": "vehicles",
    "properties.children.autoTransportGrid": "transport",
    "properties.form.weapon_type_name": "weapon",
}

df = df[["source", "lab"] + list(col_new.keys())].rename(columns=col_new)


#%%

out = []
for a, b in df[["id", "persons"]].iterrows():
    if type(b["persons"]) == str:
        ps = b["persons"].split(",")
        for p in ps:
            d = {}
            d["id"] = b["id"]
            p = p.strip()

            try:
                d["age"] = re.findall("\d+", p)[-1]
            except IndexError:
                pass

            spl = p.split(":")
            if len(spl) > 2:
                print("Warning, something wrong", p)
            try:
                d["name"] = spl[1][1:5]
                d["cat"] = spl[0]
            except IndexError:
                continue

            out.append(d)

perps = pd.DataFrame(out)
perps.to_csv("OUTPUT\people.csv")
#%%

colours = [
    "რუხი",
    "თეთრი",
    "შავი",
    "ლურჯი",
    "წითელი",
    "მწვანე",
    "ყვითელი",
    "ყავისფერი",
    "ნარინჯისფერი",
    "იისფერი",
]

out = []
for a, b in df[["id", "vehicles"]].iterrows():
    if type(b["vehicles"]) == str:
        spl = b["vehicles"].split(",")
        for s in spl:
            if len(s.strip()) > 0:
                d = {}
                d["id"] = b["id"]
                col = s.split()[0].strip()
                if col in colours:
                    d["colour"] = col
                    d["vehicle"] = re.sub(
                        "\d/\d", "", s.replace(col, "")
                    ).strip()
                else:
                    d["vehicle"] = re.sub("\d/\d", "", s)

                try:
                    d["nums"] = re.findall("\d/\d", s)[0]
                except:
                    pass
                out.append(d)

cars = pd.DataFrame(out)
cars.to_csv("OUTPUT\cars.csv")
#%%
cars2 = (
    df["transport"]
    .dropna()
    .apply(lambda x: pd.Series(x[0]))
    .merge(df["id"], left_index=True, right_index=True,)
)
cars2.to_csv("OUTPUT\cars2.csv")


#%%

df.to_csv(r"OUTPUT\full.csv")
#%%
