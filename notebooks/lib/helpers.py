import json

def get_run_date() -> str:
    try:
        with open("00_run_date.json", "r") as f:
            return json.load(f)["rundate"]
    except Exception as e:
        return "19000101"