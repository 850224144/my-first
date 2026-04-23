import os
import time
import requests
import pandas as pd

DATA_DIR = "../data"

def ensure_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def get_tencent_data(code):
    symbol = ("sh"+code) if code.startswith("6") else ("sz"+code)
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,,500,fq"
    try:
        res = requests.get(url, timeout=10).json()
        data = res["data"][symbol]["day"]
        df = pd.DataFrame(data, columns=["date","open","close","high","low","volume","_"])
        df = df[["date","open","close","high","low","volume"]]
        df = df.astype({"open":float,"close":float,"high":float,"low":float,"volume":float})
        df["date"] = pd.to_datetime(df["date"])
        return df
    except:
        return None

def load_local(code):
    path = f"{DATA_DIR}/{code}.csv"
    if os.path.exists(path):
        return pd.read_csv(path, parse_dates=["date"])
    return None

def save_local(code, df):
    path = f"{DATA_DIR}/{code}.csv"
    df.to_csv(path, index=False)

def update_data(code):
    ensure_dir()
    old = load_local(code)
    new = get_tencent_data(code)

    if new is None:
        return old

    if old is None:
        save_local(code, new)
        return new

    if new["date"].iloc[-1] > old["date"].iloc[-1]:
        save_local(code, new)
        return new
    else:
        return old