from datetime import timedelta, date, datetime
import requests
import json
import time
import os


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)


def scraper(td0, t_url, d, res_dir):

    r = requests.get(t_url)
    data = r.json()
    print(d, len(data["posts"]))
    with open(os.path.join(res_dir, "%s.json" % td0), "w") as f:
        json.dump(data, f)

proj_dir = "/mnt/raw_data/Crimson_Hexagon"
res_dir = os.path.join(proj_dir, "raw/news")
log_dir = os.path.join(proj_dir, "logs")
files = os.listdir(res_dir)
sdate = datetime(2008, 5, 23)
edate = datetime(2018, 7, 1)
dates = [d for d in daterange(sdate, edate) if not "%s.json" % d.strftime("%Y-%m-%d") in files]




# get API key
r = requests.get("https://api.crimsonhexagon.com/api/authenticate?username=bryan.kelly@yale.edu&password=Bk010378&noExpiration=true")
res = r.json()
key = res["auth"]
base_url = "https://api.crimsonhexagon.com/api/monitor/posts?auth=%s&id=11814867450" % key
base_url += "&start=%s&end=%s&fullContents=true&extendLimit=true"

t0 = datetime.now()
for d in dates:
    t1 = datetime.now()
    td0 = d.strftime("%Y-%m-%d")
    td1 = (d + timedelta(days=1)).strftime("%Y-%m-%d")
    t_url = base_url % (td0, td1)

    try:
        scraper(td0, t_url, d, res_dir)
    except Exception as e:
        print(e)
        time.sleep(60)
        try:
            scraper(td0, t_url, d, res_dir)
        except Exception as e:
            print(e)
            time.sleep(360)
            scraper(td0, t_url, d, res_dir)
    time.sleep(1)
    t2 = datetime.now()
    with open(os.path.join(log_dir, "news.log"), "a") as f:
        f.write("%s,%s,%s\n" % (td0, str(t2 - t0), str(t2 - t1)))
