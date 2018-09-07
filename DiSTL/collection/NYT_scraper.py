from datetime import datetime
import requests
import logging
import random
import json
import time
import os


# methods
def scraper(session, api_key, year, month, out_dir, log_file):

    url = "http://api.nytimes.com/svc/archive/v1/%d/%d.json" % (year, month)
    r = session.get(url, params={"api_key": api_key,
                                 "encoding": "gzip",
                                 "Cache-Control": "no-store",
                                 "Pragma": "no-cache",
                                 "accept-encoding": "gzip"})
    data = r.json()
    data = data["response"]
    data = data["docs"]
    logging.debug("%d,%d,%d" % (year, month, len(data)))
    with open(os.path.join(out_dir, "%d_%d.json" % (year, month)),
              "w") as fd:
        json.dump(data, fd)
    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%d_%d,%s,%s,%s\n" % (year, month, str(t1),
                                       str(t1 - t00), str(t1 - t0)))
    time.sleep(10)



def wrapper(session, api_key, year, month, out_dir, log_file):

    try:
        scraper(session, api_key, year, month, out_dir, log_file)
    except Exception as e:
        logging.debug("%d,%d,%s" % (year, month, e))
        time.sleep(60)
        try:
            scraper(session, api_key, year, month, out_dir, log_file)
        except Exception as e:
            logging.debug("%d,%d,%s" % (year, month, e))
            time.sleep(300)
            scraper(session, api_key, year, month, out_dir, log_file)




# params
out_dir = "/home/lbybee/Dropbox/BK_LB_Projects/Text_Data_API/raw/NYT"
log_file = "/home/lbybee/scratch_shared/NYT/scraper.log"
fail_file = "/home/lbybee/scratch_shared/NYT/scraper_fail.log"
api_key = "504b890bcf3146b6bc52f37d24d36ff7"
syear = 1851
eyear = 2018

dates = [True]
t0 = datetime.now()
backoff = 300

logging.basicConfig(level=logging.DEBUG)

while len(dates) > 0:

    session = requests.Session()

    base_dates = [(y, m) for m in range(1, 13) for y in range(syear, eyear)]
    if os.path.exists(log_file):
        with open(log_file, "r") as fd:
            fin_dates = [r.split(",")[0] for r in fd.read().split("\n")[:-1]]
    else:
        fin_dates = []
    dates = [d for d in base_dates if "%d_%d" % (d[0], d[1]) not in fin_dates]
    random.shuffle(dates)

    for d in dates:
        t00 = datetime.now()
        year, month = d

        try:
            wrapper(session, api_key, year, month, out_dir, log_file)
        except Exception as e:
            logging.debug("%d,%d,%s" % (year, month, e))
            t1 = datetime.now()
            with open(fail_file, "a") as fd:
                fd.write("%d_%d,%s,%s,%s\n" % (year, month, str(t1),
                                               str(t1 - t00), str(t1 - t0)))
            time.sleep(backoff)
