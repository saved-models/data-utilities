#!/usr/bin/env python3

import csv
import argparse
import os
import datetime

def fixdate(d):
    d,m,y = map(int, d.split("/"))
    return datetime.date(y,m,d)

def fint(r, k):
    try:
        return int(r[k])
    except ValueError:
#        print(r)
        return 0

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--delimiter", default="\t", help="Delimiter for CSV/TSV output (default TSV)")
    parser.add_argument("input")
    parser.add_argument("outdir")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    with open(args.input, newline="") as fp:
        rows = [row for row in csv.DictReader(fp)]

    cages = {}
    for row in rows:
        cage = cages.setdefault(row["Cage.Number"], { "deployments": {} })
        window = (fixdate(row["Deployment.date"]), fixdate(row["Recovery.date"]))
        depl = cage["deployments"].setdefault(window, [])
        depl.append(fint(row, "TOTAL"))

    def rdep(deps):
        ndeps = [ (d1, r2)
                  for d1, r1 in deps for d2, r2 in deps
                  if r1 - d2 <= datetime.timedelta(days=1)
                     and r1 - d2 >= datetime.timedelta(days=0)]
        return ndeps
        if ndeps == deps:
            return deps
        return rdep(ndeps)

    fields = ["cage", "deployment", "recovery", "duration", "count"]
    for cage, details in cages.items():
        print("Processing cage", cage)
        deps = [ (d1, r2)
                 for d1, r1 in details["deployments"]
                 for d2, r2 in details["deployments"]
                 if r1 - d2 <= datetime.timedelta(days=1)
                    and r1 - d2 >= datetime.timedelta(days=0)]
        print("Found deployments:")
        for d, r in deps:
            print("\t", d, "to", r)
            duration = (r - d).total_seconds() / 3600
            key = [k for k in details["deployments"] if k[1] == r][0]
            data = details["deployments"][key]

            filename = f"cage_{cage}_{r.isoformat()}.csv"
            filepath = os.path.join(args.outdir, filename)
            with open(filepath, mode="w+", newline="") as fp:
                writer = csv.DictWriter(fp, delimiter=args.delimiter, fieldnames=fields)
                writer.writerow({k:k for k in fields})
                for count in data:
                    writer.writerow({ "cage": cage,
                                      "deployment": d.isoformat(),
                                      "recovery": r.isoformat(),
                                      "duration": duration,
                                      "count": count })
