#!/usr/bin/env python3

import argparse
import pandas as pd
import numpy as np
import logging
import time
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)

start = time.time()

parser = argparse.ArgumentParser(description="Melanoma data tool")
parser.add_argument('-f', '--filter', help='Which columns and values to filter by')
parser.add_argument('-o', '--outfile', help='Output file', default="out.ipdata")
parser.add_argument('-n', '--normalise', help='Calculate normalised frequency as percentage (divide number of patients with drainage to specified node field by total number of patients at site)', action='store_true')
args = parser.parse_args()

# Load data
all_sites = pd.read_excel("data/all_melanoma_sites.xlsx")
# Replace Unicode code point U+2212 − with regular U+002D -
all_sites["cmgui_z"] = all_sites["cmgui_z"].astype(str).str.replace("−", "-").astype(float)
patients = pd.read_excel("data/melanoma_sites.xlsx", index_col=0)
cols = patients.columns[~patients.columns.str.startswith('Unnamed:')]
patients = patients[cols]
followup = pd.read_excel("data/mia_follow_up_data.xlsx", index_col=0)
logging.info(f"Data loaded in {round(time.time() - start, 4)}s")

# Merge followup data with main patient data
patients = patients.join(followup, rsuffix='_followup')
original_patients = patients.copy()

def calc():
    key = ["Map", "X", "Y"]
    if args.normalise:
        # Patients with melanoma at each site draining to selected node field
        numerator = patients.groupby(key).size().reset_index(name='selected_node')
        # All patients with melanoma at each site, any node field
        denominator = original_patients.groupby(key).size().reset_index(name='any_node')
        grouped_patients = numerator.merge(denominator, left_on=key, right_on=key, how="right").fillna({'selected_node': 0})
        grouped_patients["count"] = grouped_patients["selected_node"] / grouped_patients["any_node"] * 100 # percentage
    else:
        grouped_patients = patients.groupby(key).size().reset_index(name='count')

    # Merge all site information
    start = time.time()
    merged = grouped_patients.merge(all_sites, left_on=key, right_on=["Body map #", "X", "Y"], how="right").fillna({'count': 0})
    logging.info(f"Data merged in {round(time.time() - start, 4)}s")

    merged.index = merged.index + 1
    merged["scale"] = 1.0
    return merged

def save(merged, filename = args.outfile):
    cols = ["cmgui_x", "cmgui_y", "cmgui_z", "count"]
    # Write out
    if args.normalise:
        filename += "_normalised"
    filename = Path(filename).with_suffix(".ipdata")
    with open(filename, "w") as file:
        file.write('Data file\n')
        formatted = merged.copy()
        for col in cols:
            formatted[col] = formatted[col].apply(lambda x: '%.4f' % x)
        if args.normalise:
            formatted[["cmgui_x", "cmgui_y", "cmgui_z", "count", "scale", "scale", "scale", "selected_node"]].to_csv(file, header=False, sep='\t')
        else:
            formatted[["cmgui_x", "cmgui_y", "cmgui_z", "count", "scale", "scale", "scale", "scale"]].to_csv(file, header=False, sep='\t')
        logging.info(f"{filename} written")
    filename = Path(filename).with_suffix(".exdata")
    with open(filename, "w") as file:
        file.write(" Group name: data\n" +
                        " #Fields=2\n" +
                        " 1) coordinates, coordinate, rectangular cartesian, #Components=3\n" +
                        "   x.  Value index= 1, #Derivatives=0\n" +
                        "   y.  Value index= 2, #Derivatives=0\n" +
                        "   z.  Value index= 3, #Derivatives=0\n" +
                        " 2) count, field, rectangular cartesian, #Components=1\n" +
                        "   1.  Value index= 4, #Derivatives=0\n")
        for col in cols:
            merged[col] = merged[col].apply(lambda x: '%.5e' % x)
        for index, row in merged.iterrows():
            file.write(f" Node:  {index}\n    {row['cmgui_x']}  {row['cmgui_y']}  {row['cmgui_z']}\n    {row['count']}\n")
        logging.info(f"{filename} written")

if args.filter:
    filters = args.filter.split("&")
    for filter in filters:
        k, v = filter.split("=")
        original_length = len(patients)
        try:
            if k == "Node Fields":
                for nodeField in v.split(","):
                    patients = original_patients[original_patients[k].str.contains(rf'\b{nodeField}\b', na=False)]
                    logging.info(f"After applying filter Node Fields={nodeField}, reduced dataset size from {original_length} to {len(patients)}")
                    save(calc(), nodeField)
            else:
                patients = patients[patients[k] == v]
                logging.info(f"After applying filter {filter}, reduced dataset size from {original_length} to {len(patients)}")
                save(calc())
        except KeyError:
            logging.error(f"Column {k} not known: possible columns to choose from: {sorted(patients.columns.tolist())}")
            exit(1)
else:
    save(calc())