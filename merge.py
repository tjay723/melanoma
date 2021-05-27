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
parser.add_argument('-f', '--filter', help='which columns and values to filter by')
parser.add_argument('-o', '--outfile', help='Output file', default="out.ipdata")
parser.add_argument('-of', '--output-format', help='Output format', choices=["ipdata", "exdata"], default="ipdata")
args = parser.parse_args()

# Set correct file extensions
if args.output_format == "exdata":
    args.outfile = Path(args.outfile).with_suffix(".exdata")
elif args.output_format == "ipdata":
    args.outfile = Path(args.outfile).with_suffix(".ipdata")

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

if args.filter:
    k, v = args.filter.split("=")
    original_length = len(patients)
    try:
        if k == "Node Fields":
            patients = patients[patients[k].str.contains(rf'\b{v}\b')]
        else:
            patients = patients[patients[k] == v]
    except KeyError:
        logging.error(f"Column {k} not known: possible columns to choose from: {sorted(patients.columns.tolist())}")
        exit(1)
    logging.info(f"After applying filter {args.filter}, reduced dataset size from {original_length} to {len(patients)}")

grouped_patients = patients.value_counts(["Map", "X", "Y"]).reset_index(name='count')

# Merge all site information
start = time.time()
merged = grouped_patients.merge(all_sites, left_on=["Map", "X", "Y"], right_on=["Body map #", "X", "Y"], how="right").fillna({'count': 0})
logging.info(f"Data merged in {round(time.time() - start, 4)}s")

merged.index = merged.index + 1
merged["scale"] = 1.0

cols = ["cmgui_x", "cmgui_y", "cmgui_z", "count"]

# Write out
with open(args.outfile, 'w') as file:
    if args.output_format == "ipdata":
        file.write('Data file\n')
        for col in cols:
            merged[col] = merged[col].apply(lambda x: '%.4f' % x)
        merged[["cmgui_x", "cmgui_y", "cmgui_z", "count", "scale", "scale", "scale", "scale"]].to_csv(file, header=False, sep='\t')
    elif args.output_format == "exdata":
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

logging.info(f"{args.outfile} written")
