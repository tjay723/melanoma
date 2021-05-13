#!/usr/bin/env python3

import argparse
import pandas as pd
import logging
import time

logging.basicConfig(level=logging.DEBUG)

start = time.time()

parser = argparse.ArgumentParser(description="Melanoma data tool")
parser.add_argument('-f', '--filter', help='which columns and values to filter by', required=True)
args = parser.parse_args()

# Load data
all_sites = pd.read_excel("data/all_melanoma_sites.xlsx")
patients = pd.read_excel("data/melanoma_sites.xlsx", index_col=0)
cols = patients.columns[~patients.columns.str.startswith('Unnamed:')]
patients = patients[cols]
followup = pd.read_excel("data/mia_follow_up_data.xlsx", index_col=0)
logging.info(f"Data loaded in {round(time.time() - start, 4)}s")

# Merge followup data with main patient data
patients = patients.join(followup, rsuffix='_followup')

k, v = args.filter.split("=")
original_length = len(patients)
try:
    patients = patients[patients[k] == v]
except KeyError:
    logging.error(f"Column {k} not known: possible columns to choose from: {sorted(patients.columns.tolist())}")
    exit(1)
logging.info(f"After applying filter {args.filter}, reduced dataset size from {original_length} to {len(patients)}")

# Merge all site information
start = time.time()
merged = patients.merge(all_sites, left_on=["Map", "X", "Y"], right_on=["Body map #", "X", "Y"])
logging.info(f"Data merged in {round(time.time() - start, 4)}s")