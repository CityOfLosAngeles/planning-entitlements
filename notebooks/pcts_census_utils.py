# Utils to when merging data from PCTS and Census
import intake
import numpy as np
import os
import pandas as pd
import pcts_parser

bucket_name = "city-planning-entitlements"
catalog = intake.open_catalog("../catalogs/*.yml")

#---------------------------------------------------------------------------------------#
## Census functions
#---------------------------------------------------------------------------------------#
# Function to transform percent tables with aggregation option
def transform_census_percent(table_name, year, main_var, aggregate_me, aggregated_row_name, numer, denom):
    """
    Parameters 
    ==================
    table_name: str
    year: numeric
    main_var: str
            based on main_var column and pick only one for which the processed df is derived from
    aggregate_me: list
            a list of new_var groups to aggregate into 1 group
    aggregated_row_name: str
            will be new name for this aggregated group
    numer: str
            based on new_var column
    denom: str
            based on new_var column
    """
    df = grab_census_table(table_name, year, main_var)

    df2 = aggregate_group(df, aggregate_me, name = aggregated_row_name)
    
    cols = [aggregated_row_name, denom]
    df3 = make_wide(df2, cols)
    
    new_var = f"pct_{aggregated_row_name}"

    df3 = (df3.assign(
        new = df3[numer] / df3[denom],
        ).rename(columns = {'new': new_var})
    )
    
    return df3


income_ranges = ["lt10", "r10to14", "r15to19",
        "r20to24", "r25to29", 
        "r30to34", "r35to39",
        "r40to44", "r45to49", 
        "r50to59", "r60to74", "r75to99", 
        "r100to124", "r125to149", "r150to199",
        "gt200", "total"]


"""
Sub-functions
This most straightforward way to reshape from long to wide
Use number values, not percents, we can always derive percents later on if we need.
If we're aggregating to geographies that involve slicing parts of tracts, we need numbers, not percents.
"""
def grab_census_table(table_name, year, main_var):
    # Cache the df as an attribute on the function
    # so we only have to read it from s3 once.
    df = getattr(grab_census_table, "df", None)
    if df is None:
        df = catalog.census_cleaned.read()
        grab_census_table.df = df
    # Subset the df according to year and variable
    cols = ['GEOID', 'new_var', 'num']
    df = df[(df.year == year) & 
            (df.table == table_name) & 
            (df.main_var == main_var)][cols]
    return df


def make_wide(df, cols):
    return (
        df[df.new_var.isin(cols)]
        .assign(num=df.num.astype("Int64"))
        .pivot(index="GEOID", columns="new_var", values="num")
        .reset_index()
        .rename_axis(None, axis=1)
    )


def aggregate_group(df, aggregate_me, name="aggregated_group"):
    df = (df.assign(
        new_var2 = df.apply(lambda row: name if any(x in row.new_var for x in aggregate_me)
                             else row.new_var, axis = 1)
        ).groupby(['GEOID', 'new_var2'])
          .agg({'num':'sum'})
          .reset_index()
          .rename(columns = {'new_var2':'new_var'})
    )
    
    return df


# We'd like to calculate income percentiles from the reported ranges.
# The following function takes a row from the pivoted ACS data,
# and estimates a set of percentiles from the binned data:
def income_percentiles(row, percentiles, prefix="total"):
    # Edges of the reported income bins, in thousands of dollars
    bins = [0, 10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 75, 100, 125, 150, 200]
    # Iterator over percentiles
    p_it = iter(percentiles)
    # Final values for percentiles
    values = []
    # Initialize current percentile and an accumulator variable
    curr = next(p_it)
    acc = 0
    # The total count for the tract
    total = row[f"{prefix}_total"]
    if total <= 0:
        return values
    for i, b in enumerate(bins):
        # Compute the label of the current bin
        if i == 0:
            label = f"{prefix}_lt{bins[i+1]}"
        elif i == len(bins) - 1:
            label = f"{prefix}_gt{b}"
        else:
            label = f"{prefix}_r{b}to{bins[i+1]-1}"
        # Estimate the value for the current percentile
        # if it falls within this bin
        while (acc + row[label])/total > curr/100.0:
            frac = (total*curr/100.0 - acc)/row[label]
            lower = b
            upper = bins[i+1] if i < (len(bins) - 1) else 300. 
            interp = (1.0 - frac) * lower + frac * upper
            values.append(interp)
            try:
                curr = next(p_it)
            except StopIteration:
                return values
        # Increment the accumulator
        acc = acc + row[label]
    return values


#---------------------------------------------------------------------------------------#
## PCTS functions
#---------------------------------------------------------------------------------------#
FULL_PREFIX_LIST = ['AA','ADM',
    'APCC', 'APCE', 'APCH', 'APCNV', 'APCS', 'APCSV', 'APCW',
    'CHC', 'CPC', 'DIR', 'ENV', 'HPO', 'PAR', 'PS', 'TT', 'VTT', 'ZA'
    ]

# PM is a prefix that appears in the dummy variables as a prefix, but is not in the list of 
# prefixes or suffixes

FULL_SUFFIX_LIST = ['1A', '2A', 'AC', 'ACI', 'ADD1', 'ADU', 'AIC', 
        'BL', 'BSA', 'CA', 'CASP', 'CATEX', 'CC', 'CC1', 'CC3', 'CCMP', 'CDO', 
        'CDP', 'CE', 'CEX', 'CLQ', 'CM', 'CN', 'COA', 'COC', 'CPIO', 'CPIOA', 
        'CPIOC', 'CPIOE', 'CPU', 'CR', 'CRA', 'CU', 'CUB', 'CUC', 'CUE', 'CUW', 'CUX', 'CUZ', 
        'CWC', 'CWNC', 'DA', 'DB', 'DD', 'DEM', 'DI', 'DPS', 'DRB', 'EAF', 'EIR', 'ELD', 
        'EXT', 'EXT2', 'EXT3', 'EXT4', 'F', 'GB', 'GPA', 'GPAJ', 'HCA', 'HCM', 'HD', 'HPOZ', 
        'ICO', 'INT', 'M1', 'M2', 'M3', 'M6', 'M7', 'M8', 'M9', 'M10', 'M11', 'MA', 'MAEX',
        'MCUP', 'MEL', 'MND', 'MPA', 'MPC', 'MPR', 'MSC', 'MSP', 'NC', 'ND', 'NR',
        'O', 'OVR', 'P', 'PA', 'PA1', 'PA2', 'PA3', 'PA4', 'PA5', 'PA6', 'PA7', 'PA9', 'PA10',
        'PA15', 'PA16', 'PA17', 'PAB', 'PAD', 'PMEX', 'PMLA', 'PMW', 'POD', 
        'PP', 'PPR', 'PPSP', 'PSH', 'PUB', 'QC', 'RAO', 'RDP', 'RDPA',
        'REC1', 'REC2', 'REC3', 'REC4', 'REC5', 'REV', 'RFA', 'RV',
        'SCEA', 'SCPE', 'SE', 'SIP', 'SL', 'SLD', 'SM', 'SN', 'SP', 'SPE', 'SPP', 'SPPA', 'SPPM', 
        'SPR', 'SUD', 'SUP1', 'TC', 'TDR', 'TOC', 'UAIZ', 'UDU', 'VCU', 'VSO', 'VZC', 'VZCJ',
        'WDI', 'WTM', 'YV', 'ZAA', 'ZAD', 'ZAI', 'ZBA', 'ZC', 'ZCJ', 'ZV', 
        ]


# Subset PCTS and only get parent cases
def get_pcts_parents(start_date=None, end_date=None, prefix_list=None, suffix_list=None):
    """
    start_date: any form of date such as "1/1/2017", "2017-01-01", or even "2017-10" for a month-year.
    end_date: optional, with default set to today's date.
    prefix_list: list of prefixes to keep. If None, then default FULL_PREFIX_LIST is used. 
    suffix_list: list of suffixes. If None, then default FULL_SUFFIX_LIST is used.
    
    The sub-functions `subset_pcts` and `drop_child_cases` can be used separately.
    If PCTS cases (parent + child) were to be subsetted by a list of prefixes / suffixes,
    one could use just the `subset_pcts` function to return all cases that have those prefixes / suffixes.
    """

    pcts = subset_pcts(start_date, end_date, prefix_list, suffix_list)
    only_parents = drop_child_cases(pcts, prefix_list, suffix_list)
    
    only_parents = (only_parents.sort_values(["CASE_ID", "AIN"])
                .reset_index(drop=True)
                )
    return only_parents 


# Subset PCTS given a start date and a list of prefixes or suffixes
def subset_pcts(start_date=None, end_date=None, prefix_list=None, suffix_list=None):
    pcts = pd.read_parquet(f's3://{bucket_name}/data/final/master_pcts.parquet')

    # Subset PCTS by start / end date
    if start_date is None:
        start_date2 = pd.to_datetime("1/1/2010")
    else:
        start_date2 = pd.to_datetime(start_date)
    
    if end_date is None:
        end_date2 = pd.to_datetime("today")
    else: 
        end_date2 = pd.to_datetime(end_date)

    pcts = (pcts[(pcts.CASE_FILE_RCV_DT >= start_date2) & 
               (pcts.CASE_FILE_RCV_DT <= end_date2)]
            .drop_duplicates()
            .reset_index(drop=True)
           )
    
    # Parse CASE_NBR
    cols = pcts.CASE_NBR.str.extract(pcts_parser.GENERAL_PCTS_RE)

    all_prefixes = cols[0]
    all_suffixes = cols[3].str[1:].str.split("-")

    m1 = (pd.merge(pcts, all_prefixes, how = "left", 
                    left_index=True, right_index=True)
            .rename(columns = {0: "prefix"})
            )

    df = (pd.merge(m1, all_suffixes, how = "left", 
                left_index=True, right_index=True)
        .rename(columns = {3: "suffix"})
        )

    # Subset by prefix
    if prefix_list is None:
        prefix_list = FULL_PREFIX_LIST
    if suffix_list is None:
        suffix_list = FULL_SUFFIX_LIST

    df2 = df[(df.prefix.isin(prefix_list))]
    
    # Subset by suffix 
    df2 = df2.assign(
        has_suffix = df2.apply(lambda row: 1 if any(s in row.suffix for s in suffix_list) 
                               else 0, axis=1)
    )
    df3 = df2[df2.has_suffix == 1]    

    # Clean up
    df3 = (df3.drop(columns = ["prefix", "suffix", "has_suffix"])
           .sort_values(["CASE_ID", "AIN"])
           .reset_index(drop=True)
          )
    
    return df3
    

def drop_child_cases(df, prefix_list=None, suffix_list=None):
    """
    df: dataframe of the PCTS entitlements

    prefix_list: list of prefixes to keep. If None, then default FULL_PREFIX_LIST is used. 
    suffix_list: list of suffixes. If None, then default FULL_SUFFIX_LIST is used.

    prefix_list and suffix_list are passed from the main function `get_pcts_parents`.
    But, this sub-function would also work by itself.
    """
    parents = pd.read_parquet(f's3://{bucket_name}/data/final/parents_with_prefix_suffix.parquet')
    
    # Append two lists into one
    if prefix_list is None:
        prefix_list = FULL_PREFIX_LIST
    
    if suffix_list is None:
        suffix_list = FULL_SUFFIX_LIST

    prefix_and_suffix_list = prefix_list + suffix_list
    prefix_and_suffix_list.append("PARENT_CASE")

    parents = parents[prefix_and_suffix_list].drop_duplicates()

    # Merge and only keep parent cases
    df2 = pd.merge(df, parents, on = 'PARENT_CASE', how = 'inner', validate = 'm:1')   
    df2 = df2.drop_duplicates()

    return df2 