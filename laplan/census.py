# Utils to when merging data from PCTS and Census
"""
Utilities for dealing with census data.
"""
# ---------------------------------------------------------------------------------------#
# Census functions
# ---------------------------------------------------------------------------------------#
# Function to transform percent tables with aggregation option


def transform_census_percent(
    table,
    table_name,
    year,
    main_var,
    aggregate_me,
    aggregated_row_name,
    numer,
    denom,
):
    """
    Parameters
    ==================
    table_name: str
    year: numeric
    main_var: str
            based on main_var column and pick only one for which the processed df is
            derived from
    aggregate_me: list
            a list of new_var groups to aggregate into 1 group
    aggregated_row_name: str
            will be new name for this aggregated group
    numer: str
            based on new_var column
    denom: str
            based on new_var column
    """
    df = subset_census_table(table, table_name, year, main_var)

    df2 = aggregate_group(df, aggregate_me, name=aggregated_row_name)

    cols = [aggregated_row_name, denom]
    df3 = make_wide(df2, cols)

    new_var = f"pct_{aggregated_row_name}"

    df3 = df3.assign(new=df3[numer] / df3[denom],).rename(columns={"new": new_var})

    return df3


CENSUS_INCOME_RANGES = [
    "lt10",
    "r10to14",
    "r15to19",
    "r20to24",
    "r25to29",
    "r30to34",
    "r35to39",
    "r40to44",
    "r45to49",
    "r50to59",
    "r60to74",
    "r75to99",
    "r100to124",
    "r125to149",
    "r150to199",
    "gt200",
    "total",
]


"""
Sub-functions
This most straightforward way to reshape from long to wide
Use number values, not percents, we can always derive percents later on if we need.
If we're aggregating to geographies that involve slicing parts of tracts,
we need numbers, not percents.
"""


def subset_census_table(df, table_name, year, main_var):
    """
    Given an ACS table, subset it by variable and year.

    TODO: document expected table shapes.
    """
    cols = ["GEOID", "new_var", "num"]
    df = df[(df.year == year) & (df.table == table_name) & (df.main_var == main_var)][
        cols
    ]
    return df


def make_wide(df, cols):
    """
    Pivot an ACS table.

    TODO: document expected table shapes.
    """
    return (
        df[df.new_var.isin(cols)]
        .assign(num=df.num.astype("Int64"))
        .pivot(index="GEOID", columns="new_var", values="num")
        .reset_index()
        .rename_axis(None, axis=1)
    )


def aggregate_group(df, aggregate_me, name="aggregated_group"):
    df = (
        df.assign(
            new_var2=df.apply(
                lambda row: name
                if any(x in row.new_var for x in aggregate_me)
                else row.new_var,
                axis=1,
            )
        )
        .groupby(["GEOID", "new_var2"])
        .agg({"num": "sum"})
        .reset_index()
        .rename(columns={"new_var2": "new_var"})
    )

    return df


def income_percentiles(row, percentiles, prefix="total"):
    """
    Estimate income percentiles from counts in the census income ranges.

    Parameters
    ==========
    row: pandas.Series
        A series that contains binned incomes in the ranges given by
        CENSUS_INCOME_RANGES.

    percentiles: List[float]
        A list of percentiles (from zero to 100) for which to estimate values.

    prefix: str
        A prefix for the income range columns (e.g., census-indicated race).
        Defaults to "total"

    Returns
    =======
    A list of estimated income percentiles of the same length as percentiles,
    in thousands of dollars.
    """

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
        while (acc + row[label]) / total > curr / 100.0:
            frac = (total * curr / 100.0 - acc) / row[label]
            lower = b
            upper = bins[i + 1] if i < (len(bins) - 1) else 300.0
            interp = (1.0 - frac) * lower + frac * upper
            values.append(interp)
            try:
                curr = next(p_it)
            except StopIteration:
                return values
        # Increment the accumulator
        acc = acc + row[label]
    return values
