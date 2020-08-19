"""
Utilities for dealing with PCTS cases.
"""
import dataclasses
import re
import typing

import pandas

GENERAL_PCTS_RE = re.compile("([A-Z]+)-([0-9X]{4})-([0-9]+)((?:-[A-Z0-9]+)*)$")
MISSING_YEAR_RE = re.compile("([A-Z]+)-([0-9]+)((?:-[A-Z0-9]+)*)$")

VALID_PCTS_PREFIX = {
    "AA",
    "ADM",
    "APCC",
    "APCE",
    "APCH",
    "APCNV",
    "APCS",
    "APCSV",
    "APCW",
    "CHC",
    "CPC",
    "DIR",
    "ENV",
    "HPO",
    "PAR",
    "PS",
    "TT",
    "VTT",
    "ZA",
}
VALID_PCTS_SUFFIX = {
    "1A",
    "2A",
    "AC",
    "ACI",
    "ADD1",
    "ADU",
    "AIC",
    "BL",
    "BSA",
    "CA",
    "CASP",
    "CATEX",
    "CC",
    "CC1",
    "CC3",
    "CCMP",
    "CDO",
    "CDP",
    "CE",
    "CEX",
    "CLQ",
    "CM",
    "CN",
    "COA",
    "COC",
    "CPIO",
    "CPIOA",
    "CPIOC",
    "CPIOE",
    "CPU",
    "CR",
    "CRA",
    "CU",
    "CUB",
    "CUC",
    "CUE",
    "CUW",
    "CUX",
    "CUZ",
    "CWC",
    "CWNC",
    "DA",
    "DB",
    "DD",
    "DEM",
    "DI",
    "DPS",
    "DRB",
    "EAF",
    "EIR",
    "ELD",
    "EXT",
    "EXT2",
    "EXT3",
    "EXT4",
    "F",
    "GB",
    "GPA",
    "GPAJ",
    "HCA",
    "HCM",
    "HD",
    "HPOZ",
    "ICO",
    "INT",
    "M1",
    "M2",
    "M3",
    "M6",
    "M7",
    "M8",
    "M9",
    "M10",
    "M11",
    "MA",
    "MAEX",
    "MCUP",
    "MEL",
    "MND",
    "MPA",
    "MPC",
    "MPR",
    "MSC",
    "MSP",
    "NC",
    "ND",
    "NR",
    "O",
    "OVR",
    "P",
    "PA",
    "PA1",
    "PA2",
    "PA3",
    "PA4",
    "PA5",
    "PA6",
    "PA7",
    "PA9",
    "PA10",
    "PA15",
    "PA16",
    "PA17",
    "PAB",
    "PAD",
    "PMEX",
    "PMLA",
    "PMW",
    "POD",
    "PP",
    "PPR",
    "PPSP",
    "PSH",
    "PUB",
    "QC",
    "RAO",
    "RDP",
    "RDPA",
    "REC1",
    "REC2",
    "REC3",
    "REC4",
    "REC5",
    "REV",
    "RFA",
    "RV",
    "SCEA",
    "SCPE",
    "SE",
    "SIP",
    "SL",
    "SLD",
    "SM",
    "SN",
    "SP",
    "SPE",
    "SPP",
    "SPPA",
    "SPPM",
    "SPR",
    "SUD",
    "SUP1",
    "TC",
    "TDR",
    "TOC",
    "UAIZ",
    "UDU",
    "VCU",
    "VSO",
    "VZC",
    "VZCJ",
    "WDI",
    "WTM",
    "YV",
    "ZAA",
    "ZAD",
    "ZAI",
    "ZBA",
    "ZC",
    "ZCJ",
    "ZV",
}


@dataclasses.dataclass
class PCTSCaseNumber:
    """
    A dataclass for parsing and storing PCTS Case Number info.
    The information is accessible as data attributes on the class instance.
    If the constructor is unable to parse the pcts_case_string,
    a ValueError will be raised.

    References
    ==========

    https://planning.lacity.org/resources/prefix-suffix-report
    """

    prefix: typing.Optional[str] = None
    year: typing.Optional[int] = None
    case: typing.Optional[int] = None
    suffix: typing.Optional[typing.List[str]] = None

    def __init__(self, pcts_case_string: str):
        try:
            self._general_pcts_parser(pcts_case_string)
        except ValueError:
            try:
                self._next_pcts_parser(pcts_case_string)
            except ValueError:
                pass

    def _general_pcts_parser(self, pcts_case_string: str):
        """
        Create a new PCTSCaseNumber instance.

        Parameters
        ==========

        pcts_case_string: str
            The PCTS case number string to be parsed.
        """
        matches = GENERAL_PCTS_RE.match(pcts_case_string.strip())
        if matches is None:
            raise ValueError("Couldn't parse PCTS string")

        groups = matches.groups()

        self.prefix = groups[0]
        self.year = int(groups[1])
        self.case = int(groups[2])

        # Suffix
        if groups[3]:
            self.suffix = groups[3].strip("-").split("-")

    def _next_pcts_parser(self, pcts_case_string: str):
        # Match where there is no year, but there is prefix, case ID, and suffix
        matches = MISSING_YEAR_RE.match(pcts_case_string.strip())

        if matches is None:
            raise ValueError(f"Coudln't parse PCTS string {pcts_case_string}")

        groups = matches.groups()

        # Prefix
        self.prefix = groups[0]
        self.year = int(groups)[1]

        # Suffix
        if groups[2]:
            self.suffix = groups[2].strip("-").split("-")


# Subset PCTS given a start date and a list of prefixes or suffixes
def subset_pcts(
    pcts,
    start_date=None,
    end_date=None,
    prefix_list=None,
    suffix_list=None,
    get_dummies=False,
    verbose=False,
):
    """
    Download an subset a PCTS extract for analysis. This is intended to
    be the primary entry point for loading PCTS data.

    Parameters
    ==========
    pcts: pandas.DataFrame
        A PCTS extract of the shape returned by subset_pcts.

    start_date: time-like
        Optional start date cutoff.

    end_date: time-like
        Optional end-date cutoff

    prefix_list: iterable of strings
        A list of prefixes to use. If not given, all prefixes are returned.

    suffix_list: iterable of strings
        A list of suffixes to use. If not given, all suffixes are used.

    get_dummies: bool
        Whether to get dummy indicator columns for all prefixes and suffixes.

    verbose: bool
        Whether to ouptut information about subsetting as it happens.
    """
    # Subset PCTS by start / end date
    start_date = (
        pandas.to_datetime(start_date)
        if start_date
        else pandas.to_datetime("2010-01-01")
    )
    end_date = pandas.to_datetime(end_date) if end_date else pandas.Timestamp.now()

    pcts = (
        pcts[
            (pcts.CASE_FILE_RCV_DT >= start_date) & (pcts.CASE_FILE_RCV_DT <= end_date)
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    if not suffix_list and not prefix_list and not get_dummies:
        return pcts.sort_values(["CASE_ID", "AIN"]).reset_index(drop=True)

    if verbose:
        print("Parsing PCTS case numbers")
    # Parse CASE_NBR
    cols = pcts.CASE_NBR.str.extract(GENERAL_PCTS_RE)

    all_prefixes = cols[0]
    all_suffixes = cols[3].str[1:].str.split("-", expand=True)

    allow_prefix = pandas.Series(True, index=pcts.index)
    allow_suffix = pandas.Series(True, index=pcts.index)
    # Subset by prefix
    if prefix_list is not None:
        allow_prefix = all_prefixes.isin(prefix_list)
    # Subset by suffix. Since the suffix may be in any of the all_suffixes
    # column, we logical-or them together, checking if each column has one
    # of the requested ones.
    if suffix_list is not None:
        exclude_suffixes = set(VALID_PCTS_SUFFIX) - set(suffix_list)
        allow_suffix = ~all_suffixes[0].isin(exclude_suffixes)
        for c in all_suffixes.columns[1:]:
            allow_suffix = allow_suffix & ~all_suffixes[c].isin(exclude_suffixes)

    subset = allow_prefix & allow_suffix
    pcts = pcts[subset]
    all_prefixes = all_prefixes[subset]
    all_suffixes = all_suffixes[subset]

    if get_dummies:
        if verbose:
            print("Getting dummy indicators for case types")
        # Get dummy columns for all prefixes
        prefix_dummies = pandas.get_dummies(all_prefixes, dtype="bool")
        # Identify if any of the requested prefixes are missing. If so,
        # populate them with a column of falses
        missing_prefixes = set(prefix_list or VALID_PCTS_PREFIX) - set(
            prefix_dummies.columns
        )
        if verbose and len(missing_prefixes):
            print("Prefixes with no associated cases: ", missing_prefixes)
        prefix_dummies = prefix_dummies.assign(**{p: False for p in missing_prefixes})

        # Get dummy columns for all suffixes
        suffix_dummies = pandas.get_dummies(all_suffixes.stack(), dtype="bool").max(
            level=0
        )
        # Identify if any of the requested suffixes are missing. If so,
        # populate them with a column of falses
        missing_suffixes = set(suffix_list or VALID_PCTS_SUFFIX) - set(
            suffix_dummies.columns
        )
        if verbose and len(missing_suffixes):
            print("Suffixes with no associated cases: ", missing_suffixes)
        suffix_dummies = suffix_dummies.assign(**{p: False for p in missing_suffixes})

        # Some suffixes appear in the prefix position due to (presumably) data entry
        # errors. If that is the case, drop the prefix dummy and combine the suffix.
        bad_prefixes = [p for p in prefix_dummies.columns if p in VALID_PCTS_SUFFIX]
        if verbose and len(bad_prefixes):
            print("Suffixes appearing in the prefix position: ", bad_prefixes)
        suffix_dummies = suffix_dummies.assign(
            **{p: prefix_dummies[p] | suffix_dummies[p] for p in bad_prefixes}
        )
        prefix_dummies = prefix_dummies.drop(columns=bad_prefixes)

        # Subset by the suffix and prefix lists if relevant
        suffix_dummies = suffix_dummies[suffix_list] if suffix_list else suffix_dummies
        prefix_dummies = prefix_dummies[prefix_list] if prefix_list else prefix_dummies

        # Make sure they are all nullable boolean type
        suffix_dummies = suffix_dummies.astype("boolean")
        prefix_dummies = prefix_dummies.astype("boolean")

        # Combine the dfs.
        pcts = pandas.concat((pcts, prefix_dummies, suffix_dummies), axis=1)

    # Clean up
    return pcts.sort_values(["CASE_ID", "AIN"]).reset_index(drop=True)


def drop_child_cases(pcts, keep_child_entitlements=True):
    """
    Drop all child cases from a PCTS extract (as indicated by them
    having a parent case listed).

    Parameters
    ==========

    pcts: pandas.DataFrame
        A PCTS extract of the shape returned by subset_pcts.

    keep_child_entitlements: boolean
        Whether to include entitlements in child cases among the dummy
        indicator variables of the parent cases. For this to work,
        the dummy indicators must be included (i.e., get_dummies must
        be True in subset_pcts).
    """
    if keep_child_entitlements:
        # Get a list of all the suffixes and prefixes in the pcts dataset
        prefixes = [c for c in pcts.columns if c in VALID_PCTS_PREFIX]
        suffixes = [c for c in pcts.columns if c in VALID_PCTS_SUFFIX]
        # Aggregate all the entitlements of the children with those of the parent case
        parent_entitlements = (
            pcts[["PARENT_CASE"] + suffixes + prefixes]
            .set_index("PARENT_CASE")
            .fillna(False)
            .pivot_table(index="PARENT_CASE", aggfunc="max")
        )
        # Merge those aggregated entitlements with the original pcts df
        pcts_agg = pandas.merge(
            pcts.drop(columns=suffixes + prefixes),
            parent_entitlements,
            how="left",
            left_on="PARENT_CASE",
            right_index=True,
        )
        # Finally, subset by all the parent cases, now that they
        # inlcude all of the entitlements of their children
        return pcts_agg[pcts_agg.PARNT_CASE_ID.isna()]
    else:
        return pcts[pcts.PARNT_CASE_ID.isna()]
