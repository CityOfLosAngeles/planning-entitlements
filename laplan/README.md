# README for laplan package

---

The `laplan` package is created for the Los Angeles Department of City Planning. 

There are sub-modules that allow users to clean up zoning data from ZIMAS, entitlement data from PCTS, and Census data from the American Community Survey. The classes and methods for the sub-modules are documented here.

There are 3 sub-modules, each of which can be used independently.
1. [Zoning](#zoning)
1. [PCTS](#pcts)
1. [Census](#census)

## Zoning
The sub-module is `zoning.py`. Zoning data comes from ZIMAS, and is publicly available on the [GeoHub](http://geohub.lacity.org/datasets/zoning). Planning's [Guide to Zoning String](https://planning.lacity.org/zoning/guide-current-zoning-string) shows that the zoning string is made up of component parts. 

The zoning string contains information about prefix on (Q)ualified or (T)entative zone classifications, zone class, the height district, (D)evelopment limits, and specific plans and overlays applicable. 

The `ZoningInfo` dataclass takes a zoning string and returns any or all of the components as a new dataframe. 

Ex 1: Return all the components
```
parsed_col_names = ['Q', 'T', 
                    'zone_class', 'specific_plan', 
                    'height_district', 'D', 'overlay']

# ZONE_CMPLT is the column to be parsed.
def parse_zoning(row):
    try:
        z = utils.ZoningInfo(row.ZONE_CMPLT)
        return pd.Series([z.Q, z.T, 
                            z.zone_class, z.specific_plan, 
                            z.height_district, z.D, z.overlay], 
                            index = parsed_col_names)
    # If it can't be parsed, return either a failed or blank string
    except ValueError:
        return pd.Series(['failed', 'failed',  
                            'failed', 'failed', 
                            'failed', 'failed', ''], 
                            index = parsed_col_names)

parsed = df.apply(parse_zoning, axis = 1)
df = pd.concat([df, parsed], axis = 1)
```

| ZONE_CMPLT | Q | T | zone_class | height_district | D | overlay |
| ---| --- | --- | --- | --- | --- | --- | --- |
| C2-1-SP| False | False | C2 | 1  | False | [SP]
| [Q]C1.5-1VLD-RIO | True | False | C1.5 | 1 | True |  [RIO] 


Ex 2: Return just one of the components

```
parsed_col_names = ['zone_class']

def parse_zoning(row):
    try:
        z = utils.ZoningInfo(row.ZONE_CMPLT)
        return pd.Series([z.zone_class], 
                         index = parsed_col_names)
    except ValueError:
        return pd.Series(['failed'], 
                         index = parsed_col_names)

    
parsed = df.apply(parse_zoning, axis = 1)
```

## PCTS
The sub-module is `pcts.py`. PCTS case strings contain prefixes and suffixes. Planning's [PCTS Prefix & Suffix Report](https://planning.lacity.org/resources/prefix-suffix-report) lists the valid values. 

The `PCTSCaseNumber` dataclass takes a string and returns any or all of the components as a new dataframe (note that `year` and `case` are available columns in PCTS, and parsing these may not be necessary). This dataclass is used infrequently.

The function `subset_pcts` can be used once a PCTS connection is made.  It standardizes the initial steps in the data cleaning pipeline so that the PCTS data is extracted and parent/child cases are combined in a standardized way before analysis. The function has optional args. `subset_pcts` and `drop_child_cases` should be used in conjunction with one another. The default is that the full dataset is returned. 
* **pcts**: pandas.DataFrame of PCTS data. 
* **start_date**: defaults to "1/1/2010". 
* **end_date**: defaults to present day.
* **prefix_list**: a list of prefixes of interest, defaults to all prefixes.
* **suffix_list**: a list of suffixes of interest, defaults to all suffixes. 
* **get_dummies**: bool, defaults to False. True returns columns for all the prefixes/suffixes of interest.
* **verbose**: bool, defaults to False. True returns some comments for prefixes/suffixes that have no cases.

Ex: Return PCTS entitlement cases between Oct 2017-Dec 2019 for the ADM and DIR prefixes and TOC suffixes.

```
import laplan

prefix_list = ['ADM', 'DIR']
suffix_list = ['TOC']

df = laplan.pcts.subset_pcts(
    pcts, 
    start_date = "10/1/17",
    end_date = "12/31/19", 
    prefix_list=prefix_list,
    suffix_list=suffix_list,
    get_dummies=True,
    verbose=True,
)
```

| CASE_NBR | CASE_FILE_RCV_DT | ADM | DIR | TOC |  
| ---| --- | --- | --- | --- | 
| DIR-2017-81-TOC-SPR | 2018-10-19 | False | True | True  |
| ADM-2017-4594-TOC | 2017-11-08 | True | False | True  |

The function `drop_child_cases` returns a dataframe of only parent cases. 
* **df**: pandas.DataFrame returned from `subset_pcts`. 
* **keep_child_entitlements**: bool, defaults to True. True means that the parent case should also hold all of the prefixes and suffixes from any child cases. `get_dummies` must be True in `subset_pcts`.  False means all the prefix/suffix dummies of the parent case show up, but child cases are dropped, and the prefixes/suffixes of the child cases are not stored. If a child case holds a different suffix not found in the parent case, `keep_child_entitlements = True` would store this information. 

```
df2 = laplan.pcts.drop_child_cases(
    df,   
    keep_child_entitlements=True
)
```

## Census
The sub-module is `census.py`. 