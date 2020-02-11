import dataclasses
import os
import re
import typing


#---------------------------------------------------------------------------------------#
## Parser
#---------------------------------------------------------------------------------------#
# A regex for parsing a zoning string
GENERAL_PCTS_RE = re.compile("([A-Z]+)-([0-9X]{4})-([0-9]+)((?:-[A-Z0-9]+)*)$")
MISSING_YEAR_RE = re.compile("([A-Z]+)-([0-9]+)((?:-[A-Z0-9]+)*)$")


VALID_PCTS_PREFIX = {
    'AA', 'ADM', 'APCC', 'APCE', 'APCH', 
    'APCNV', 'APCS', 'APCSV', 'APCW',
    'CHC', 'CPC', 'DIR', 'ENV', 'HPO', 
    'PAR', 'PS', 'TT', 'VTT', 'ZA'
}

VALID_PCTS_ALL_SUFFIX = {
 
}




@dataclasses.dataclass
class PCTSCaseNumber:
    """
    A dataclass for parsing and storing PCTS Case Number info.
    The information is accessible as data attributes on the class instance.
    If the constructor is unable to parse the zoning string,
    a ValueError will be raised.

    References
    ==========

    https://planning.lacity.org/resources/prefix-suffix-report
    """
    prefix: str = ""
    year: str = ""
    pcts_case_id: str = ""
    suffix: typing.Optional[typing.List[str]] = None
    invalid_prefix: str = ""


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
        
        invalid_prefix = ""

        # Prefix
        if groups[0] in VALID_PCTS_PREFIX:
            prefix = groups[0]
        else:
            prefix = 'invalid'
            invalid_prefix = groups[0]
            
        self.prefix = prefix
        self.invalid_prefix = invalid_prefix
        
        # Year
        self.year = groups[1] or ""

        # Case ID
        self.pcts_case_id = groups[2] or ""

        # Suffix
        if groups[3]:
            self.suffix = groups[3].strip('-').split('-')
    


    def _next_pcts_parser(self, pcts_case_string: str):
        # Match where there is no year, but there is prefix, case ID, and suffix
        matches = MISSING_YEAR_RE.match(pcts_case_string.strip())
        
        if matches is None:
            raise ValueError(f"Coudln't parse PCTS string {pcts_case_string}")
        
        groups = matches.groups()
        
        invalid_prefix = ""

        # Prefix
        if groups[0] in VALID_PCTS_PREFIX:
            prefix = groups[0]
        else:
            prefix = 'invalid'
            invalid_prefix = groups[0]
        
        self.prefix = prefix
        self.invalid_prefix = invalid_prefix
    
        # Case ID
        self.pcts_case_id = groups[1] or ""

        # Suffix
        if groups[2]:
            self.suffix = groups[2].strip('-').split('-')
