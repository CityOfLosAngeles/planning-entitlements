import dataclasses
import os
import re
import typing


#---------------------------------------------------------------------------------------#
## Parser
#---------------------------------------------------------------------------------------#
# A regex for parsing a zoning string
GENERAL_PCTS_RE = re.compile("([A-Z]+)-([0-9]+)-([0-9]+)((?:-[A-Z]+)*)$")


VALID_PCTS_PREFIX = {
    'AA','ADM', 'APCC', 'APCE', 'APCH', 
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
    year: float = np.nan
    pcts_case_id: str = ""
    suffix: typing.Optional[typing.List[str]] = None

    def __init__(self, pcts_case_string: str):
        try:
            self._general_pcts_parser(pcts_case_string)
        except ValueError:
            pass
        #try:
            #self.no_hyphen_parser(zoning_string)
        #except ValueError:
            #pass


    def _general_pcts_parser(self, pcts_case_string: str):
        """
        Create a new PCTSCaseNumber instance.
        
        Parameters
        ==========

        pcts_case_string: str
            The PCTS case number string to be parsed.
        """
        matches = GENERAL_PCTS_RE.match(pcts_case_string)
        if matches is None:
            raise ValueError("Couldn't parse PCTS string")
        
        groups = matches.groups()
        
        # Prefix
        if groups[0] in VALID_PCTS_PREFIX:
            prefix = groups[0]
            self.prefix = groups[0]
        
        # Year
        year = groups[1] or np.nan

        # Check for valid zone class values
        if zone_class not in VALID_ZONE_CLASS:
            self.invalid_zone = zone_class
            zone_class = "invalid"

        self.zone_class = zone_class

        # Height District
        height_district = groups[3] or ""
        
        # D Limit
        if height_district[-1] == "D":
            self.D = True
            height_district = height_district[:-1]
        else:
            self.D = False
            height_district = height_district
        
        # Check for valid height district values
        if height_district not in VALID_HEIGHT_DISTRICTS or height_district in INVALID_HEIGHT_DISTRICTS:
            self.invalid_height = height_district 
            height_district = "invalid"

        self.height_district = height_district

        # Overlays
        if groups[4]:
            self.overlay = groups[4].strip('-').split('-')