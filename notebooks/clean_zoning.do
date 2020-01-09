*** Purpose: Clean up zoning string
*** Date: 1/8/2020

local folder "C:\Users\404031\Documents\GitHub\planning-entitlements\data\"

use "`folder'zoning.dta", clear

drop index
renvars, lower


*** Split the string into its different components
split zone_cmplt, gen(z) parse(-)


egen filled = rownonmiss(z1-z5), strok

	/* tab filled

		 filled |      Freq.     Percent        Cum.
	------------+-----------------------------------
			  1 |         39        2.03        2.03
			  2 |        638       33.18       35.21
			  3 |        907       47.17       82.37
			  4 |        323       16.80       99.17
			  5 |         16        0.83      100.00
	------------+-----------------------------------
		  Total |      1,923      100.00 */
		  
		  
tempfile temp
save `temp', replace

		  
*** Clean the groupings of zone strings separately
use `temp', clear


*** Zone Class
** There are 35 zone classes, define them in groups
loc abbrev "OS A1 A2"
loc fullname ""

. local n : word count `agrp'

. forvalues i = 1/`n' {
  2.    local a : word `i' of `agrp'
  3.    local b : word `i' of `bgrp'
  4.    di "`a' says `b'"
}



keep if filled == 1
















