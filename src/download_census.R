# Purpose: Import income-related Census data
# Date Created: 2/26/2020
# Output: csvs in data/

library(tidycensus)
library(tidyverse)
library(censusapi)
Sys.getenv("CENSUS_API_KEY")
Sys.getenv("CENSUS_KEY")

# Set working directory
setwd("GitHub/planning-entitlements")

## Load years
tract_years <- list(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018)


#------------------------------------------------------------------#
## Median Income by Tract -- got all years
#------------------------------------------------------------------#. 
# Use C01 and C02 for 2010-2016; C01 and C03 for 2017-2018. 2017 added new column that shows % of hh (derived from C01).
print('Download median income (S1903) 2010-16')
income_list = list()

for (y in 2010:2016) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1903"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "002$") |
                         str_detect(variable, "003$") |
                         str_detect(variable, "004$") |
                         str_detect(variable, "005$") |
                         str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "008$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "010$")) &
                        str_detect(GEOID, "^06037") &
                        (str_detect(variable, "_C01") | str_detect(variable, "_C02"))
  )
  
  la$year <- y
  income_list[[y]] <- la
  
}


print('Download median income (S1903) 2017-2018')
for (y in 2017:2018) 
{
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S1903"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "002$") |
                         str_detect(variable, "003$") |
                         str_detect(variable, "004$") |
                         str_detect(variable, "005$") |
                         str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "008$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "010$")) &
                        str_detect(GEOID, "^06037") &
                        (str_detect(variable, "_C01") | str_detect(variable, "_C03"))
  )
  
  la$year <- y
  income_list[[y]] <- la
  
}


# Append dfs and export
print('Append median income dfs')
income = do.call(rbind, income_list)

write_csv(income, "data/income_tract.csv")
print('Saved data/income_tract.csv')


#------------------------------------------------------------------#
## Household Income Ranges by Tract -- got all years
#------------------------------------------------------------------#
print('Download household income ranges (B19001) 2010-18')
inc_list = list()


for (y in tract_years) {
  var <- load_variables(y, "acs5", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "B19001"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "002$") |
                         str_detect(variable, "003$") |
                         str_detect(variable, "004$") |
                         str_detect(variable, "005$") |
                         str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "008$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "010$") |
                         str_detect(variable, "011$") |
                         str_detect(variable, "012$") |
                         str_detect(variable, "013$") |
                         str_detect(variable, "014$") |
                         str_detect(variable, "015$") |
                         str_detect(variable, "016$") |
                         str_detect(variable, "017$")) &
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  inc_list[[y]] <- la
  
}


# Append all the years into a df
print('Append household income ranges')
inc = do.call(rbind, inc_list)

write_csv(inc, "data/income_range_tract.csv")
print('Saved data/income_range_tract.csv')


#------------------------------------------------------------------#
## Vehicles Available by Tract -- got all years
#------------------------------------------------------------------#
# This table is available at tract-level, and contains means of transportation by
# selected characteristics. Denominator is workers who are 16+ yrs. Also contains
# race/ethnicity, housing tenure (owner vs renter), and vehicles available.
# Just use vehicles available, because looking at workers 16+ yrs makes sense. 
print('Download vehicles available (S0802) 2010-18')
veh_list = list()


for (y in tract_years) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S0802"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "094$") |
                         str_detect(variable, "095$") |
                         str_detect(variable, "096$") |
                         str_detect(variable, "097$")) &
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  veh_list[[y]] <- la
  
}


# Append all the years into a df
print('Append vehicles')
vehicles = do.call(rbind, veh_list)

write_csv(vehicles, "data/vehicles_tract.csv")
print('Saved data/vehicles_tract.csv')


#------------------------------------------------------------------#
## Commute Mode by Tract -- got all years
#------------------------------------------------------------------#
print('Download commute mode (S0801) 2010-18')
commute_list = list()


for (y in tract_years) {
  var <- load_variables(y, "acs5/subject", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "S0801"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "003$") |
                         str_detect(variable, "005$") |
                         str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "009$") |
                         str_detect(variable, "010$") |
                         str_detect(variable, "011$") |
                         str_detect(variable, "012$") |
                         str_detect(variable, "013$")) &
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  commute_list[[y]] <- la
  
}


# Append all the years into a df
print('Append commute')
commute = do.call(rbind, commute_list)

write_csv(commute, "data/commute_tract.csv")
print('Saved data/commute_tract.csv')


#------------------------------------------------------------------#
## Owner vs Renter Occupied / Tenure by Tract -- got all years
#------------------------------------------------------------------#
print('Download tenure (B25008) 2010-18')
tenure_list = list()


for (y in tract_years) {
  var <- load_variables(y, "acs5", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "B25008"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "002$") |
                         str_detect(variable, "003$")) &
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  tenure_list[[y]] <- la
  
}


# Append all the years into a df
print('Append tenure')
tenure = do.call(rbind, tenure_list)

write_csv(tenure, "data/tenure_tract.csv")
print('Saved data/tenure_tract.csv')

#------------------------------------------------------------------#
## Race by Tract -- got all years
#------------------------------------------------------------------#
print('Download tenure (B02001) 2010-18')
race_list = list()


for (y in tract_years) {
  var <- load_variables(y, "acs5", cache = TRUE)
  columns <- var %>% filter(str_detect(name, "B02001"))
  
  ca <- get_acs(geography = "tract", year = y, variables = columns$name,
                state = "CA", survey = "acs5", geometry = FALSE)
  
  la <- ca %>% filter((str_detect(variable, "001$") |
                         str_detect(variable, "002$") |
                         str_detect(variable, "003$") |
                         str_detect(variable, "004$") |
                         str_detect(variable, "005$") |
                         str_detect(variable, "006$") |
                         str_detect(variable, "007$") |
                         str_detect(variable, "008$")) &
                        str_detect(GEOID, "^06037")
  )
  
  la$year <- y
  race_list[[y]] <- la
  
}


# Append all the years into a df
print('Append race')
race = do.call(rbind, race_list)

write_csv(race, "data/race_tract.csv")
print('Saved data/race_tract.csv')