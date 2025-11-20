# Configuration constants
const FEDERAL_MIN_WAGE = 7.25
const LOOKBACK_MONTHS = 38
const OUTPUT_FILE = "data/labor_stats.json"

# Month name mapping for Census API
const MONTH_NAMES = Dict(
    1 => "jan", 2 => "feb", 3 => "mar", 4 => "apr",
    5 => "may", 6 => "jun", 7 => "jul", 8 => "aug",
    9 => "sep", 10 => "oct", 11 => "nov", 12 => "dec"
)
# Major industry recode mapping for `PRMJIND1` (job 1)
# Edited universe: PRDTIND1 = 1-51
const INDUSTRY_NAMES = Dict(
    1 => "Agriculture, forestry, fishing, and hunting",
    2 => "Mining",
    3 => "Construction",
    4 => "Manufacturing",
    5 => "Wholesale and retail trade",
    6 => "Transportation and utilities",
    7 => "Information",
    8 => "Financial activities",
    9 => "Professional and business services",
    10 => "Educational and health services",
    11 => "Leisure and hospitality",
    12 => "Other services",
    13 => "Public administration",
    14 => "Armed Forces"
)

# Occupation name mapping (PRMJOCC1)
const OCCUPATION_NAMES = Dict(
    1 => "Management, business, and financial occupations",
    2 => "Professional and related occupations",
    3 => "Service occupations",
    4 => "Sales and related occupations",
    5 => "Office and administrative support occupations",
    6 => "Farming, fishing, and forestry occupations",
    7 => "Construction and extraction occupations",
    8 => "Installation, maintenance, and repair occupations",
    9 => "Production occupations",
    10 => "Transportation and material moving occupations",
    11 => "Armed Forces"
)