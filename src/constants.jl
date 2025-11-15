# Configuration constants
const FEDERAL_MIN_WAGE = 7.25
const LOOKBACK_MONTHS = 12
const OUTPUT_FILE = "data/labor_stats.json"

# Month name mapping for Census API
const MONTH_NAMES = Dict(
    1 => "jan", 2 => "feb", 3 => "mar", 4 => "apr",
    5 => "may", 6 => "jun", 7 => "jul", 8 => "aug",
    9 => "sep", 10 => "oct", 11 => "nov", 12 => "dec"
)

# Industry name mapping (PRDTIND1)
const INDUSTRY_NAMES = Dict(
    1 => "Agriculture",
    2 => "Forestry, logging, fishing, and hunting",
    3 => "Mining, quarrying, and oil and gas extraction",
    4 => "Construction",
    5 => "Nonmetallic mineral product manufacturing",
    6 => "Primary metals and fabricated metal products",
    7 => "Machinery manufacturing",
    8 => "Computer and electronic product manufacturing",
    9 => "Electrical equipment, appliance manufacturing",
    10 => "Transportation equipment manufacturing",
    11 => "Wood products",
    12 => "Furniture and fixtures manufacturing",
    13 => "Miscellaneous and not specified manufacturing",
    14 => "Food manufacturing",
    15 => "Beverage and tobacco products",
    16 => "Textile, apparel, and leather manufacturing",
    17 => "Paper and printing",
    18 => "Petroleum and coal products manufacturing",
    19 => "Chemical manufacturing",
    20 => "Plastics and rubber products",
    21 => "Wholesale trade",
    22 => "Retail trade",
    23 => "Transportation and warehousing",
    24 => "Utilities",
    25 => "Publishing industries (except internet)",
    26 => "Motion picture and sound recording industries",
    27 => "Broadcasting (except internet)",
    28 => "Internet publishing and broadcasting",
    29 => "Telecommunications",
    30 => "Internet service providers and data processing services",
    31 => "Other information services",
    32 => "Finance",
    33 => "Insurance",
    34 => "Real estate",
    35 => "Rental and leasing services",
    36 => "Professional, scientific, and technical services",
    37 => "Management of companies and enterprises",
    38 => "Administrative and support services",
    39 => "Waste management and remediation services",
    40 => "Educational services",
    41 => "Hospitals",
    42 => "Health care services, except hospitals",
    43 => "Social assistance services",
    44 => "Arts, entertainment, and recreation",
    45 => "Accommodation",
    46 => "Food services and drinking places",
    47 => "Repair and maintenance",
    48 => "Personal and laundry services",
    49 => "Membership associations and organizations",
    50 => "Private households",
    51 => "Public administration",
    52 => "Armed forces"
)

# Occupation name mapping (PRDTOCC1)
const OCCUPATION_NAMES = Dict(
    1 => "Management",
    2 => "Business and financial operations",
    3 => "Computer and mathematical",
    4 => "Architecture and engineering",
    5 => "Life, physical, and social science",
    6 => "Community and social service",
    7 => "Legal",
    8 => "Education, training, and library",
    9 => "Arts, design, entertainment, sports, and media",
    10 => "Healthcare practitioners and technical",
    11 => "Healthcare support",
    12 => "Protective service",
    13 => "Food preparation and serving",
    14 => "Building and grounds cleaning and maintenance",
    15 => "Personal care and service",
    16 => "Sales",
    17 => "Office and administrative support",
    18 => "Farming, fishing, and forestry",
    19 => "Construction and extraction",
    20 => "Installation, maintenance, and repair",
    21 => "Production",
    22 => "Transportation and material moving",
    23 => "Armed Forces"
)