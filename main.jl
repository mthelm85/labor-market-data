using Dates
using HTTP
using JSON3
using DataFramesMeta

# Configuration
const FEDERAL_MIN_WAGE = 7.25
const LOOKBACK_MONTHS = 3
const OUTPUT_FILE = "data/labor_stats.json"

# Month name mapping for Census API
const MONTH_NAMES = Dict(
    1 => "jan", 2 => "feb", 3 => "mar", 4 => "apr",
    5 => "may", 6 => "jun", 7 => "jul", 8 => "aug",
    9 => "sep", 10 => "oct", 11 => "nov", 12 => "dec"
)

"""
Fetch CPS microdata for a given year and month.
Returns a DataFrame with all requested variables, or nothing if fetch fails.
"""
function fetch_cps_data(year::Int, month::Int, api_key::String)
    month_name = MONTH_NAMES[month]
    url = "https://api.census.gov/data/$year/cps/basic/$month_name"
    
    # Request all variables we might need for various statistics
    params = Dict(
        "get" => "PWCMPWGT,PWORWGT,PTERNHLY,PEMLR,PEIO1COW,PEERNHRY,PEHRACT1,PRDTIND1,PRDTOCC1",
        "key" => api_key
    )
    
    try
        res = HTTP.get(url, query=params)
        body = JSON3.read(String(res.body))
        
        # Convert to DataFrame
        df = DataFrame(
            [getindex.(body[2:end][:], i) for i in 1:length(body[1])],
            body[1][:],
            copycols=false
        )
        
        # Parse all numeric columns
        df.PWCMPWGT = parse.(Float64, df.PWCMPWGT)
        df.PWORWGT = parse.(Float64, df.PWORWGT)
        df.PTERNHLY = parse.(Float64, df.PTERNHLY)
        df.PEMLR = parse.(Int64, df.PEMLR)
        df.PEIO1COW = parse.(Int64, df.PEIO1COW)
        df.PEERNHRY = parse.(Int64, df.PEERNHRY)
        df.PEHRACT1 = parse.(Int64, df.PEHRACT1)
        df.PRDTIND1 = parse.(Int64, df.PRDTIND1)
        df.PRDTOCC1 = parse.(Int64, df.PRDTOCC1)
        
        return df
        
    catch e
        @warn "Failed to fetch data for $year-$month: $e"
        return nothing
    end
end

"""
Compute percentage of workforce at or below minimum wage from CPS data.
"""
function compute_min_wage_percentage(df::DataFrame)
    # Filter to private sector hourly workers
    analysis_df = @rsubset df begin
        :PEMLR ∈ [1, 2]         # Employed
        :PEIO1COW ∈ [4, 5, 7]   # Private sector
        :PEERNHRY == 1          # Paid hourly
        :PTERNHLY != -0.01      # Valid wage data
    end
    
    # Calculate weighted counts
    total_workers = sum(analysis_df.PWORWGT)
    at_or_below_mw = sum(analysis_df.PWORWGT .* (analysis_df.PTERNHLY .<= FEDERAL_MIN_WAGE))
    
    percentage = (at_or_below_mw / total_workers) * 100
    
    return (
        percentage = round(percentage, digits=2),
        count = round(Int, at_or_below_mw),
        total = round(Int, total_workers)
    )
end

"""
Main pipeline: fetch data for last 12 months and write to JSON
"""
function main()
    # Get API key from environment
    api_key = get(ENV, "CENSUS_API_KEY", nothing)
    if isnothing(api_key)
        error("CENSUS_API_KEY environment variable not set")
    end
    
    # Calculate date range (last 12 months)
    current_date = today()
    # Use previous month since current month data may not be available yet
    end_date = current_date - Month(1)
    start_date = end_date - Month(LOOKBACK_MONTHS - 1)
    
    println("Fetching data from $(year(start_date))-$(month(start_date)) to $(year(end_date))-$(month(end_date))")
    
    # Collect data for each month
    results = []
    for i in 0:(LOOKBACK_MONTHS-1)
        target_date = end_date - Month(i)
        y = year(target_date)
        m = month(target_date)
        
        println("Processing $y-$(lpad(m, 2, '0'))...")
        
        # Fetch data once
        df = fetch_cps_data(y, m, api_key)
        
        if !isnothing(df)
            # Compute statistics from the same data
            min_wage_stats = compute_min_wage_percentage(df)
            
            # Add more compute functions here as needed
            # unemployment_stats = compute_unemployment_rate(df)
            # median_wage_stats = compute_median_wage(df)
            
            push!(results, (
                year = y,
                month = m,
                min_wage_stats...
            ))
        end
    end
    
    # Sort by date (oldest to newest)
    sort!(results, by = x -> (x.year, x.month))
    
    # Create output structure
    output = Dict(
        "generated_at" => string(now()),
        "metric" => "percentage_at_or_below_minimum_wage",
        "minimum_wage" => FEDERAL_MIN_WAGE,
        "data" => results
    )
    
    # Ensure output directory exists
    mkpath(dirname(OUTPUT_FILE))
    
    # Write to JSON file
    open(OUTPUT_FILE, "w") do f
        JSON3.pretty(f, output)
    end
    
    println("\n✓ Successfully wrote $(length(results)) months of data to $OUTPUT_FILE")
    println("  Latest: $(results[end].percentage)% ($(results[end].year)-$(lpad(results[end].month, 2, '0')))")
end

# Run the pipeline
if abspath(PROGRAM_FILE) == @__FILE__
    main()
end