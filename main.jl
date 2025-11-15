using DataFramesMeta
using Dates
using HTTP
using JSON3
using StatsBase

# Load all modules
include("src/constants.jl")
include("src/fetch.jl")
include("src/compute.jl")
include("src/output.jl")

# Main pipeline: fetch data for last 12 months and compute statistics
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
            # Compute all statistics from the same data
            println("  Computing statistics...")
            min_wage_stats = compute_min_wage_percentage(df)
            unemployment_stats = compute_unemployment_rate(df)
            lfpr_stats = compute_lfpr(df)
            median_wage_stats = compute_median_wage(df)
            top_industries = compute_top_industries(df)
            top_occupations = compute_top_occupations(df)
            parttime_stats = compute_parttime_rate(df)
            overtime_stats = compute_median_overtime_hours(df)
            overtime_industry_stats = compute_overtime_by_industry(df)
            wage_industry_stats = compute_median_wage_by_industry(df)
            
            # Combine all statistics
            month_data = merge(
                (year = y, month = m),
                min_wage_stats,
                unemployment_stats,
                lfpr_stats,
                median_wage_stats,
                top_industries,
                top_occupations,
                parttime_stats,
                overtime_stats,
                overtime_industry_stats,
                wage_industry_stats
            )
            
            push!(results, month_data)
        end
    end
    
    # Write results to file
    write_results(results, OUTPUT_FILE)
end

# Run the pipeline
if abspath(PROGRAM_FILE) == @__FILE__
    main()
end