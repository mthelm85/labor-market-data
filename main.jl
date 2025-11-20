using DataFramesMeta
using Dates
using HTTP
using JSON3
using StatsBase

# Load all modules
include("src/constants.jl")
include("src/fetch.jl")
include("src/compute_stats.jl")
include("src/output.jl")

# =============================================================================
# Helper Functions
# =============================================================================

# Build monthly statistics dictionary for a single month
function compute_monthly_stats(df::DataFrame, year::Int, month::Int)
    # Core statistics
    urate = unemployment_rate(df)
    min_wage_pct = at_below_mw(df, FEDERAL_MIN_WAGE)
    youth_participation = youth_participation_rate(df)
    median_wage = median_hourly_wage(df)
    
    # Industry unemployment breakdown
    industry_unemployment = build_industry_unemployment_array(df)
    
    # Occupation unemployment breakdown
    occupation_unemployment = build_occupation_unemployment_array(df)
    
    return Dict(
        "year" => year,
        "month" => month,
        "unemployment_rate" => round(urate, digits=2),
        "min_wage_pct" => round(min_wage_pct, digits=2),
        "youth_participation_rate" => round(youth_participation, digits=2),
        "median_hourly_wage" => round(median_wage, digits=2),
        "unemployment_by_industry" => industry_unemployment,
        "unemployment_by_occupation" => occupation_unemployment
    )
end

# Build industry unemployment rate array
function build_industry_unemployment_array(df::DataFrame)
    unemp_by_industry = unemployment_rate_by_sector(df)
    
    return [Dict(
        "industry_code" => row.PRMJIND1,
        "industry_name" => get(INDUSTRY_NAMES, row.PRMJIND1, "Industry $(row.PRMJIND1)"),
        "unemployment_rate" => round(row.unemployment_rate, digits=2)
    ) for row in eachrow(unemp_by_industry)]
end

# Build occupation unemployment rate array
function build_occupation_unemployment_array(df::DataFrame)
    unemp_by_occupation = unemployment_rate_by_occupation(df)
    
    return [Dict(
        "occupation_code" => row.PRMJOCC1,
        "occupation_name" => get(OCCUPATION_NAMES, row.PRMJOCC1, "Occupation $(row.PRMJOCC1)"),
        "unemployment_rate" => round(row.unemployment_rate, digits=2)
    ) for row in eachrow(unemp_by_occupation)]
end

# Fetch data for all months in the lookback period
function fetch_monthly_data(api_key::String, end_date::Date, lookback_months::Int)
    # Pre-allocate storage
    dataframes = Vector{DataFrame}(undef, lookback_months)
    monthly_stats = Vector{Dict}(undef, lookback_months)
    fetch_count = 0
    
    # Iterate through months in reverse chronological order
    for i in 0:(lookback_months - 1)
        target_date = end_date - Month(i)
        year_val = year(target_date)
        month_val = month(target_date)
        
        println("Processing $year_val-$(lpad(month_val, 2, '0'))...")
        
        # Fetch data for this month
        df = fetch_cps_data(year_val, month_val, api_key)
        
        # Skip if fetch failed
        if isnothing(df)
            continue
        end
        
        # Store data
        fetch_count += 1
        dataframes[fetch_count] = df
        
        # Compute and store monthly statistics
        println("  Computing monthly statistics...")
        monthly_stats[fetch_count] = compute_monthly_stats(df, year_val, month_val)
    end
    
    # Trim to actual fetched data
    resize!(dataframes, fetch_count)
    resize!(monthly_stats, fetch_count)
    
    return dataframes, monthly_stats, fetch_count
end

# Build employment dictionary by grouping variable (industry or occupation)
function build_employment_dict(employed_df::DataFrame, grouping_var::Symbol)
    employment_dict = Dict{Int, Float64}()
    
    for group in groupby(employed_df, grouping_var)
        code = first(group[!, grouping_var])
        total_weight = sum(group.PWCMPWGT)
        employment_dict[code] = total_weight
    end
    
    return employment_dict
end

# Build industry statistics array with wage distributions and employment
function build_industry_array(wage_stats::DataFrame, employment_dict::Dict{Int, Float64}, 
                              months_count::Int)
    return map(eachrow(wage_stats)) do row
        total_employment = get(employment_dict, row.PRMJIND1, 0.0)
        avg_employment = months_count > 0 ? round(Int, total_employment / months_count) : 0
        
        Dict(
            "industry_code" => row.PRMJIND1,
            "industry_name" => get(INDUSTRY_NAMES, row.PRMJIND1, "Industry $(row.PRMJIND1)"),
            "avg_monthly_employment" => avg_employment,
            "p10" => round(row.p10, digits=2),
            "p25" => round(row.p25, digits=2),
            "p50" => round(row.p50, digits=2),
            "p75" => round(row.p75, digits=2),
            "p90" => round(row.p90, digits=2)
        )
    end
end

# Build occupation statistics array with wage distributions and employment
function build_occupation_array(wage_stats::DataFrame, employment_dict::Dict{Int, Float64},
                                months_count::Int)
    return map(eachrow(wage_stats)) do row
        total_employment = get(employment_dict, row.PRMJOCC1, 0.0)
        avg_employment = months_count > 0 ? round(Int, total_employment / months_count) : 0
        
        Dict(
            "occupation_code" => row.PRMJOCC1,
            "occupation_name" => get(OCCUPATION_NAMES, row.PRMJOCC1, "Occupation $(row.PRMJOCC1)"),
            "avg_monthly_employment" => avg_employment,
            "p10" => round(row.p10, digits=2),
            "p25" => round(row.p25, digits=2),
            "p50" => round(row.p50, digits=2),
            "p75" => round(row.p75, digits=2),
            "p90" => round(row.p90, digits=2)
        )
    end
end

# Compute aggregate statistics across all months
function compute_aggregate_stats(combined_df::DataFrame, months_count::Int)
    println("Computing wage distributions by industry and occupation...")
    
    # Filter to employed individuals
    employed_df = get_employed(combined_df)
    
    # Compute wage distributions
    industry_wage_stats = wage_distribution_by_sector(combined_df)
    occupation_wage_stats = wage_distribution_by_occupation(combined_df)
    
    # Build employment lookup dictionaries
    industry_employment = build_employment_dict(employed_df, :PRMJIND1)
    occupation_employment = build_employment_dict(employed_df, :PRMJOCC1)
    
    # Build final output arrays
    industries = build_industry_array(industry_wage_stats, industry_employment, months_count)
    occupations = build_occupation_array(occupation_wage_stats, occupation_employment, months_count)
    
    return industries, occupations
end

# =============================================================================
# Main Pipeline
# =============================================================================

function main()
    # Validate API key
    api_key = get(ENV, "CENSUS_API_KEY", nothing)
    if isnothing(api_key)
        error("CENSUS_API_KEY environment variable not set")
    end
    
    # Calculate date range for data fetch
    current_date = today()
    end_date = current_date - Month(1)
    
    println("Fetching data for the last $LOOKBACK_MONTHS months through $(year(end_date))-$(lpad(month(end_date), 2, '0'))")
    
    # Fetch monthly data
    dataframes, monthly_stats, fetch_count = fetch_monthly_data(api_key, end_date, LOOKBACK_MONTHS)
    
    # Ensure we got at least some data
    if isempty(dataframes)
        error("No data fetched for aggregation period")
    end
    
    # Combine all monthly dataframes
    println("Combining data from $fetch_count months...")
    combined_df = vcat(dataframes...)
    
    # Compute aggregate statistics
    industries, occupations = compute_aggregate_stats(combined_df, fetch_count)
    
    # Build final output structure
    output = Dict(
        "generated_at" => string(now()),
        "lookback_months" => fetch_count,
        "monthly" => monthly_stats,
        "industries" => industries,
        "occupations" => occupations
    )
    
    # Write to output file
    write_results(output, OUTPUT_FILE)
end

# =============================================================================
# Entry Point
# =============================================================================

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end