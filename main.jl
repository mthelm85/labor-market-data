using Dates
using HTTP
using JSON3
using DataFramesMeta

# Configuration
const FEDERAL_MIN_WAGE = 7.25
const LOOKBACK_MONTHS = 4
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
        min_wage_percentage=round(percentage, digits=2),
        min_wage_count=round(Int, at_or_below_mw),
        min_wage_total=round(Int, total_workers)
    )
end

"""
Compute unemployment rate from CPS data.
PEMLR codes: 1-2=employed, 3-4=unemployed, 5-7=not in labor force
"""
function compute_unemployment_rate(df::DataFrame)
    # Filter to labor force (employed + unemployed)
    labor_force_df = @rsubset df begin
        :PEMLR ∈ [1, 2, 3, 4]
    end

    total_labor_force = sum(labor_force_df.PWCMPWGT)
    unemployed = sum(labor_force_df.PWCMPWGT .* (labor_force_df.PEMLR .∈ Ref([3, 4])))

    unemployment_rate = (unemployed / total_labor_force) * 100

    return (
        unemployment_rate=round(unemployment_rate, digits=2),
        unemployed_count=round(Int, unemployed),
        labor_force_count=round(Int, total_labor_force)
    )
end

"""
Compute labor force participation rate from CPS data.
"""
function compute_lfpr(df::DataFrame)
    total_population = sum(df.PWCMPWGT)
    labor_force = sum(df.PWCMPWGT .* (df.PEMLR .∈ Ref([1, 2, 3, 4])))

    lfpr = (labor_force / total_population) * 100

    return (
        labor_force_participation_rate=round(lfpr, digits=2),
        labor_force_count=round(Int, labor_force),
        total_population_count=round(Int, total_population)
    )
end

"""
Compute median hourly wage from CPS data.
"""
function compute_median_wage(df::DataFrame)
    # Filter to workers with valid hourly earnings
    wage_df = @rsubset df begin
        :PEMLR ∈ [1, 2]         # Employed
        :PEERNHRY == 1          # Paid hourly
        :PTERNHLY != -0.01      # Valid wage data
        :PTERNHLY > 0           # Positive wages only
    end

    if nrow(wage_df) == 0
        return (median_hourly_wage=missing,)
    end

    # Weighted median using Statistics
    sorted_idx = sortperm(wage_df.PTERNHLY)
    sorted_wages = wage_df.PTERNHLY[sorted_idx]
    sorted_weights = wage_df.PWORWGT[sorted_idx]

    cumsum_weights = cumsum(sorted_weights)
    total_weight = sum(sorted_weights)
    median_idx = findfirst(x -> x >= total_weight / 2, cumsum_weights)

    median_wage = sorted_wages[median_idx]

    return (
        median_hourly_wage=round(median_wage, digits=2),
    )
end

"""
Compute top 10 industries by employment.
"""
function compute_top_industries(df::DataFrame)
    # Industry name mapping (top industries only for brevity)
    industry_names = Dict(
        4 => "Construction",
        22 => "Retail trade",
        40 => "Educational services",
        42 => "Health care (excluding hospitals)",
        41 => "Hospitals",
        46 => "Food services",
        23 => "Transportation and warehousing",
        36 => "Professional and technical services",
        21 => "Wholesale trade",
        38 => "Administrative and support services"
    )

    # Filter to employed
    employed_df = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PRDTIND1 > 0
    end

    # Aggregate by industry
    industry_totals = combine(groupby(employed_df, :PRDTIND1)) do group
        (employment=sum(group.PWCMPWGT),)
    end

    # Sort and take top 10
    sort!(industry_totals, :employment, rev=true)
    top_10 = first(industry_totals, 10)

    # Format output
    industries = [(
        industry_code=row.PRDTIND1,
        industry_name=get(industry_names, row.PRDTIND1, "Industry $(row.PRDTIND1)"),
        employment=round(Int, row.employment)
    ) for row in eachrow(top_10)]

    return (top_industries=industries,)
end

"""
Compute top 10 occupations by employment.
"""
function compute_top_occupations(df::DataFrame)
    # Occupation name mapping (subset)
    occupation_names = Dict(
        1 => "Management",
        13 => "Food preparation and serving",
        16 => "Sales",
        17 => "Office and administrative support",
        22 => "Transportation and material moving",
        10 => "Healthcare practitioners and technical",
        11 => "Healthcare support",
        8 => "Education, training, and library",
        3 => "Computer and mathematical",
        2 => "Business and financial operations"
    )

    # Filter to employed
    employed_df = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PRDTOCC1 > 0
    end

    # Aggregate by occupation
    occupation_totals = combine(groupby(employed_df, :PRDTOCC1)) do group
        (employment=sum(group.PWCMPWGT),)
    end

    # Sort and take top 10
    sort!(occupation_totals, :employment, rev=true)
    top_10 = first(occupation_totals, 10)

    # Format output
    occupations = [(
        occupation_code=row.PRDTOCC1,
        occupation_name=get(occupation_names, row.PRDTOCC1, "Occupation $(row.PRDTOCC1)"),
        employment=round(Int, row.employment)
    ) for row in eachrow(top_10)]

    return (top_occupations=occupations,)
end

"""
Compute part-time employment rate from CPS data.
Part-time defined as usually working < 35 hours per week.
"""
function compute_parttime_rate(df::DataFrame)
    # Filter to employed
    employed_df = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PEHRACT1 >= 0  # Valid hours data
    end

    total_employed = sum(employed_df.PWCMPWGT)
    part_time = sum(employed_df.PWCMPWGT .* (employed_df.PEHRACT1 .< 35))

    parttime_rate = (part_time / total_employed) * 100

    return (
        parttime_employment_rate=round(parttime_rate, digits=2),
        parttime_count=round(Int, part_time),
        total_employed_count=round(Int, total_employed)
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
            # Compute all statistics from the same data
            println("  Computing statistics...")
            min_wage_stats = compute_min_wage_percentage(df)
            unemployment_stats = compute_unemployment_rate(df)
            lfpr_stats = compute_lfpr(df)
            median_wage_stats = compute_median_wage(df)
            top_industries = compute_top_industries(df)
            top_occupations = compute_top_occupations(df)
            parttime_stats = compute_parttime_rate(df)

            # Combine all statistics
            month_data = merge(
                (year=y, month=m),
                min_wage_stats,
                unemployment_stats,
                lfpr_stats,
                median_wage_stats,
                top_industries,
                top_occupations,
                parttime_stats
            )

            push!(results, month_data)
        end
    end

    # Sort by date (oldest to newest)
    sort!(results, by=x -> (x.year, x.month))

    # Create output structure
    output = Dict(
        "generated_at" => string(now()),
        "lookback_months" => LOOKBACK_MONTHS,
        "data" => results
    )

    # Ensure output directory exists
    mkpath(dirname(OUTPUT_FILE))

    # Write to JSON file
    open(OUTPUT_FILE, "w") do f
        JSON3.pretty(f, output)
    end

    println("\n✓ Successfully wrote $(length(results)) months of data to $OUTPUT_FILE")
    if !isempty(results)
        latest = results[end]
        println("  Latest month: $(latest.year)-$(lpad(latest.month, 2, '0'))")
        println("  - Unemployment rate: $(latest.unemployment_rate)%")
        println("  - Labor force participation: $(latest.labor_force_participation_rate)%")
        println("  - Median hourly wage: \$$(latest.median_hourly_wage)")
    end
end

# Run the pipeline
if abspath(PROGRAM_FILE) == @__FILE__
    main()
end