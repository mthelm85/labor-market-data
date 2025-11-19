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

function main()
    # Get API key from environment
    api_key = get(ENV, "CENSUS_API_KEY", nothing)
    if isnothing(api_key)
        error("CENSUS_API_KEY environment variable not set")
    end

    # Calculate date range
    current_date = today()
    end_date = current_date - Month(1)

    println("Fetching data for the last $LOOKBACK_MONTHS months through $(year(end_date))-$(lpad(month(end_date),2,'0'))")

    # Pre-allocate arrays with known size
    dfs = Vector{DataFrame}(undef, LOOKBACK_MONTHS)
    monthly = Vector{Dict}(undef, LOOKBACK_MONTHS)
    fetch_count = 0

    # Fetch data in reverse chronological order
    for i in 0:(LOOKBACK_MONTHS-1)
        target_date = end_date - Month(i)
        y = year(target_date)
        m = month(target_date)

        println("Processing $y-$(lpad(m, 2, '0'))...")

        df = fetch_cps_data(y, m, api_key)

        if isnothing(df)
            continue
        end

        fetch_count += 1
        dfs[fetch_count] = df

        # Compute monthly statistics
        println("  Computing monthly statistics...")
        ustat = unemployment_rate(df)
        mw = at_below_mw(df, FEDERAL_MIN_WAGE)
        ystat = youth_participation_rate(df)
        mwage = median_hourly_wage(df)
        
        # Unemployment by industry
        unemp_by_ind = unemployment_rate_by_sector(df)
        ind_unemp_array = [Dict(
            "industry_code" => row.PRMJIND1,
            "industry_name" => get(PRMJIND1_NAMES, row.PRMJIND1, "Industry $(row.PRMJIND1)"),
            "unemployment_rate" => round(row.unemployment_rate, digits=2)
        ) for row in eachrow(unemp_by_ind) if 1 ≤ row.PRMJIND1 ≤ 14]
        
        # Unemployment by occupation
        unemp_by_occ = unemployment_rate_by_occupation(df)
        occ_unemp_array = [Dict(
            "occupation_code" => row.PRDTOCC1,
            "occupation_name" => get(OCCUPATION_NAMES, row.PRDTOCC1, "Occupation $(row.PRDTOCC1)"),
            "unemployment_rate" => round(row.unemployment_rate, digits=2)
        ) for row in eachrow(unemp_by_occ) if 1 ≤ row.PRDTOCC1 ≤ 22]

        monthly[fetch_count] = Dict(
            "year" => y,
            "month" => m,
            "unemployment_rate" => round(ustat, digits=2),
            "min_wage_pct" => round(mw.pct_at_below_mw, digits=2),
            "youth_participation_rate" => round(ystat, digits=2),
            "median_hourly_wage" => round(mwage, digits=2),
            "unemployment_by_industry" => ind_unemp_array,
            "unemployment_by_occupation" => occ_unemp_array
        )
    end

    # Trim to actual fetched data
    resize!(dfs, fetch_count)
    resize!(monthly, fetch_count)

    if isempty(dfs)
        error("No data fetched for aggregation period")
    end

    # Combine DataFrames once
    println("Combining data from $fetch_count months...")
    df_all = vcat(dfs...)

    println("Computing wage distributions by industry and occupation...")
    
    # Filter employed once
    employed_df = @rsubset(df_all, :PEMLR ∈ [1, 2])
    
    # Compute distributions and employment
    ind_stats = wage_distribution_by_sector(df_all)
    occ_stats = wage_distribution_by_occupation(df_all)
    
    # Build employment dictionaries for O(1) lookup
    ind_employment_dict = Dict{Int, Float64}()
    for row in groupby(employed_df, :PRMJIND1)
        ind_employment_dict[first(row.PRMJIND1)] = sum(row.PWCMPWGT)
    end
    
    occ_employment_dict = Dict{Int, Float64}()
    for row in groupby(employed_df, :PRDTOCC1)
        occ_employment_dict[first(row.PRDTOCC1)] = sum(row.PWCMPWGT)
    end

    # Build industry objects with O(1) lookups
    industries = map(eachrow(ind_stats)) do row
        total_emp = get(ind_employment_dict, row.PRMJIND1, 0.0)
        avg_emp = fetch_count > 0 ? round(Int, total_emp / fetch_count) : 0
        
        Dict(
            "industry_code" => row.PRMJIND1,
            "industry_name" => get(PRMJIND1_NAMES, row.PRMJIND1, "Industry $(row.PRMJIND1)"),
            "avg_monthly_employment" => avg_emp,
            "p10" => round(row.p10, digits=2),
            "p25" => round(row.p25, digits=2),
            "p50" => round(row.p50, digits=2),
            "p75" => round(row.p75, digits=2),
            "p90" => round(row.p90, digits=2)
        )
    end

    # Build occupation objects with O(1) lookups
    occupations = map(eachrow(occ_stats)) do row
        total_emp = get(occ_employment_dict, row.PRDTOCC1, 0.0)
        avg_emp = fetch_count > 0 ? round(Int, total_emp / fetch_count) : 0
        
        Dict(
            "occupation_code" => row.PRDTOCC1,
            "occupation_name" => get(OCCUPATION_NAMES, row.PRDTOCC1, "Occupation $(row.PRDTOCC1)"),
            "avg_monthly_employment" => avg_emp,
            "p10" => round(row.p10, digits=2),
            "p25" => round(row.p25, digits=2),
            "p50" => round(row.p50, digits=2),
            "p75" => round(row.p75, digits=2),
            "p90" => round(row.p90, digits=2)
        )
    end

    # Final output object
    output = Dict(
        "generated_at" => string(now()),
        "lookback_months" => fetch_count,
        "monthly" => monthly,
        "industries" => industries,
        "occupations" => occupations
    )

    write_results(output, OUTPUT_FILE)
end

# Run the pipeline
if abspath(PROGRAM_FILE) == @__FILE__
    main()
end