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

# Main pipeline: fetch data for last 12 months and compute statistics
function main()
    # Get API key from environment
    api_key = get(ENV, "CENSUS_API_KEY", nothing)
    if isnothing(api_key)
        error("CENSUS_API_KEY environment variable not set")
    end

    # Calculate date range
    current_date = today()
    # Use previous month since current month data may not be available yet
    end_date = current_date - Month(1)

    # Fetch data for the configured lookback window.
    # Monthly stats and the aggregation window both use `LOOKBACK_MONTHS`.
    months_count = LOOKBACK_MONTHS

    println("Fetching data for the last $months_count months through $(year(end_date))-$(lpad(month(end_date),2,'0'))")

    # Collect DataFrames and monthly stats
    dfs = DataFrame[]
    monthly = []

    for i in 0:(months_count-1)
        target_date = end_date - Month(i)
        y = year(target_date)
        m = month(target_date)

        println("Processing $y-$(lpad(m, 2, '0'))...")

        df = fetch_cps_data(y, m, api_key)
        if isnothing(df)
            continue
        end

        push!(dfs, df)

        # For the monthly outputs we only produce stats for the most recent LOOKBACK_MONTHS
        if i < LOOKBACK_MONTHS
            println("  Computing monthly statistics...")
            # compute_stats.jl provides these functions: unemployment_rate, at_below_mw, discouraged_rate, median_hourly_wage
            ustat = unemployment_rate(df)
            mw = at_below_mw(df, FEDERAL_MIN_WAGE)
            dstat = discouraged_rate(df)
            mwage = median_hourly_wage(df)

            month_obj = Dict(
                "year" => y,
                "month" => m,
                "unemployment_rate" => round(ustat, digits=2),
                "min_wage_pct" => round(mw.pct_at_below_mw, digits=2),
                "discouraged_rate" => round(dstat, digits=2),
                "median_hourly_wage" => round(mwage, digits=2)
            )

            push!(monthly, month_obj)
        end
    end

    # Build 12-month combined DataFrame for industry/occupation aggregations
    if isempty(dfs)
        error("No data fetched for aggregation period")
    end

    df_12m = vcat(dfs...)

    println("Computing 12-month wage distributions by industry and occupation...")
    ind_stats = wage_distribution_by_industry(df_12m)
    occ_stats = wage_distribution_by_occupation(df_12m)

    # Build industry quantile objects from ind_stats (fields: PRMJIND1, min, q1, q2, q3, max)
    industries = []
    for row in eachrow(ind_stats)
        push!(industries, Dict(
            "industry_code" => row.PRMJIND1,
            "industry_name" => get(PRMJIND1_NAMES, row.PRMJIND1, "Industry $(row.PRMJIND1)"),
            "min" => round(row.min, digits=2),
            "q1" => round(row.q1, digits=2),
            "q2" => round(row.q2, digits=2),
            "q3" => round(row.q3, digits=2),
            "max" => round(row.max, digits=2)
        ))
    end

    # Build occupation quantile objects from occ_stats (fields: PRDTOCC1, min, q1, q2, q3, max)
    occupations = []
    for row in eachrow(occ_stats)
        push!(occupations, Dict(
            "occupation_code" => row.PRDTOCC1,
            "occupation_name" => get(OCCUPATION_NAMES, row.PRDTOCC1, "Occupation $(row.PRDTOCC1)"),
            "min" => round(row.min, digits=2),
            "q1" => round(row.q1, digits=2),
            "q2" => round(row.q2, digits=2),
            "q3" => round(row.q3, digits=2),
            "max" => round(row.max, digits=2)
        ))
    end

    # Final output object
    output = Dict(
        "generated_at" => string(now()),
        "lookback_months" => LOOKBACK_MONTHS,
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