# Write results to JSON file with pretty formatting.
function write_results(results::Vector, output_file::String)
    # Sort by date (oldest to newest)
    sort!(results, by = x -> (x.year, x.month))
    
    # Create output structure
    output = Dict(
        "generated_at" => string(now()),
        "lookback_months" => LOOKBACK_MONTHS,
        "data" => results
    )
    
    # Ensure output directory exists
    mkpath(dirname(output_file))
    
    # Write to JSON file
    open(output_file, "w") do f
        JSON3.pretty(f, output)
    end
    
    return length(results)
end