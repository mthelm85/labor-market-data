# Write results (new schema) to JSON file with pretty formatting.
function write_results(output::Dict, output_file::String)
    # Ensure monthly array is sorted oldest -> newest if present
    if haskey(output, "monthly") && isa(output["monthly"], AbstractVector)
        sort!(output["monthly"], by = x -> (x["year"], x["month"]))
    end

    # Ensure output directory exists
    mkpath(dirname(output_file))

    # Write to JSON file
    open(output_file, "w") do f
        JSON3.pretty(f, output)
    end

    return true
end