#=
Fetch CPS microdata for a given year and month with timeout and retry.
Returns a DataFrame with all requested variables, or nothing if fetch fails.
=#
function fetch_cps_data(year::Int, month::Int, api_key::String; max_retries::Int=3, timeout::Int=120)
    month_name = MONTH_NAMES[month]
    url = "https://api.census.gov/data/$year/cps/basic/$month_name"
    
    params = Dict(
        "get" => "PWCMPWGT,PWORWGT,PTERNHLY,PEMLR,PEIO1COW,PEERNHRY,PRMJIND1,PRMJOCC1,PRHRUSL,PRTAGE",
        "key" => api_key
    )
    
    safe_parse_float(s) = something(tryparse(Float64, s), -1.0)
    safe_parse_int(s) = something(tryparse(Int64, s), -1)
    
    for attempt in 1:max_retries
        try
            println("  Attempt $attempt/$max_retries...")
            
            # HTTP request with timeout
            res = HTTP.get(url, query=params, readtimeout=timeout, connect_timeout=30)
            body = JSON3.read(String(res.body))
            
            data = body[2:end]
            n_rows = length(data)
            
            df = DataFrame(
                PWCMPWGT = Vector{Float64}(undef, n_rows),
                PWORWGT = Vector{Float64}(undef, n_rows),
                PTERNHLY = Vector{Float64}(undef, n_rows),
                PEMLR = Vector{Int64}(undef, n_rows),
                PEIO1COW = Vector{Int64}(undef, n_rows),
                PEERNHRY = Vector{Int64}(undef, n_rows),
                PRMJIND1 = Vector{Int64}(undef, n_rows),
                PRMJOCC1 = Vector{Int64}(undef, n_rows),
                PRHRUSL = Vector{Union{Int64, Missing}}(undef, n_rows),
                PRTAGE = Vector{Int64}(undef, n_rows)
            )
            
            for (i, row) in enumerate(data)
                df.PWCMPWGT[i] = safe_parse_float(row[1])
                df.PWORWGT[i] = safe_parse_float(row[2])
                df.PTERNHLY[i] = safe_parse_float(row[3])
                df.PEMLR[i] = safe_parse_int(row[4])
                df.PEIO1COW[i] = safe_parse_int(row[5])
                df.PEERNHRY[i] = safe_parse_int(row[6])
                df.PRMJIND1[i] = safe_parse_int(row[7])
                df.PRMJOCC1[i] = safe_parse_int(row[8])
                df.PRHRUSL[i] = something(tryparse(Int64, row[9]), missing)
                df.PRTAGE[i] = safe_parse_int(row[10])
            end
            
            println("  Fetched data for $year-$month")
            return df
            
        catch e
            if attempt < max_retries
                wait_time = 2^attempt  # Exponential backoff: 2, 4, 8 seconds
                @warn "Attempt $attempt failed for $year-$month: $e. Retrying in $wait_time seconds..."
                sleep(wait_time)
            else
                @warn "Failed to fetch data for $year-$month after $max_retries attempts: $e"
                return nothing
            end
        end
    end
    
    return nothing
end