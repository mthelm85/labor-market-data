#=
Fetch CPS microdata for a given year and month.
Returns a DataFrame with all requested variables, or nothing if fetch fails.
=#
function fetch_cps_data(year::Int, month::Int, api_key::String)
    month_name = MONTH_NAMES[month]
    url = "https://api.census.gov/data/$year/cps/basic/$month_name"
    
    params = Dict(
        "get" => "PWCMPWGT,PWORWGT,PTERNHLY,PEMLR,PEIO1COW,PEERNHRY,PRDTIND1,PRDTOCC1,PRMJIND1,PRMJOCC1,PRHRUSL,PRTAGE",
        "key" => api_key
    )
    
    safe_parse_float(s) = something(tryparse(Float64, s), -1.0)
    safe_parse_int(s) = something(tryparse(Int64, s), -1)
    
    try
        res = HTTP.get(url, query=params)
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
            PRDTIND1 = Vector{Int64}(undef, n_rows),
            PRDTOCC1 = Vector{Int64}(undef, n_rows),
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
            df.PRDTIND1[i] = safe_parse_int(row[7])
            df.PRDTOCC1[i] = safe_parse_int(row[8])
            df.PRMJIND1[i] = safe_parse_int(row[9])
            df.PRMJOCC1[i] = safe_parse_int(row[10])
            df.PRHRUSL[i] = something(tryparse(Int64, row[11]), missing)
            df.PRTAGE[i] = safe_parse_int(row[12])
        end
        
        println("  Fetched data for $year-$month")
        return df
        
    catch e
        @warn "Failed to fetch data for $year-$month: $e"
        return nothing
    end
end