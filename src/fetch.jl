#=
Fetch CPS microdata for a given year and month.
Returns a DataFrame with all requested variables, or nothing if fetch fails.
=#
function fetch_cps_data(year::Int, month::Int, api_key::String)
    month_name = MONTH_NAMES[month]
    url = "https://api.census.gov/data/$year/cps/basic/$month_name"
    
    # Request all variables we might need for various statistics
    params = Dict(
        "get" => "PWCMPWGT,PWORWGT,PTERNHLY,PTERNH1C,PEMLR,PEIO1COW,PEERNHRY,PRDTIND1,PRDTOCC1,PRMJIND1,PRMJOCC1,PRHRUSL,PRTAGE,PEERNHRO,PRDISC",
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
        df.PRDTIND1 = parse.(Int64, df.PRDTIND1)
        df.PRDTOCC1 = parse.(Int64, df.PRDTOCC1)
        df.PRHRUSL = tryparse.(Int64, df.PRHRUSL)
        df.PRTAGE = parse.(Int64, df.PRTAGE)
        df.PEERNHRO = parse.(Int64, df.PEERNHRO)
        df.PRDISC = parse.(Int64, df.PRDISC)
        df.PTERNH1C = parse.(Float64, df.PTERNH1C)
        df.PRMJIND1 = parse.(Int64, df.PRMJIND1)
        df.PRMJOCC1 = parse.(Int64, df.PRMJOCC1)
        
        return df
        
    catch e
        @warn "Failed to fetch data for $year-$month: $e"
        return nothing
    end
end