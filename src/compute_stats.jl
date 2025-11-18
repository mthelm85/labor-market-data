get_labor_force(df::DataFrame) = @rsubset(df, :PEMLR ∈ [1, 2, 3, 4])
get_employed(df::DataFrame) = @rsubset(df, :PEMLR ∈ [1, 2])
get_unemployed(df::DataFrame) = @rsubset(df, :PEMLR ∈ [3, 4])
get_prime_age(df::DataFrame) = @rsubset(df, 25 ≤ :PRTAGE ≤ 54)

function at_below_mw(df::DataFrame, mw::Float64)
    universe = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PEERNHRY == 1
        :PEIO1COW ∈ [4, 5, 7]
    end

    target = @rsubset universe 0 < :PTERNHLY ≤ mw

    return (
        total_employed_hourly_private = sum(universe.PWCMPWGT),
        total_at_below_mw = sum(target.PWORWGT),
        pct_at_below_mw = sum(target.PWORWGT) / sum(universe.PWCMPWGT) * 100
    )
end

function unemployment_rate(df::DataFrame)
    labor_force = get_labor_force(df)
    unemployed = get_unemployed(df)

    return (sum(unemployed.PWCMPWGT) / sum(labor_force.PWCMPWGT) * 100)
end

function discouraged_rate(df::DataFrame)
    universe = get_prime_age(df)
    discouraged = @rsubset universe :PRDISC .== 1

    return (sum(discouraged.PWCMPWGT) / sum(universe.PWCMPWGT) * 100_000)
end

function median_hourly_wage(df::DataFrame)
    universe = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PEIO1COW ∈ [4, 5, 7]
        :PTERNHLY > 0
    end

    return (median(universe.PTERNHLY, weights(universe.PWORWGT)))
end

function wage_distribution_by_industry(df::DataFrame)
    universe = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PEIO1COW ∈ [4, 5, 7]
        :PTERNHLY > 0
    end

    sector_stats = @by universe :PRMJIND1 begin
        :min = minimum(:PTERNHLY)
        :q1 = quantile(:PTERNHLY, weights(:PWORWGT), 0.25)
        :q2 = quantile(:PTERNHLY, weights(:PWORWGT), 0.5)
        :q3 = quantile(:PTERNHLY, weights(:PWORWGT), 0.75)
        :max = maximum(:PTERNHLY)
    end

    return sector_stats
end

function wage_distribution_by_occupation(df::DataFrame)
    universe = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PEIO1COW ∈ [4, 5, 7]
        :PTERNHLY > 0
    end

    occ_stats = @by universe :PRDTOCC1 begin
        :min = minimum(:PTERNHLY)
        :q1 = quantile(:PTERNHLY, weights(:PWORWGT), 0.25)
        :q2 = quantile(:PTERNHLY, weights(:PWORWGT), 0.5)
        :q3 = quantile(:PTERNHLY, weights(:PWORWGT), 0.75)
        :max = maximum(:PTERNHLY)
    end

    return occ_stats
end

