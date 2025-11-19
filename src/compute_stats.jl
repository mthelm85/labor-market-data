get_labor_force(df::DataFrame) = @rsubset(df, :PEMLR ∈ [1, 2, 3, 4])
get_employed(df::DataFrame) = @rsubset(df, :PEMLR ∈ [1, 2])
get_unemployed(df::DataFrame) = @rsubset(df, :PEMLR ∈ [3, 4])
get_youth(df::DataFrame) = @rsubset(df, :PRTAGE < 18)

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

function youth_participation_rate(df::DataFrame)
    total_youth = get_youth(df)
    youth_labor_force = @rsubset total_youth :PEMLR ∈ [1, 2, 3, 4]

    return (sum(youth_labor_force.PWCMPWGT) / sum(total_youth.PWCMPWGT) * 100)
end

function median_hourly_wage(df::DataFrame)
    universe = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PEIO1COW ∈ [4, 5, 7]
        :PTERNHLY > 0
    end

    return (median(universe.PTERNHLY, weights(universe.PWORWGT)))
end

function wage_distribution_by_sector(df::DataFrame)
    universe = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PEIO1COW ∈ [4, 5, 7]
        :PTERNHLY > 0
        :PWORWGT > 0 
        1 ≤ :PRMJIND1 ≤ 14 
    end

    sector_stats = @by universe :PRMJIND1 begin
        :p10 = quantile(:PTERNHLY, weights(:PWORWGT), 0.1)
        :p25 = quantile(:PTERNHLY, weights(:PWORWGT), 0.25)
        :p50 = quantile(:PTERNHLY, weights(:PWORWGT), 0.5)
        :p75 = quantile(:PTERNHLY, weights(:PWORWGT), 0.75)
        :p90 = quantile(:PTERNHLY, weights(:PWORWGT), 0.9)
    end

    return sector_stats
end

function wage_distribution_by_occupation(df::DataFrame)
    universe = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PEIO1COW ∈ [4, 5, 7]
        :PTERNHLY > 0
        :PWORWGT > 0 
        1 ≤ :PRMJOCC1 ≤ 22  
    end

    occ_stats = @by universe :PRMJOCC1 begin
        :p10 = quantile(:PTERNHLY, weights(:PWORWGT), 0.1)
        :p25 = quantile(:PTERNHLY, weights(:PWORWGT), 0.25)
        :p50 = quantile(:PTERNHLY, weights(:PWORWGT), 0.5)
        :p75 = quantile(:PTERNHLY, weights(:PWORWGT), 0.75)
        :p90 = quantile(:PTERNHLY, weights(:PWORWGT), 0.9)
    end

    return occ_stats
end

function employment_by_sector(df::DataFrame)
    employed = get_employed(df)

    sector_stats = @by employed :PRMJIND1 begin
        :total_employed = sum(:PWCMPWGT)
    end

    return sector_stats
end

function employment_by_occupation(df::DataFrame)
    employed = get_employed(df)

    occ_stats = @by employed :PRMJOCC1 begin
        :total_employed = sum(:PWCMPWGT)
    end

    return occ_stats
end

function unemployment_rate_by_sector(df::DataFrame)
    @chain df begin
        @rsubset 1 ≤ :PRMJIND1 ≤ 14
        @by :PRMJIND1 begin
            :labor_force = sum(:PWCMPWGT[:PEMLR .∈ Ref([1, 2, 3, 4])])
            :unemployed = sum(:PWCMPWGT[:PEMLR .∈ Ref([3, 4])])
        end
        @rtransform :unemployment_rate = (:unemployed / :labor_force) * 100
    end
end

function unemployment_rate_by_occupation(df::DataFrame)
    @chain df begin
        @rsubset 1 ≤ :PRMJOCC1 ≤ 22
        @by :PRMJOCC1 begin
            :labor_force = sum(:PWCMPWGT[:PEMLR .∈ Ref([1, 2, 3, 4])])
            :unemployed = sum(:PWCMPWGT[:PEMLR .∈ Ref([3, 4])])
        end
        @rtransform :unemployment_rate = (:unemployed / :labor_force) * 100
    end
end