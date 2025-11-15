# Compute percentage of workforce at or below minimum wage from CPS data.
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
        min_wage_percentage = round(percentage, digits=2),
        min_wage_count = round(Int, at_or_below_mw),
        min_wage_total = round(Int, total_workers)
    )
end

function compute_unemployment_rate(df::DataFrame)
    # Filter to labor force (employed + unemployed)
    labor_force_df = @rsubset df begin
        :PEMLR ∈ [1, 2, 3, 4]
    end
    
    total_labor_force = sum(labor_force_df.PWCMPWGT)
    unemployed = sum(labor_force_df.PWCMPWGT .* (labor_force_df.PEMLR .∈ Ref([3, 4])))
    
    unemployment_rate = (unemployed / total_labor_force) * 100
    
    return (
        unemployment_rate = round(unemployment_rate, digits=2),
        unemployed_count = round(Int, unemployed),
        labor_force_count = round(Int, total_labor_force)
    )
end

# Compute labor force participation rate from CPS data.
function compute_lfpr(df::DataFrame)
    total_population = sum(df.PWCMPWGT)
    labor_force = sum(df.PWCMPWGT .* (df.PEMLR .∈ Ref([1, 2, 3, 4])))
    
    lfpr = (labor_force / total_population) * 100
    
    return (
        labor_force_participation_rate = round(lfpr, digits=2),
        labor_force_count = round(Int, labor_force),
        total_population_count = round(Int, total_population)
    )
end

# Compute median hourly wage from CPS data.
function compute_median_wage(df::DataFrame)
    # Filter to workers with valid hourly earnings
    wage_df = @rsubset df begin
        :PEMLR ∈ [1, 2]         # Employed
        :PEERNHRY == 1          # Paid hourly
        :PTERNHLY != -0.01      # Valid wage data
        :PTERNHLY > 0           # Positive wages only
    end
    
    if nrow(wage_df) == 0
        return (median_hourly_wage = missing,)
    end
    
    # Weighted median
    sorted_idx = sortperm(wage_df.PTERNHLY)
    sorted_wages = wage_df.PTERNHLY[sorted_idx]
    sorted_weights = wage_df.PWORWGT[sorted_idx]
    
    cumsum_weights = cumsum(sorted_weights)
    total_weight = sum(sorted_weights)
    median_idx = findfirst(x -> x >= total_weight / 2, cumsum_weights)
    
    median_wage = sorted_wages[median_idx]
    
    return (
        median_hourly_wage = round(median_wage, digits=2),
    )
end

# Compute top 10 industries by employment.
function compute_top_industries(df::DataFrame)
    # Filter to employed
    employed_df = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PRDTIND1 > 0
    end
    
    # Aggregate by industry
    industry_totals = combine(groupby(employed_df, :PRDTIND1)) do group
        (employment = sum(group.PWCMPWGT),)
    end
    
    # Sort and take top 10
    sort!(industry_totals, :employment, rev=true)
    top_10 = first(industry_totals, 10)
    
    # Format output
    industries = [(
        industry_code = row.PRDTIND1,
        industry_name = get(INDUSTRY_NAMES, row.PRDTIND1, "Industry $(row.PRDTIND1)"),
        employment = round(Int, row.employment)
    ) for row in eachrow(top_10)]
    
    return (top_industries = industries,)
end

# Compute top 10 occupations by employment.
function compute_top_occupations(df::DataFrame)
    # Filter to employed
    employed_df = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PRDTOCC1 > 0
    end
    
    # Aggregate by occupation
    occupation_totals = combine(groupby(employed_df, :PRDTOCC1)) do group
        (employment = sum(group.PWCMPWGT),)
    end
    
    # Sort and take top 10
    sort!(occupation_totals, :employment, rev=true)
    top_10 = first(occupation_totals, 10)
    
    # Format output
    occupations = [(
        occupation_code = row.PRDTOCC1,
        occupation_name = get(OCCUPATION_NAMES, row.PRDTOCC1, "Occupation $(row.PRDTOCC1)"),
        employment = round(Int, row.employment)
    ) for row in eachrow(top_10)]
    
    return (top_occupations = occupations,)
end

#= Compute part-time employment rate from CPS data.
Part-time defined as usually working < 35 hours per week.
=#
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
        parttime_employment_rate = round(parttime_rate, digits=2),
        parttime_count = round(Int, part_time),
        total_employed_count = round(Int, total_employed)
    )
end

#=
Compute median overtime hours (hours beyond 40) from CPS data.
PRHRUSL codes: 5=35-39 hours, 6=40 hours, etc.
=#
function compute_median_overtime_hours(df::DataFrame)
    # Filter to full-time workers (usually work 35+ hours)
    fulltime_df = @rsubset df begin
        :PEMLR ∈ [1, 2]         # Employed
        :PRHRUSL ∈ [5, 6]       # Usually work 35+ hours
        :PEHRACT1 >= 0          # Valid actual hours
    end
    
    if nrow(fulltime_df) == 0
        return (median_overtime_hours = missing,)
    end
    
    # Calculate weighted median of actual hours worked
    median_hours = median(fulltime_df.PEHRACT1, pweights(fulltime_df.PWCMPWGT))
    overtime_hours = median_hours - 40.0
    
    return (
        median_overtime_hours = round(overtime_hours, digits=2),
    )
end

# Compute median overtime hours by industry (top 10 industries with most OT).
function compute_overtime_by_industry(df::DataFrame)
    # Filter to full-time workers
    fulltime_df = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PRHRUSL ∈ [5, 6]
        :PEERNHRY == 1          # Paid hourly
        :PEIO1COW ∈ [4, 5, 7]   # Private sector
        :PEHRACT1 >= 0
        :PRDTIND1 > 0
    end
    
    # Calculate median OT hours by industry
    industry_ot = combine(groupby(fulltime_df, :PRDTIND1)) do group
        med_hours = median(group.PEHRACT1, pweights(group.PWCMPWGT))
        (median_ot_hours = med_hours - 40.0, sample_size = nrow(group))
    end
    
    # Filter to industries with reasonable sample size and sort
    filter!(row -> row.sample_size >= 30, industry_ot)
    sort!(industry_ot, :median_ot_hours, rev=true)
    top_10 = first(industry_ot, min(10, nrow(industry_ot)))
    
    # Format output
    industries = [(
        industry_code = row.PRDTIND1,
        industry_name = get(INDUSTRY_NAMES, row.PRDTIND1, "Industry $(row.PRDTIND1)"),
        median_ot_hours = round(row.median_ot_hours, digits=2)
    ) for row in eachrow(top_10)]
    
    return (overtime_by_industry = industries,)
end

# Compute median hourly wage by industry (top 10 lowest-paying industries).
function compute_median_wage_by_industry(df::DataFrame)
    # Filter to hourly workers with valid wage data
    wage_df = @rsubset df begin
        :PEMLR ∈ [1, 2]
        :PEERNHRY == 1
        :PEIO1COW ∈ [4, 5, 7]
        :PTERNHLY != -0.01
        :PTERNHLY > 0
        :PRDTIND1 > 0
    end
    
    # Calculate median wage by industry
    industry_wages = combine(groupby(wage_df, :PRDTIND1)) do group
        (median_wage = median(group.PTERNHLY, pweights(group.PWORWGT)),
         sample_size = nrow(group))
    end
    
    # Filter to industries with reasonable sample size
    filter!(row -> row.sample_size >= 30, industry_wages)
    sort!(industry_wages, :median_wage)
    lowest_10 = first(industry_wages, min(10, nrow(industry_wages)))
    
    # Format output
    industries = [(
        industry_code = row.PRDTIND1,
        industry_name = get(INDUSTRY_NAMES, row.PRDTIND1, "Industry $(row.PRDTIND1)"),
        median_hourly_wage = round(row.median_wage, digits=2)
    ) for row in eachrow(lowest_10)]
    
    return (lowest_wage_industries = industries,)
end