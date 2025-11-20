# Employment status codes (PEMLR)
const EMPLOYED_AT_WORK = [1, 2]
const UNEMPLOYED = [3, 4]
const IN_LABOR_FORCE = [1, 2, 3, 4]

# Class of worker codes (PEIO1COW)
const PRIVATE_SECTOR = [4, 5, 7]

# Valid industry codes (PRMJIND1)
const VALID_INDUSTRIES = 1:13

# Valid occupation codes (PRMJOCC1)
const VALID_OCCUPATIONS = 1:10

const YOUTH_AGE_THRESHOLD = 18

# Filter to individuals in the labor force (employed or unemployed)
get_labor_force(df::DataFrame) = @rsubset df :PEMLR ∈ IN_LABOR_FORCE

# Filter to employed individuals
get_employed(df::DataFrame) = @rsubset df :PEMLR ∈ EMPLOYED_AT_WORK

# Filter to unemployed individuals
get_unemployed(df::DataFrame) = @rsubset df :PEMLR ∈ UNEMPLOYED

# Filter to youth population (under age 18)
get_youth(df::DataFrame) = @rsubset df :PRTAGE < YOUTH_AGE_THRESHOLD

# Filter to employed individuals paid hourly in the private sector
get_employed_hourly_private(df::DataFrame) = @rsubset df begin
    :PEMLR ∈ EMPLOYED_AT_WORK
    :PEERNHRY == 1
    :PEIO1COW ∈ PRIVATE_SECTOR
end

# Filter to employed private sector workers with valid wage data
get_private_employed_with_wages(df::DataFrame) = @rsubset df begin
    :PEMLR ∈ EMPLOYED_AT_WORK
    :PEIO1COW ∈ PRIVATE_SECTOR
    :PTERNHLY > 0
    :PWORWGT > 0
end

# Calculate overall unemployment rate as a percentage
function unemployment_rate(df::DataFrame)
    labor_force = get_labor_force(df)
    unemployed = get_unemployed(df)

    return (sum(unemployed.PWCMPWGT) / sum(labor_force.PWCMPWGT)) * 100
end

# Calculate percentage of hourly private sector workers at or below minimum wage
function at_below_mw(df::DataFrame, mw::Float64)
    employed_hourly_private = get_employed_hourly_private(df)
    below_mw = @rsubset employed_hourly_private 0 < :PTERNHLY ≤ mw

    return (sum(below_mw.PWORWGT) / sum(employed_hourly_private.PWCMPWGT)) * 100
end

# Calculate labor force participation rate for youth (under 18)
function youth_participation_rate(df::DataFrame)
    total_youth = get_youth(df)
    youth_labor_force = @rsubset total_youth :PEMLR ∈ IN_LABOR_FORCE

    return (sum(youth_labor_force.PWCMPWGT) / sum(total_youth.PWCMPWGT)) * 100
end

# Calculate weighted median hourly wage for private sector workers
function median_hourly_wage(df::DataFrame)
    private_employed = get_private_employed_with_wages(df)

    return median(private_employed.PTERNHLY, weights(private_employed.PWORWGT))
end

# Generic: compute wage percentile distribution by dimension (industry/occupation)
function wage_distribution_by_dimension(df::DataFrame, dimension::Symbol, valid_range)
    universe = @chain df begin
        get_private_employed_with_wages
        @rsubset $dimension ∈ valid_range
    end

    return @by universe dimension begin
        :p10 = quantile(:PTERNHLY, weights(:PWORWGT), 0.1)
        :p25 = quantile(:PTERNHLY, weights(:PWORWGT), 0.25)
        :p50 = quantile(:PTERNHLY, weights(:PWORWGT), 0.5)
        :p75 = quantile(:PTERNHLY, weights(:PWORWGT), 0.75)
        :p90 = quantile(:PTERNHLY, weights(:PWORWGT), 0.9)
    end
end

# Calculate hourly wage percentile distribution by industry
wage_distribution_by_sector(df::DataFrame) = wage_distribution_by_dimension(df, :PRMJIND1, VALID_INDUSTRIES)

# Calculate hourly wage percentile distribution by occupation
wage_distribution_by_occupation(df::DataFrame) = wage_distribution_by_dimension(df, :PRMJOCC1, VALID_OCCUPATIONS)

# Generic: compute unemployment rates by dimension (industry/occupation)
function unemployment_rate_by_dimension(df::DataFrame, dimension::Symbol, valid_range)
    @chain df begin
        @rsubset $dimension ∈ valid_range
        @by dimension begin
            :labor_force = sum(:PWCMPWGT[:PEMLR.∈Ref(IN_LABOR_FORCE)])
            :unemployed = sum(:PWCMPWGT[:PEMLR.∈Ref(UNEMPLOYED)])
        end
        @rtransform :unemployment_rate = (:unemployed / :labor_force) * 100
    end
end

# Calculate unemployment rate by industry
unemployment_rate_by_sector(df::DataFrame) = unemployment_rate_by_dimension(df, :PRMJIND1, VALID_INDUSTRIES)

# Calculate unemployment rate by occupation
unemployment_rate_by_occupation(df::DataFrame) = unemployment_rate_by_dimension(df, :PRMJOCC1, VALID_OCCUPATIONS)

# Generic: compute total employment by dimension (industry/occupation)
function employment_by_dimension(df::DataFrame, dimension::Symbol)
    employed = get_employed(df)

    return @by employed dimension begin
        :total_employed = sum(:PWCMPWGT)
    end
end

# Calculate total employment by industry
employment_by_sector(df::DataFrame) = employment_by_dimension(df, :PRMJIND1)

# Calculate total employment by occupation
employment_by_occupation(df::DataFrame) = employment_by_dimension(df, :PRMJOCC1)