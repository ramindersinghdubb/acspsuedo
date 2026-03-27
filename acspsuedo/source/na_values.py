"""
Missing values to properly indicate to users that queries may
contain unique integer and/or non-integer values indicating
particular characteristics of queried tables.
"""

# REFERENCE
# https://www.census.gov/data/developers/data-sets/acs-1year/notes-on-acs-estimate-and-annotation-values.html
# Note that by construction, annotation variables are discarded upon query.
# Thus, the concern is only applicable to estimate values.


INSUFFICIENT_OBSERVATIONS_FOR_MOE = -222222222
"""
The margin of error could not be computed because there were an insufficient number of sample observations.
"""



EXTREME_MEDIAN = -333333333
"""
The margin of error could not be computed because the median falls in the lowest interval or highest interval
of an open-ended distribution.
"""



INDEPENDENT_ESTIMATE = -555555555
"""
A margin of error is not appropriate because the corresponding estimate is controlled to an independent
population or housing estimate. Effectively, the corresponding estimate has no sampling error and the
margin of error may be treated as zero.
"""



INSUFFICIENT_SAMPLE_OBSERVATIONS = -666666666
"""
The estimate could not be computed because there were an insufficient number of sample observations.
For a ratio of medians estimate, one or both of the median estimates falls in the lowest interval or
highest interval of an open-ended distribution. For a 5-year median estimate, the margin of error
associated with a median was larger than the median itself.
"""



UNAVAILABLE_OR_NOT_APPLICABLE_MOE = -888888888
"The estimate or margin of error is not applicable or not available."



INSUFFICENT_SAMPLES_IN_GEOGRAPHY = -999999999
"""
The estimate or margin of error cannot be displayed because there were an insufficient number of sample
cases in the selected geographic area
"""




REPLACEMENT_VALUES = [
    INSUFFICIENT_OBSERVATIONS_FOR_MOE,
    EXTREME_MEDIAN,
    INDEPENDENT_ESTIMATE,
    INSUFFICIENT_SAMPLE_OBSERVATIONS,
    UNAVAILABLE_OR_NOT_APPLICABLE_MOE,
    INSUFFICENT_SAMPLES_IN_GEOGRAPHY
]