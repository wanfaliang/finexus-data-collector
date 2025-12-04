BEA ITA (International Transactions) API - Comprehensive Guide

  Based on Appendix G (pages 40-42) of the BEA API User Guide, here's the detailed information for
  the ITA dataset:

  Overview

  DataSetName: ITA

  The ITA dataset contains data on U.S. international transactions. BEA's international
  transactions (balance of payments) accounts include all transactions between U.S. and foreign
  residents.

  ---
  API Parameters

  | Parameter     | Type   | Description
                      | Required | Multiple Values | "All" Value | Default      |
  |---------------|--------|-----------------------------------------------------------------------
  --------------------|----------|-----------------|-------------|--------------|
  | Indicator     | String | The indicator code for the type of transaction requested
                      | No       | Yes             | All         | All          |
  | AreaOrCountry | String | The area or country requested
                      | No       | Yes             | All         | AllCountries |
  | Frequency     | String | A - Annual, QSA - Quarterly seasonally adjusted, QNSA - Quarterly not
  seasonally adjusted | No       | Yes             | All         | All          |
  | Year          | String | Year requested
                      | No       | Yes             | All         | All          |

  ---
  Parameter Details

  1. Indicator Parameter (optional, multiple values allowed)

  - Specifies the type of transaction
  - Values usually correspond to lines in ITA tables at BEA's interactive tables
  - Constraint: Exactly one Indicator parameter value must be provided unless exactly one
  AreaOrCountry parameter value (other than "ALL" and "AllCountries") is requested
  - Multiple Indicators can only be specified if a single AreaOrCountry is specified

  2. AreaOrCountry Parameter (optional, multiple values allowed)

  - Specifies the counterparty area or country of the transactions
  - Default: "AllCountries" (returns the total for all countries)
  - "All": Returns all data available by area and country
  - Constraint: Exactly one AreaOrCountry parameter value must be provided unless exactly one
  Indicator parameter value is requested. A list of countries can only be specified if a single
  Indicator is specified.

  3. Frequency Parameter (optional, multiple values allowed)

  Valid values:
  - A – Annual
  - QSA – Quarterly seasonally adjusted
  - QNSA – Quarterly not seasonally adjusted

  4. Year Parameter (optional, multiple values allowed)

  - Specifies the year of the data requested
  - When quarterly data are requested, all available quarters for the specified year will be
  returned

  ---
  API Calls to Get Parameter Values

  # Get available Indicators
  https://apps.bea.gov/api/data/?&UserID=Your-36Character-Key&method=GetParameterValues&DataSetName
  =ITA&ParameterName=Indicator&ResultFormat=xml

  # Get available Areas/Countries
  https://apps.bea.gov/api/data/?&UserID=Your-36Character-Key&method=GetParameterValues&DataSetName
  =ITA&ParameterName=AreaOrCountry&ResultFormat=xml

  # Get available Frequencies
  https://apps.bea.gov/api/data/?&UserID=Your-36Character-Key&method=GetParameterValues&DataSetName
  =ITA&ParameterName=Frequency&ResultFormat=xml

  # Get available Years
  https://apps.bea.gov/api/data/?&UserID=Your-36Character-Key&method=GetParameterValues&DataSetName
  =ITA&ParameterName=Year&ResultFormat=xml

  ---
  Example API Requests

  Balance on goods with China for 2011 and 2012:
  https://apps.bea.gov/api/data/?&UserID=Your-36Character-Key&method=GetData&DataSetName=ITA&Indica
  tor=BalGds&AreaOrCountry=China&Frequency=A&Year=2011,2012&ResultFormat=xml

  Net U.S. acquisition of portfolio investment assets (quarterly not seasonally adjusted) for 2013:
  https://apps.bea.gov/api/data/?&UserID=Your-36Character-Key&method=GetData&DataSetName=ITA&Indica
  tor=PfInvAssets&AreaOrCountry=AllCountries&Frequency=QNSA&Year=2013&ResultFormat=xml

  ---
  ITA Dimensions Elements in Return Data

  | Parameter Name        | Ordinal | Datatype | IsValue | Description
                                                                |
  |-----------------------|---------|----------|---------|-----------------------------------------
  --------------------------------------------------------------|
  | Indicator             | 1       | String   | No      | The Indicator parameter value of the
  data item                                                        |
  | AreaOrCountry         | 2       | String   | No      | The AreaOrCountry parameter value of the
   data item                                                    |
  | Frequency             | 3       | String   | No      | The Frequency parameter value of the
  data item                                                        |
  | Year                  | 4       | String   | No      | The Year parameter value of the data
  item                                                             |
  | TimeSeriesId          | 5       | String   | No      | A unique identifier for the time series
  of the data item                                              |
  | TimeSeriesDescription | 6       | String   | No      | A description of the transactions
  measured in the data item                                           |
  | TimePeriod            | 7       | String   | No      | Time period in form YYYY for annual data
   and YYYYQn for quarterly data (where n is the quarter digit) |
  | CL_UNIT               | 8       | String   | No      | Base unit of measurement (e.g., "USD"
  for U.S. dollars)                                               |
  | UNIT_MULT             | 9       | String   | No      | Base-10 exponent of the multiplier
  (e.g., "6" = millions, DataValue × 10⁶)                            |
  | DataValue             | 10      | Numeric  | No      | Integer or decimal value of the
  statistic (may be blank)                                              |

  Notes

  - A NoteRef attribute is included in all data elements as a reference to Notes elements in the
  returned data
  - NoteRef may have multiple values (comma-delimited string)
  - Any NoteRef attribute is guaranteed to have a corresponding Notes element
  - NoteRef may be blank

  ---
  Key Implementation Considerations

  1. Rate Limiting:
    - 100 requests per minute
    - 100 MB data volume per minute
    - 30 errors per minute
    - Exceeding limits results in 1-hour timeout (HTTP 429)
  2. Result Formats:
    - JSON (default)
    - XML
  3. Base URL:
  https://apps.bea.gov/api/data
  4. Required Parameters for all requests:
    - UserID (36-character key)
    - method (GetData, GetParameterValues, etc.)
    - DataSetName (ITA)
