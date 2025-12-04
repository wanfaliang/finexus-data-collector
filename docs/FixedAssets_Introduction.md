Appendix D – Fixed Assets (BEA API Documentation)

  Here's a comprehensive summary of the Fixed Assets dataset from pages 27-31 of the BEA API User Guide:

  Overview

  - DataSetName: FixedAssets
  - Description: Contains data from the standard set of Fixed Assets tables as published online
  - Data Frequency: Annual only (updated once per year, typically late August through early October)

  ---
  API Parameters

  | Parameter | Type   | Description                         | Required | Multiple Values | "All" Value |
  |-----------|--------|-------------------------------------|----------|-----------------|-------------|
  | TableName | String | The standard NIPA table identifier  | Yes      | No              | N/A         |
  | Year      | String | List of year(s) of data to retrieve | Yes      | Yes             | X or ALL    |

  ---
  Parameter Details

  TableName Parameter (Required, Single Value)

  - Identifies a specific Fixed Assets table
  - Only one table can be requested per data request
  - Requests with invalid TableName + Year combinations will return an error
  - Note: The old TableID parameter was discontinued in October 2018; use TableName instead

  Year Parameter (Required, Multiple Values Allowed)

  - Specifies the year(s) of data requested
  - Multiple years: comma-delimited string (e.g., "2000,2001,2002")
  - Special values: X or ALL returns all available years
  - If requested years aren't available, only available data is returned; if no data exists, an error is returned

  ---
  API Calls to Get Parameter Values

  | Parameter | API Call                                                                                                                                               |
  |-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
  | TableName | https://apps.bea.gov/api/data/?&UserID=Your-36Character-Key&method=GetParameterValues&DataSetName=FixedAssets&ParameterName=TableName&ResultFormat=xml |
  | Year      | https://apps.bea.gov/api/data/?&UserID=Your-36Character-Key&method=GetParameterValues&DataSetName=FixedAssets&ParameterName=Year&ResultFormat=xml      |

  ---
  Example API Requests

  1. Current-Cost Net Stock of Private Fixed Assets for all years:
  https://apps.bea.gov/api/data/?&UserID=Your-36Character-Key&method=GetData&DataSetName=FixedAssets&TableName=FAAt201&Year=ALL&ResultFormat=xml

  2. Chain-Type Quantity Indexes for Depreciation for 2015 and 2016:
  https://apps.bea.gov/api/data/?&UserID=Your-36Character-Key&method=GetData&DataSetName=FixedAssets&TableName=FAAt405&Year=2015,2016&ResultFormat=xml

  ---
  Fixed Assets Dimensions Elements in Return Data

  | Parameter Name  | Ordinal | Datatype | IsValue | Description                                                             |
  |-----------------|---------|----------|---------|-------------------------------------------------------------------------|
  | TableName       | 1       | String   | No      | Unique identifier for the NIPA table requested                          |
  | SeriesCode      | 2       | String   | No      | Unique identifier for the time series of the data item                  |
  | LineNumber      | 3       | String   | No      | Sequence of the data item within the table                              |
  | LineDescription | 4       | String   | No      | Description of the transactions measured in the data item               |
  | TimePeriod      | 5       | String   | No      | Time period (YYYY for annual, YYYYQn for quarterly, YYYYMx for monthly) |
  | Metric_Name     | 6       | String   | No      | Measurement indicator (e.g., "Current Dollars", "Fisher Price Index")   |
  | CL_UNIT         | 7       | String   | No      | Calculation type of the data item                                       |
  | UNIT_MULT       | 8       | String   | No      | Base-10 exponent multiplier (e.g., "6" = millions)                      |
  | DataValue       | 9       | Numeric  | Yes     | Value of the data item, formatted with commas                           |

  NoteRef Attribute

  - Included in all data elements as a reference to Notes elements
  - May have multiple comma-delimited values
  - Every NoteRef is guaranteed to have a corresponding Notes element containing the table title

  ---
  TableName to TableID Mapping (Selected Examples)

  | TableName | TableId | TableName  | TableId | TableName | TableId |
  |-----------|---------|------------|---------|-----------|---------|
  | FAAt101   | 16      | FAAt307ESI | 138     | FAAt610   | 119     |
  | FAAt102   | 17      | FAAt307I   | 139     | FAAt701   | 149     |
  | FAAt201   | 18      | FAAt401    | 26      | FAAt801   | 32      |
  | FAAt202   | 19      | FAAt402    | 27      | FAAt901   | 34      |
  | FAAt301E  | 21      | FAAt501    | 28      | FAAt902   | 95      |
  | FAAt301S  | 22      | FAAt601    | 41      | FAAt903   | 104     |

  (Full mapping table available in the document with ~90 table mappings)

  ---
  Key Implementation Considerations

  1. Base URL: https://apps.bea.gov/api/data
  2. Response Formats: JSON (default) or XML via ResultFormat parameter
  3. Rate Limiting:
    - 100 requests/minute
    - 100 MB data/minute
    - 30 errors/minute
    - Exceeding triggers 1-hour timeout (HTTP 429 with RETRY-AFTER header)
  4. Authentication: Required 36-character UserID from registration
  5. Updates: Data updated annually (late August - early October)
  6. Additional Info: https://www.bea.gov/resources/learning-center/what-to-know-fixed-assets