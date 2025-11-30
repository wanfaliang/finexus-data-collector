/**
 * Mapping of LA survey area codes to geographic coordinates
 * States use their geographic center, metros use city center
 */

export interface LAAreaCoordinate {
  lat: number;
  lng: number;
  type: 'state' | 'metro' | 'national';
}

// State FIPS codes to coordinates (geographic center of each state)
// Format: ST + 2-digit FIPS + 11 zeros
export const stateCoordinates: Record<string, LAAreaCoordinate> = {
  'ST0100000000000': { lat: 32.3182, lng: -86.9023, type: 'state' }, // Alabama
  'ST0200000000000': { lat: 64.2008, lng: -152.4937, type: 'state' }, // Alaska
  'ST0400000000000': { lat: 34.0489, lng: -111.0937, type: 'state' }, // Arizona
  'ST0500000000000': { lat: 35.2010, lng: -91.8318, type: 'state' }, // Arkansas
  'ST0600000000000': { lat: 36.7783, lng: -119.4179, type: 'state' }, // California
  'ST0800000000000': { lat: 39.5501, lng: -105.7821, type: 'state' }, // Colorado
  'ST0900000000000': { lat: 41.6032, lng: -73.0877, type: 'state' }, // Connecticut
  'ST1000000000000': { lat: 38.9108, lng: -75.5277, type: 'state' }, // Delaware
  'ST1100000000000': { lat: 38.9072, lng: -77.0369, type: 'state' }, // DC
  'ST1200000000000': { lat: 27.6648, lng: -81.5158, type: 'state' }, // Florida
  'ST1300000000000': { lat: 32.1656, lng: -82.9001, type: 'state' }, // Georgia
  'ST1500000000000': { lat: 19.8968, lng: -155.5828, type: 'state' }, // Hawaii
  'ST1600000000000': { lat: 44.0682, lng: -114.7420, type: 'state' }, // Idaho
  'ST1700000000000': { lat: 40.6331, lng: -89.3985, type: 'state' }, // Illinois
  'ST1800000000000': { lat: 40.2672, lng: -86.1349, type: 'state' }, // Indiana
  'ST1900000000000': { lat: 41.8780, lng: -93.0977, type: 'state' }, // Iowa
  'ST2000000000000': { lat: 39.0119, lng: -98.4842, type: 'state' }, // Kansas
  'ST2100000000000': { lat: 37.8393, lng: -84.2700, type: 'state' }, // Kentucky
  'ST2200000000000': { lat: 30.9843, lng: -91.9623, type: 'state' }, // Louisiana
  'ST2300000000000': { lat: 45.2538, lng: -69.4455, type: 'state' }, // Maine
  'ST2400000000000': { lat: 39.0458, lng: -76.6413, type: 'state' }, // Maryland
  'ST2500000000000': { lat: 42.4072, lng: -71.3824, type: 'state' }, // Massachusetts
  'ST2600000000000': { lat: 44.3148, lng: -85.6024, type: 'state' }, // Michigan
  'ST2700000000000': { lat: 46.7296, lng: -94.6859, type: 'state' }, // Minnesota
  'ST2800000000000': { lat: 32.3547, lng: -89.3985, type: 'state' }, // Mississippi
  'ST2900000000000': { lat: 37.9643, lng: -91.8318, type: 'state' }, // Missouri
  'ST3000000000000': { lat: 46.8797, lng: -110.3626, type: 'state' }, // Montana
  'ST3100000000000': { lat: 41.4925, lng: -99.9018, type: 'state' }, // Nebraska
  'ST3200000000000': { lat: 38.8026, lng: -116.4194, type: 'state' }, // Nevada
  'ST3300000000000': { lat: 43.1939, lng: -71.5724, type: 'state' }, // New Hampshire
  'ST3400000000000': { lat: 40.0583, lng: -74.4057, type: 'state' }, // New Jersey
  'ST3500000000000': { lat: 34.5199, lng: -105.8701, type: 'state' }, // New Mexico
  'ST3600000000000': { lat: 43.2994, lng: -74.2179, type: 'state' }, // New York
  'ST3700000000000': { lat: 35.7596, lng: -79.0193, type: 'state' }, // North Carolina
  'ST3800000000000': { lat: 47.5515, lng: -101.0020, type: 'state' }, // North Dakota
  'ST3900000000000': { lat: 40.4173, lng: -82.9071, type: 'state' }, // Ohio
  'ST4000000000000': { lat: 35.4676, lng: -97.5164, type: 'state' }, // Oklahoma
  'ST4100000000000': { lat: 43.8041, lng: -120.5542, type: 'state' }, // Oregon
  'ST4200000000000': { lat: 41.2033, lng: -77.1945, type: 'state' }, // Pennsylvania
  'ST4400000000000': { lat: 41.5801, lng: -71.4774, type: 'state' }, // Rhode Island
  'ST4500000000000': { lat: 33.8361, lng: -81.1637, type: 'state' }, // South Carolina
  'ST4600000000000': { lat: 43.9695, lng: -99.9018, type: 'state' }, // South Dakota
  'ST4700000000000': { lat: 35.5175, lng: -86.5804, type: 'state' }, // Tennessee
  'ST4800000000000': { lat: 31.9686, lng: -99.9018, type: 'state' }, // Texas
  'ST4900000000000': { lat: 39.3210, lng: -111.0937, type: 'state' }, // Utah
  'ST5000000000000': { lat: 44.5588, lng: -72.5778, type: 'state' }, // Vermont
  'ST5100000000000': { lat: 37.4316, lng: -78.6569, type: 'state' }, // Virginia
  'ST5300000000000': { lat: 47.7511, lng: -120.7401, type: 'state' }, // Washington
  'ST5400000000000': { lat: 38.5976, lng: -80.4549, type: 'state' }, // West Virginia
  'ST5500000000000': { lat: 43.7844, lng: -88.7879, type: 'state' }, // Wisconsin
  'ST5600000000000': { lat: 43.0759, lng: -107.2903, type: 'state' }, // Wyoming
  'ST7200000000000': { lat: 18.2208, lng: -66.5901, type: 'state' }, // Puerto Rico
};

// City name to coordinates lookup - used to match metro area names to coordinates
// The key is a partial match (first city name in metro area name)
const cityCoordinates: Record<string, { lat: number; lng: number }> = {
  // Major cities A-Z
  'Abilene': { lat: 32.4487, lng: -99.7331 },
  'Akron': { lat: 41.0814, lng: -81.5190 },
  'Albany': { lat: 42.6526, lng: -73.7562 },
  'Albuquerque': { lat: 35.0844, lng: -106.6504 },
  'Allentown': { lat: 40.6084, lng: -75.4902 },
  'Amarillo': { lat: 35.2220, lng: -101.8313 },
  'Anaheim': { lat: 33.8366, lng: -117.9143 },
  'Anchorage': { lat: 61.2181, lng: -149.9003 },
  'Ann Arbor': { lat: 42.2808, lng: -83.7430 },
  'Anniston': { lat: 33.6598, lng: -85.8316 },
  'Asheville': { lat: 35.5951, lng: -82.5515 },
  'Atlanta': { lat: 33.7490, lng: -84.3880 },
  'Atlantic City': { lat: 39.3643, lng: -74.4229 },
  'Auburn': { lat: 32.6099, lng: -85.4808 },
  'Augusta': { lat: 33.4735, lng: -82.0105 },
  'Austin': { lat: 30.2672, lng: -97.7431 },
  'Bakersfield': { lat: 35.3733, lng: -119.0187 },
  'Baltimore': { lat: 39.2904, lng: -76.6122 },
  'Bangor': { lat: 44.8016, lng: -68.7712 },
  'Barnstable': { lat: 41.7003, lng: -70.3002 },
  'Baton Rouge': { lat: 30.4515, lng: -91.1871 },
  'Beaumont': { lat: 30.0802, lng: -94.1266 },
  'Bellingham': { lat: 48.7519, lng: -122.4787 },
  'Bend': { lat: 44.0582, lng: -121.3153 },
  'Billings': { lat: 45.7833, lng: -108.5007 },
  'Binghamton': { lat: 42.0987, lng: -75.9180 },
  'Birmingham': { lat: 33.5207, lng: -86.8025 },
  'Bismarck': { lat: 46.8083, lng: -100.7837 },
  'Bloomington': { lat: 40.4842, lng: -88.9937 },
  'Boise': { lat: 43.6150, lng: -116.2023 },
  'Boise City': { lat: 43.6150, lng: -116.2023 },
  'Barnstable Town': { lat: 41.7003, lng: -70.3002 },
  'Boston': { lat: 42.3601, lng: -71.0589 },
  'Boulder': { lat: 40.0150, lng: -105.2705 },
  'Bowling Green': { lat: 36.9685, lng: -86.4808 },
  'Bremerton': { lat: 47.5673, lng: -122.6326 },
  'Bridgeport': { lat: 41.1865, lng: -73.1952 },
  'Brownsville': { lat: 25.9017, lng: -97.4975 },
  'Brunswick': { lat: 31.1499, lng: -81.4915 },
  'Buffalo': { lat: 42.8864, lng: -78.8784 },
  'Burlington': { lat: 44.4759, lng: -73.2121 },
  'Canton': { lat: 40.7989, lng: -81.3784 },
  'Cape Coral': { lat: 26.5629, lng: -81.9495 },
  'Carson City': { lat: 39.1638, lng: -119.7674 },
  'Cedar Rapids': { lat: 42.0083, lng: -91.6441 },
  'Champaign': { lat: 40.1164, lng: -88.2434 },
  'Charleston': { lat: 32.7765, lng: -79.9311 },
  'Charlotte': { lat: 35.2271, lng: -80.8431 },
  'Charlottesville': { lat: 38.0293, lng: -78.4767 },
  'Chattanooga': { lat: 35.0456, lng: -85.3097 },
  'Cheyenne': { lat: 41.1400, lng: -104.8202 },
  'Chicago': { lat: 41.8781, lng: -87.6298 },
  'Chico': { lat: 39.7285, lng: -121.8375 },
  'Cincinnati': { lat: 39.1031, lng: -84.5120 },
  'Clarksville': { lat: 36.5298, lng: -87.3595 },
  'Cleveland': { lat: 41.4993, lng: -81.6944 },
  'Coeur d\'Alene': { lat: 47.6777, lng: -116.7805 },
  'College Station': { lat: 30.6280, lng: -96.3344 },
  'Colorado Springs': { lat: 38.8339, lng: -104.8214 },
  'Columbia': { lat: 34.0007, lng: -81.0348 },
  'Columbus': { lat: 39.9612, lng: -82.9988 },
  'Corpus Christi': { lat: 27.8006, lng: -97.3964 },
  'Dallas': { lat: 32.7767, lng: -96.7970 },
  'Dalton': { lat: 34.7698, lng: -84.9702 },
  'Danville': { lat: 36.5860, lng: -79.3950 },
  'Daphne': { lat: 30.6035, lng: -87.9036 },
  'Davenport': { lat: 41.5236, lng: -90.5776 },
  'Dayton': { lat: 39.7589, lng: -84.1916 },
  'Decatur': { lat: 34.6059, lng: -86.9833 },
  'Deltona': { lat: 28.9005, lng: -81.2637 },
  'Denver': { lat: 39.7392, lng: -104.9903 },
  'Des Moines': { lat: 41.5868, lng: -93.6250 },
  'Detroit': { lat: 42.3314, lng: -83.0458 },
  'Dothan': { lat: 31.2232, lng: -85.3905 },
  'Dover': { lat: 39.1582, lng: -75.5244 },
  'Dubuque': { lat: 42.5006, lng: -90.6646 },
  'Duluth': { lat: 46.7867, lng: -92.1005 },
  'Durham': { lat: 35.9940, lng: -78.8986 },
  'East Stroudsburg': { lat: 41.0023, lng: -75.1810 },
  'Eau Claire': { lat: 44.8113, lng: -91.4985 },
  'El Centro': { lat: 32.7920, lng: -115.5631 },
  'El Paso': { lat: 31.7619, lng: -106.4850 },
  'Elizabethtown': { lat: 37.6940, lng: -85.8591 },
  'Elkhart': { lat: 41.6820, lng: -85.9767 },
  'Elmira': { lat: 42.0898, lng: -76.8077 },
  'Erie': { lat: 42.1292, lng: -80.0851 },
  'Eugene': { lat: 44.0521, lng: -123.0868 },
  'Evansville': { lat: 37.9716, lng: -87.5711 },
  'Fairbanks': { lat: 64.8378, lng: -147.7164 },
  'Fargo': { lat: 46.8772, lng: -96.7898 },
  'Farmington': { lat: 36.7281, lng: -108.2187 },
  'Fayetteville': { lat: 36.0626, lng: -94.1574 },
  'Flagstaff': { lat: 35.1983, lng: -111.6513 },
  'Flint': { lat: 43.0125, lng: -83.6875 },
  'Florence': { lat: 34.1954, lng: -79.7626 },
  'Fort Collins': { lat: 40.5853, lng: -105.0844 },
  'Fort Lauderdale': { lat: 26.1224, lng: -80.1373 },
  'Fort Smith': { lat: 35.3859, lng: -94.3985 },
  'Fort Wayne': { lat: 41.0793, lng: -85.1394 },
  'Fort Worth': { lat: 32.7555, lng: -97.3308 },
  'Fresno': { lat: 36.7378, lng: -119.7871 },
  'Gadsden': { lat: 34.0143, lng: -86.0066 },
  'Gainesville': { lat: 29.6516, lng: -82.3248 },
  'Glens Falls': { lat: 43.3095, lng: -73.6440 },
  'Goldsboro': { lat: 35.3849, lng: -77.9928 },
  'Grand Forks': { lat: 47.9253, lng: -97.0329 },
  'Grand Junction': { lat: 39.0639, lng: -108.5506 },
  'Grand Rapids': { lat: 42.9634, lng: -85.6681 },
  'Great Falls': { lat: 47.5053, lng: -111.3008 },
  'Greeley': { lat: 40.4233, lng: -104.7091 },
  'Green Bay': { lat: 44.5133, lng: -88.0133 },
  'Greensboro': { lat: 36.0726, lng: -79.7920 },
  'Greenville': { lat: 34.8526, lng: -82.3940 },
  'Gulfport': { lat: 30.3674, lng: -89.0928 },
  'Hagerstown': { lat: 39.6418, lng: -77.7200 },
  'Hammond': { lat: 30.5044, lng: -90.4612 },
  'Hanford': { lat: 36.3275, lng: -119.6457 },
  'Harrisburg': { lat: 40.2732, lng: -76.8867 },
  'Harrisonburg': { lat: 38.4496, lng: -78.8689 },
  'Hartford': { lat: 41.7658, lng: -72.6734 },
  'Hattiesburg': { lat: 31.3271, lng: -89.2903 },
  'Hickory': { lat: 35.7331, lng: -81.3412 },
  'Hilton Head': { lat: 32.2163, lng: -80.7526 },
  'Hinesville': { lat: 31.8468, lng: -81.5959 },
  'Homosassa Springs': { lat: 28.8003, lng: -82.5751 },
  'Hot Springs': { lat: 34.5037, lng: -93.0552 },
  'Houma': { lat: 29.5958, lng: -90.7195 },
  'Houston': { lat: 29.7604, lng: -95.3698 },
  'Huntington': { lat: 38.4192, lng: -82.4452 },
  'Huntsville': { lat: 34.7304, lng: -86.5861 },
  'Idaho Falls': { lat: 43.4666, lng: -112.0341 },
  'Indianapolis': { lat: 39.7684, lng: -86.1581 },
  'Iowa City': { lat: 41.6611, lng: -91.5302 },
  'Ithaca': { lat: 42.4440, lng: -76.5019 },
  'Jackson': { lat: 32.2988, lng: -90.1848 },
  'Jacksonville': { lat: 30.3322, lng: -81.6557 },
  'Janesville': { lat: 42.6828, lng: -89.0187 },
  'Jefferson City': { lat: 38.5767, lng: -92.1735 },
  'Johnson City': { lat: 36.3134, lng: -82.3535 },
  'Johnstown': { lat: 40.3267, lng: -78.9220 },
  'Jonesboro': { lat: 35.8423, lng: -90.7043 },
  'Joplin': { lat: 37.0842, lng: -94.5133 },
  'Kahului': { lat: 20.8893, lng: -156.4729 },
  'Kalamazoo': { lat: 42.2917, lng: -85.5872 },
  'Kankakee': { lat: 41.1200, lng: -87.8612 },
  'Kansas City': { lat: 39.0997, lng: -94.5786 },
  'Kennewick': { lat: 46.2112, lng: -119.1372 },
  'Killeen': { lat: 31.1171, lng: -97.7278 },
  'Kingsport': { lat: 36.5484, lng: -82.5618 },
  'Kingston': { lat: 41.9270, lng: -73.9974 },
  'Knoxville': { lat: 35.9606, lng: -83.9207 },
  'Kokomo': { lat: 40.4864, lng: -86.1336 },
  'La Crosse': { lat: 43.8014, lng: -91.2396 },
  'Lafayette': { lat: 30.2241, lng: -92.0198 },
  'Lake Charles': { lat: 30.2266, lng: -93.2174 },
  'Lake Havasu City': { lat: 34.4839, lng: -114.3225 },
  'Lakeland': { lat: 28.0395, lng: -81.9498 },
  'Lancaster': { lat: 40.0379, lng: -76.3055 },
  'Lansing': { lat: 42.7325, lng: -84.5555 },
  'Laredo': { lat: 27.5306, lng: -99.4803 },
  'Las Cruces': { lat: 32.3199, lng: -106.7637 },
  'Las Vegas': { lat: 36.1699, lng: -115.1398 },
  'Lawrence': { lat: 38.9717, lng: -95.2353 },
  'Lawton': { lat: 34.6036, lng: -98.3959 },
  'Lebanon': { lat: 40.3409, lng: -76.4114 },
  'Lewiston': { lat: 44.1004, lng: -70.2148 },
  'Lexington': { lat: 37.9887, lng: -84.4777 },
  'Lima': { lat: 40.7428, lng: -84.1052 },
  'Lincoln': { lat: 40.8258, lng: -96.6852 },
  'Little Rock': { lat: 34.7465, lng: -92.2896 },
  'Logan': { lat: 41.7370, lng: -111.8338 },
  'Longview': { lat: 32.5007, lng: -94.7405 },
  'Los Angeles': { lat: 34.0522, lng: -118.2437 },
  'Louisville': { lat: 38.2527, lng: -85.7585 },
  'Lubbock': { lat: 33.5779, lng: -101.8552 },
  'Lynchburg': { lat: 37.4138, lng: -79.1422 },
  'Macon': { lat: 32.8407, lng: -83.6324 },
  'Madera': { lat: 36.9613, lng: -120.0607 },
  'Madison': { lat: 43.0731, lng: -89.4012 },
  'Manchester': { lat: 42.9956, lng: -71.4548 },
  'Manhattan': { lat: 39.1836, lng: -96.5717 },
  'Mankato': { lat: 44.1636, lng: -93.9994 },
  'Mansfield': { lat: 40.7589, lng: -82.5145 },
  'McAllen': { lat: 26.2034, lng: -98.2300 },
  'Medford': { lat: 42.3265, lng: -122.8756 },
  'Memphis': { lat: 35.1495, lng: -90.0490 },
  'Merced': { lat: 37.3022, lng: -120.4830 },
  'Miami': { lat: 25.7617, lng: -80.1918 },
  'Midland': { lat: 31.9973, lng: -102.0779 },
  'Milwaukee': { lat: 43.0389, lng: -87.9065 },
  'Minneapolis': { lat: 44.9778, lng: -93.2650 },
  'Missoula': { lat: 46.8721, lng: -113.9940 },
  'Mobile': { lat: 30.6954, lng: -88.0399 },
  'Modesto': { lat: 37.6391, lng: -120.9969 },
  'Monroe': { lat: 32.5093, lng: -92.1193 },
  'Montgomery': { lat: 32.3792, lng: -86.3077 },
  'Morgantown': { lat: 39.6295, lng: -79.9559 },
  'Morristown': { lat: 36.2140, lng: -83.2949 },
  'Mount Vernon': { lat: 48.4201, lng: -122.3343 },
  'Muncie': { lat: 40.1934, lng: -85.3864 },
  'Muskegon': { lat: 43.2342, lng: -86.2484 },
  'Myrtle Beach': { lat: 33.6891, lng: -78.8867 },
  'Napa': { lat: 38.2975, lng: -122.2869 },
  'Naples': { lat: 26.1420, lng: -81.7948 },
  'Nashville': { lat: 36.1627, lng: -86.7816 },
  'New Haven': { lat: 41.3083, lng: -72.9279 },
  'New Orleans': { lat: 29.9511, lng: -90.0715 },
  'New York': { lat: 40.7128, lng: -74.0060 },
  'Newark': { lat: 40.7357, lng: -74.1724 },
  'Niles': { lat: 41.1839, lng: -86.2542 },
  'North Port': { lat: 27.0442, lng: -82.2359 },
  'Norwich': { lat: 41.5243, lng: -72.0759 },
  'Ocala': { lat: 29.1872, lng: -82.1401 },
  'Ocean City': { lat: 38.3365, lng: -75.0849 },
  'Odessa': { lat: 31.8457, lng: -102.3676 },
  'Ogden': { lat: 41.2230, lng: -111.9738 },
  'Oklahoma City': { lat: 35.4676, lng: -97.5164 },
  'Olympia': { lat: 47.0379, lng: -122.9007 },
  'Omaha': { lat: 41.2565, lng: -95.9345 },
  'Orlando': { lat: 28.5383, lng: -81.3792 },
  'Oshkosh': { lat: 44.0247, lng: -88.5426 },
  'Owensboro': { lat: 37.7719, lng: -87.1112 },
  'Oxnard': { lat: 34.1975, lng: -119.1771 },
  'Palm Bay': { lat: 28.0345, lng: -80.5887 },
  'Panama City': { lat: 30.1588, lng: -85.6602 },
  'Parkersburg': { lat: 39.2667, lng: -81.5615 },
  'Pensacola': { lat: 30.4213, lng: -87.2169 },
  'Peoria': { lat: 40.6936, lng: -89.5890 },
  'Philadelphia': { lat: 39.9526, lng: -75.1652 },
  'Phoenix': { lat: 33.4484, lng: -112.0740 },
  'Pine Bluff': { lat: 34.2284, lng: -92.0032 },
  'Pittsburgh': { lat: 40.4406, lng: -79.9959 },
  'Pittsfield': { lat: 42.4501, lng: -73.2454 },
  'Pocatello': { lat: 42.8713, lng: -112.4455 },
  'Portland': { lat: 45.5152, lng: -122.6784 },
  'Port St. Lucie': { lat: 27.2730, lng: -80.3582 },
  'Poughkeepsie': { lat: 41.7004, lng: -73.9210 },
  'Prescott': { lat: 34.5400, lng: -112.4685 },
  'Providence': { lat: 41.8240, lng: -71.4128 },
  'Provo': { lat: 40.2338, lng: -111.6585 },
  'Pueblo': { lat: 38.2544, lng: -104.6091 },
  'Punta Gorda': { lat: 26.9298, lng: -82.0454 },
  'Racine': { lat: 42.7261, lng: -87.7829 },
  'Raleigh': { lat: 35.7796, lng: -78.6382 },
  'Rapid City': { lat: 44.0805, lng: -103.2310 },
  'Reading': { lat: 40.3356, lng: -75.9269 },
  'Redding': { lat: 40.5865, lng: -122.3917 },
  'Reno': { lat: 39.5296, lng: -119.8138 },
  'Richmond': { lat: 37.5407, lng: -77.4360 },
  'Riverside': { lat: 33.9533, lng: -117.3962 },
  'Roanoke': { lat: 37.2710, lng: -79.9414 },
  'Rochester': { lat: 43.1566, lng: -77.6088 },
  'Rockford': { lat: 42.2711, lng: -89.0940 },
  'Rocky Mount': { lat: 35.9382, lng: -77.7905 },
  'Rome': { lat: 34.2570, lng: -85.1647 },
  'Sacramento': { lat: 38.5816, lng: -121.4944 },
  'Saginaw': { lat: 43.4195, lng: -83.9508 },
  'St. Cloud': { lat: 45.5579, lng: -94.1632 },
  'St. George': { lat: 37.0965, lng: -113.5684 },
  'St. Joseph': { lat: 39.7675, lng: -94.8467 },
  'St. Louis': { lat: 38.6270, lng: -90.1994 },
  'Salem': { lat: 44.9429, lng: -123.0351 },
  'Salinas': { lat: 36.6777, lng: -121.6555 },
  'Salisbury': { lat: 38.3607, lng: -75.5994 },
  'Salt Lake City': { lat: 40.7608, lng: -111.8910 },
  'San Angelo': { lat: 31.4638, lng: -100.4370 },
  'San Antonio': { lat: 29.4241, lng: -98.4936 },
  'San Diego': { lat: 32.7157, lng: -117.1611 },
  'San Francisco': { lat: 37.7749, lng: -122.4194 },
  'San Jose': { lat: 37.3382, lng: -121.8863 },
  'San Luis Obispo': { lat: 35.2828, lng: -120.6596 },
  'Santa Cruz': { lat: 36.9741, lng: -122.0308 },
  'Santa Fe': { lat: 35.6870, lng: -105.9378 },
  'Santa Maria': { lat: 34.9530, lng: -120.4357 },
  'Santa Rosa': { lat: 38.4404, lng: -122.7141 },
  'Savannah': { lat: 32.0809, lng: -81.0912 },
  'Scranton': { lat: 41.4090, lng: -75.6624 },
  'Seattle': { lat: 47.6062, lng: -122.3321 },
  'Sebastian': { lat: 27.8164, lng: -80.4706 },
  'Sebring': { lat: 27.4955, lng: -81.4409 },
  'Sherman': { lat: 33.6357, lng: -96.6089 },
  'Shreveport': { lat: 32.5252, lng: -93.7502 },
  'Sierra Vista': { lat: 31.5455, lng: -110.2773 },
  'Sioux City': { lat: 42.4963, lng: -96.4049 },
  'Sioux Falls': { lat: 43.5446, lng: -96.7311 },
  'South Bend': { lat: 41.6764, lng: -86.2520 },
  'Spartanburg': { lat: 34.9496, lng: -81.9320 },
  'Spokane': { lat: 47.6588, lng: -117.4260 },
  'Springfield': { lat: 39.7817, lng: -89.6501 },
  'State College': { lat: 40.7934, lng: -77.8600 },
  'Staunton': { lat: 38.1496, lng: -79.0717 },
  'Stockton': { lat: 37.9577, lng: -121.2908 },
  'Sumter': { lat: 33.9204, lng: -80.3415 },
  'Syracuse': { lat: 43.0481, lng: -76.1474 },
  'Tacoma': { lat: 47.2529, lng: -122.4443 },
  'Tallahassee': { lat: 30.4383, lng: -84.2807 },
  'Tampa': { lat: 27.9506, lng: -82.4572 },
  'Terre Haute': { lat: 39.4667, lng: -87.4139 },
  'Texarkana': { lat: 33.4418, lng: -94.0377 },
  'The Villages': { lat: 28.9347, lng: -81.9612 },
  'Toledo': { lat: 41.6528, lng: -83.5379 },
  'Topeka': { lat: 39.0473, lng: -95.6752 },
  'Trenton': { lat: 40.2206, lng: -74.7597 },
  'Tucson': { lat: 32.2226, lng: -110.9747 },
  'Tulsa': { lat: 36.1540, lng: -95.9928 },
  'Tuscaloosa': { lat: 33.2098, lng: -87.5692 },
  'Tyler': { lat: 32.3513, lng: -95.3011 },
  'Urban Honolulu': { lat: 21.3069, lng: -157.8583 },
  'Utica': { lat: 43.1009, lng: -75.2327 },
  'Valdosta': { lat: 30.8327, lng: -83.2785 },
  'Vallejo': { lat: 38.1041, lng: -122.2566 },
  'Victoria': { lat: 28.8053, lng: -97.0036 },
  'Vineland': { lat: 39.4863, lng: -75.0258 },
  'Virginia Beach': { lat: 36.8529, lng: -75.9780 },
  'Visalia': { lat: 36.3302, lng: -119.2921 },
  'Waco': { lat: 31.5493, lng: -97.1467 },
  'Warner Robins': { lat: 32.6130, lng: -83.5988 },
  'Washington': { lat: 38.9072, lng: -77.0369 },
  'Waterloo': { lat: 42.4928, lng: -92.3426 },
  'Watertown': { lat: 43.9748, lng: -75.9108 },
  'Wausau': { lat: 44.9591, lng: -89.6301 },
  'Wheeling': { lat: 40.0640, lng: -80.7209 },
  'Wichita': { lat: 37.6872, lng: -97.3301 },
  'Wichita Falls': { lat: 33.9137, lng: -98.4934 },
  'Williamsport': { lat: 41.2412, lng: -77.0011 },
  'Wilmington': { lat: 34.2257, lng: -77.9447 },
  'Winston': { lat: 36.0999, lng: -80.2442 },
  'Worcester': { lat: 42.2626, lng: -71.8023 },
  'Yakima': { lat: 46.6021, lng: -120.5059 },
  'York': { lat: 39.9626, lng: -76.7277 },
  'Youngstown': { lat: 41.0998, lng: -80.6495 },
  'Yuba City': { lat: 39.1404, lng: -121.6169 },
  'Yuma': { lat: 32.6927, lng: -114.6277 },
  // Additional cities for better metro matching
  'Oxford': { lat: 33.6140, lng: -85.8355 },
  'Opelika': { lat: 32.6454, lng: -85.3783 },
  'Fairhope': { lat: 30.5230, lng: -87.9033 },
  'Foley': { lat: 30.4066, lng: -87.6836 },
  'Muscle Shoals': { lat: 34.7445, lng: -87.6675 },
  'Crestview': { lat: 30.7621, lng: -86.5705 },
  'Fort Walton Beach': { lat: 30.4058, lng: -86.6189 },
  'Destin': { lat: 30.3935, lng: -86.4958 },
  'Daytona Beach': { lat: 29.2108, lng: -81.0228 },
  'Ormond Beach': { lat: 29.2858, lng: -81.0559 },
  'Winter Haven': { lat: 28.0222, lng: -81.7329 },
  'West Palm Beach': { lat: 26.7153, lng: -80.0534 },
  'Marco Island': { lat: 25.9412, lng: -81.7184 },
  'Bradenton': { lat: 27.4989, lng: -82.5748 },
  'Sarasota': { lat: 27.3364, lng: -82.5307 },
  'Kissimmee': { lat: 28.2920, lng: -81.4076 },
  'Sanford': { lat: 28.8001, lng: -81.2733 },
  'Melbourne': { lat: 28.0836, lng: -80.6081 },
  'Titusville': { lat: 28.6122, lng: -80.8076 },
  'Panama City Beach': { lat: 30.1766, lng: -85.8055 },
  'Ferry Pass': { lat: 30.5102, lng: -87.2119 },
  'Brent': { lat: 30.4688, lng: -87.2358 },
  'Vero Beach': { lat: 27.6386, lng: -80.3973 },
  'West Vero Corridor': { lat: 27.6297, lng: -80.4495 },
  'Clearwater': { lat: 27.9659, lng: -82.8001 },
  'St. Petersburg': { lat: 27.7676, lng: -82.6403 },
  'The Villages': { lat: 28.9347, lng: -81.9612 },
  'Wildwood': { lat: 28.8653, lng: -82.0412 },
  'Clarke County': { lat: 33.9608, lng: -83.3781 },
  'Athens': { lat: 33.9519, lng: -83.3576 },
  'Sandy Springs': { lat: 33.9304, lng: -84.3733 },
  'Roswell': { lat: 34.0232, lng: -84.3616 },
  'Richmond County': { lat: 33.4735, lng: -82.0105 },
  'St. Simons': { lat: 31.1496, lng: -81.3884 },
  'Bibb County': { lat: 32.8065, lng: -83.6974 },
  'Honolulu': { lat: 21.3069, lng: -157.8583 },
  'College': { lat: 64.8578, lng: -147.8028 },
  'Mesa': { lat: 33.4152, lng: -111.8315 },
  'Chandler': { lat: 33.3062, lng: -111.8413 },
  'Prescott Valley': { lat: 34.6100, lng: -112.3150 },
  'Douglas': { lat: 31.3445, lng: -109.5453 },
  'Springdale': { lat: 36.1867, lng: -94.1288 },
  'Rogers': { lat: 36.3320, lng: -94.1185 },
  'North Little Rock': { lat: 34.7695, lng: -92.2671 },
  'Conway': { lat: 35.0887, lng: -92.4421 },
  'Delano': { lat: 35.7688, lng: -119.2471 },
  'Long Beach': { lat: 33.7701, lng: -118.1937 },
  'Corcoran': { lat: 36.0980, lng: -119.5604 },
  'Thousand Oaks': { lat: 34.1706, lng: -118.8376 },
  'Ventura': { lat: 34.2746, lng: -119.2290 },
  'San Bernardino': { lat: 34.1083, lng: -117.2898 },
  'Ontario': { lat: 34.0633, lng: -117.6509 },
  'Roseville': { lat: 38.7521, lng: -121.2880 },
  'Folsom': { lat: 38.6780, lng: -121.1761 },
  'Chula Vista': { lat: 32.6401, lng: -117.0842 },
  'Carlsbad': { lat: 33.1581, lng: -117.3506 },
  'Oakland': { lat: 37.8044, lng: -122.2712 },
  'Fremont': { lat: 37.5485, lng: -121.9886 },
  'Sunnyvale': { lat: 37.3688, lng: -122.0363 },
  'Santa Clara': { lat: 37.3541, lng: -121.9552 },
  'Paso Robles': { lat: 35.6266, lng: -120.6910 },
  'Watsonville': { lat: 36.9103, lng: -121.7569 },
  'Santa Barbara': { lat: 34.4208, lng: -119.6982 },
  'Petaluma': { lat: 38.2324, lng: -122.6367 },
  'Lodi': { lat: 38.1302, lng: -121.2724 },
  'Loveland': { lat: 40.3978, lng: -105.0750 },
  'Aurora': { lat: 39.7294, lng: -104.8319 },
  'Centennial': { lat: 39.5807, lng: -104.8772 },
  'Stamford': { lat: 41.0534, lng: -73.5387 },
  'Danbury': { lat: 41.3948, lng: -73.4540 },
  'West Hartford': { lat: 41.7620, lng: -72.7420 },
  'East Hartford': { lat: 41.7823, lng: -72.6128 },
  'New London': { lat: 41.3557, lng: -72.0995 },
  'Willimantic': { lat: 41.7109, lng: -72.2081 },
  'Shelton': { lat: 41.3165, lng: -73.0932 },
  'Waterbury': { lat: 41.5582, lng: -73.0515 },
  'Fort Myers': { lat: 26.6406, lng: -81.8723 },
  'Arlington': { lat: 32.7357, lng: -97.1081 },
  'Alexandria': { lat: 38.8048, lng: -77.0469 },
  'Newark': { lat: 40.7357, lng: -74.1724 },
  'Jersey City': { lat: 40.7178, lng: -74.0431 },
  'Camden': { lat: 39.9259, lng: -75.1196 },
  'Wilmington': { lat: 39.7391, lng: -75.5398 },
  'Cambridge': { lat: 42.3736, lng: -71.1097 },
  'Newton': { lat: 42.3370, lng: -71.2092 },
  'Warren': { lat: 42.4775, lng: -83.0277 },
  'Dearborn': { lat: 42.3223, lng: -83.1763 },
  'Tacoma': { lat: 47.2529, lng: -122.4443 },
  'Bellevue': { lat: 47.6101, lng: -122.2015 },
  'St. Paul': { lat: 44.9537, lng: -93.0900 },
  'Bloomington': { lat: 44.8408, lng: -93.2983 },
  'Columbia': { lat: 39.2037, lng: -76.8610 },
  'Towson': { lat: 39.4015, lng: -76.6019 },
  'Concord': { lat: 35.4088, lng: -80.5795 },
  'Gastonia': { lat: 35.2621, lng: -81.1873 },
  'New Braunfels': { lat: 29.7030, lng: -98.1245 },
  'Vancouver': { lat: 45.6387, lng: -122.6615 },
  'Hillsboro': { lat: 45.5229, lng: -122.9898 },
  'Murfreesboro': { lat: 35.8456, lng: -86.3903 },
  'Franklin': { lat: 35.9251, lng: -86.8689 },
  'Round Rock': { lat: 30.5083, lng: -97.6789 },
  'Georgetown': { lat: 30.6333, lng: -97.6778 },
  'Cary': { lat: 35.7915, lng: -78.7811 },
  'Henderson': { lat: 36.0395, lng: -114.9817 },
  'Paradise': { lat: 36.0972, lng: -115.1468 },
  'Carmel': { lat: 39.9784, lng: -86.1180 },
  'Anderson': { lat: 40.1053, lng: -85.6803 },
  'Elyria': { lat: 41.3684, lng: -82.1076 },
  'Waukesha': { lat: 43.0117, lng: -88.2315 },
  'Metairie': { lat: 29.9841, lng: -90.1528 },
  'Norfolk': { lat: 36.8508, lng: -76.2859 },
  'Newport News': { lat: 37.0871, lng: -76.4730 },
  'Jefferson County': { lat: 38.1938, lng: -85.6435 },
  'Warwick': { lat: 41.7001, lng: -71.4162 },
  'Sugar Land': { lat: 29.6197, lng: -95.6349 },
  'The Woodlands': { lat: 30.1658, lng: -95.4613 },
  'Pasadena': { lat: 29.6911, lng: -95.2091 },
  'Plano': { lat: 33.0198, lng: -96.6989 },
  'Irving': { lat: 32.8140, lng: -96.9489 },
  'Fort Worth': { lat: 32.7555, lng: -97.3308 },
  'Appleton': { lat: 44.2619, lng: -88.4154 },
  'Bellingham': { lat: 48.7519, lng: -122.4787 },
  'Bremerton': { lat: 47.5673, lng: -122.6326 },
  'Champaign': { lat: 40.1164, lng: -88.2434 },
  'Daphne': { lat: 30.6035, lng: -87.9036 },
  'Duluth': { lat: 46.7867, lng: -92.1005 },
  'Durham': { lat: 35.9940, lng: -78.8986 },
  'Erie': { lat: 42.1292, lng: -80.0851 },
  'Eugene': { lat: 44.0521, lng: -123.0868 },
  'Evansville': { lat: 37.9716, lng: -87.5711 },
  'Fargo': { lat: 46.8772, lng: -96.7898 },
  'Fayetteville': { lat: 36.0626, lng: -94.1574 },
  'Flint': { lat: 43.0125, lng: -83.6875 },
  'Gainesville': { lat: 29.6516, lng: -82.3248 },
  'Grand Rapids': { lat: 42.9634, lng: -85.6681 },
  'Gulfport': { lat: 30.3674, lng: -89.0928 },
  'Hagerstown': { lat: 39.6418, lng: -77.7200 },
  'Harrisburg': { lat: 40.2732, lng: -76.8867 },
  'Hickory': { lat: 35.7331, lng: -81.3412 },
  'Huntington': { lat: 38.4192, lng: -82.4452 },
  'Huntsville': { lat: 34.7304, lng: -86.5861 },
  'Kalamazoo': { lat: 42.2917, lng: -85.5872 },
  'Kennewick': { lat: 46.2112, lng: -119.1372 },
  'Killeen': { lat: 31.1171, lng: -97.7278 },
  'Kingsport': { lat: 36.5484, lng: -82.5618 },
  'Kiryas Joel': { lat: 41.3420, lng: -74.1687 },
  'Lafayette': { lat: 30.2241, lng: -92.0198 },
  'Lakeland': { lat: 28.0395, lng: -81.9498 },
  'Lancaster': { lat: 40.0379, lng: -76.3055 },
  'Lansing': { lat: 42.7325, lng: -84.5555 },
  'Laredo': { lat: 27.5306, lng: -99.4803 },
  'Las Cruces': { lat: 32.3199, lng: -106.7637 },
  'Lincoln': { lat: 40.8258, lng: -96.6852 },
  'Little Rock': { lat: 34.7465, lng: -92.2896 },
  'Longview': { lat: 32.5007, lng: -94.7405 },
  'Lubbock': { lat: 33.5779, lng: -101.8552 },
  'Lynchburg': { lat: 37.4138, lng: -79.1422 },
  'Macon': { lat: 32.8407, lng: -83.6324 },
  'Madison': { lat: 43.0731, lng: -89.4012 },
  'Manchester': { lat: 42.9956, lng: -71.4548 },
  'McAllen': { lat: 26.2034, lng: -98.2300 },
  'Medford': { lat: 42.3265, lng: -122.8756 },
  'Merced': { lat: 37.3022, lng: -120.4830 },
  'Midland': { lat: 31.9973, lng: -102.0779 },
  'Mobile': { lat: 30.6954, lng: -88.0399 },
  'Modesto': { lat: 37.6391, lng: -120.9969 },
  'Montgomery': { lat: 32.3792, lng: -86.3077 },
  'Myrtle Beach': { lat: 33.6891, lng: -78.8867 },
  'Naples': { lat: 26.1420, lng: -81.7948 },
  'New Haven': { lat: 41.3083, lng: -72.9279 },
  'North Port': { lat: 27.0442, lng: -82.2359 },
  'Ocala': { lat: 29.1872, lng: -82.1401 },
  'Odessa': { lat: 31.8457, lng: -102.3676 },
  'Ogden': { lat: 41.2230, lng: -111.9738 },
  'Olympia': { lat: 47.0379, lng: -122.9007 },
  'Oxnard': { lat: 34.1975, lng: -119.1771 },
  'Palm Bay': { lat: 28.0345, lng: -80.5887 },
  'Pensacola': { lat: 30.4213, lng: -87.2169 },
  'Peoria': { lat: 40.6936, lng: -89.5890 },
  'Port St. Lucie': { lat: 27.2730, lng: -80.3582 },
  'Poughkeepsie': { lat: 41.7004, lng: -73.9210 },
  'Provo': { lat: 40.2338, lng: -111.6585 },
  'Pueblo': { lat: 38.2544, lng: -104.6091 },
  'Reno': { lat: 39.5296, lng: -119.8138 },
  'Riverside': { lat: 33.9533, lng: -117.3962 },
  'Roanoke': { lat: 37.2710, lng: -79.9414 },
  'Rockford': { lat: 42.2711, lng: -89.0940 },
  'Salinas': { lat: 36.6777, lng: -121.6555 },
  'Santa Maria': { lat: 34.9530, lng: -120.4357 },
  'Santa Rosa': { lat: 38.4404, lng: -122.7141 },
  'Savannah': { lat: 32.0809, lng: -81.0912 },
  'Scranton': { lat: 41.4090, lng: -75.6624 },
  'Shreveport': { lat: 32.5252, lng: -93.7502 },
  'Sioux Falls': { lat: 43.5446, lng: -96.7311 },
  'South Bend': { lat: 41.6764, lng: -86.2520 },
  'Spartanburg': { lat: 34.9496, lng: -81.9320 },
  'Spokane': { lat: 47.6588, lng: -117.4260 },
  'Springfield': { lat: 39.7817, lng: -89.6501 },
  'Stockton': { lat: 37.9577, lng: -121.2908 },
  'Syracuse': { lat: 43.0481, lng: -76.1474 },
  'Tallahassee': { lat: 30.4383, lng: -84.2807 },
  'Toledo': { lat: 41.6528, lng: -83.5379 },
  'Topeka': { lat: 39.0473, lng: -95.6752 },
  'Trenton': { lat: 40.2206, lng: -74.7597 },
  'Tyler': { lat: 32.3513, lng: -95.3011 },
  'Utica': { lat: 43.1009, lng: -75.2327 },
  'Vallejo': { lat: 38.1041, lng: -122.2566 },
  'Visalia': { lat: 36.3302, lng: -119.2921 },
  'Waco': { lat: 31.5493, lng: -97.1467 },
  'Winston': { lat: 36.0999, lng: -80.2442 },
  'Worcester': { lat: 42.2626, lng: -71.8023 },
  'Yakima': { lat: 46.6021, lng: -120.5059 },
  'York': { lat: 39.9626, lng: -76.7277 },
  'Youngstown': { lat: 41.0998, lng: -80.6495 },
};

/**
 * Get coordinates for an LA area code
 * For states: direct lookup
 * For metros: parse the area name and match to city coordinates
 */
export function getLAAreaCoordinates(areaCode: string, areaName?: string): LAAreaCoordinate | null {
  // Check states first (direct lookup)
  if (stateCoordinates[areaCode]) {
    return stateCoordinates[areaCode];
  }

  // For metros, try to match by area name
  if (areaName && areaCode.startsWith('MT')) {
    // Remove "Metropolitan Statistical Area" suffix and state abbreviations
    const cleanName = areaName
      .replace(/Metropolitan Statistical Area/i, '')
      .replace(/,\s*[A-Z]{2}(-[A-Z]{2})*\s*$/, '')
      .trim();

    // Split by hyphen to get individual city names
    const cities = cleanName.split('-').map(c => c.trim());

    // Try each city in order
    for (let cityName of cities) {
      // Skip empty city names
      if (!cityName) continue;

      // Try direct match first
      if (cityCoordinates[cityName]) {
        return { ...cityCoordinates[cityName], type: 'metro' };
      }

      // Remove common suffixes like "City", "Town", "County" and try again
      const withoutSuffix = cityName.replace(/\s+(City|Town|County)$/i, '').trim();
      if (withoutSuffix !== cityName && cityCoordinates[withoutSuffix]) {
        return { ...cityCoordinates[withoutSuffix], type: 'metro' };
      }

      // Try case-insensitive match
      const lowerCityName = cityName.toLowerCase();
      const lowerWithoutSuffix = withoutSuffix.toLowerCase();
      for (const [city, coords] of Object.entries(cityCoordinates)) {
        const lowerCity = city.toLowerCase();
        if (lowerCity === lowerCityName || lowerCity === lowerWithoutSuffix) {
          return { ...coords, type: 'metro' };
        }
      }

      // Try partial match
      for (const [city, coords] of Object.entries(cityCoordinates)) {
        if (cityName.length >= 4 && city.length >= 4) {
          if (cityName.startsWith(city) || city.startsWith(cityName) ||
              withoutSuffix.startsWith(city) || city.startsWith(withoutSuffix)) {
            return { ...coords, type: 'metro' };
          }
        }
      }
    }

  }

  return null;
}

/**
 * Get state abbreviation from area code
 */
export function getStateAbbreviation(areaCode: string): string | null {
  const stateMap: Record<string, string> = {
    'ST0100000000000': 'AL', 'ST0200000000000': 'AK', 'ST0400000000000': 'AZ',
    'ST0500000000000': 'AR', 'ST0600000000000': 'CA', 'ST0800000000000': 'CO',
    'ST0900000000000': 'CT', 'ST1000000000000': 'DE', 'ST1100000000000': 'DC',
    'ST1200000000000': 'FL', 'ST1300000000000': 'GA', 'ST1500000000000': 'HI',
    'ST1600000000000': 'ID', 'ST1700000000000': 'IL', 'ST1800000000000': 'IN',
    'ST1900000000000': 'IA', 'ST2000000000000': 'KS', 'ST2100000000000': 'KY',
    'ST2200000000000': 'LA', 'ST2300000000000': 'ME', 'ST2400000000000': 'MD',
    'ST2500000000000': 'MA', 'ST2600000000000': 'MI', 'ST2700000000000': 'MN',
    'ST2800000000000': 'MS', 'ST2900000000000': 'MO', 'ST3000000000000': 'MT',
    'ST3100000000000': 'NE', 'ST3200000000000': 'NV', 'ST3300000000000': 'NH',
    'ST3400000000000': 'NJ', 'ST3500000000000': 'NM', 'ST3600000000000': 'NY',
    'ST3700000000000': 'NC', 'ST3800000000000': 'ND', 'ST3900000000000': 'OH',
    'ST4000000000000': 'OK', 'ST4100000000000': 'OR', 'ST4200000000000': 'PA',
    'ST4400000000000': 'RI', 'ST4500000000000': 'SC', 'ST4600000000000': 'SD',
    'ST4700000000000': 'TN', 'ST4800000000000': 'TX', 'ST4900000000000': 'UT',
    'ST5000000000000': 'VT', 'ST5100000000000': 'VA', 'ST5300000000000': 'WA',
    'ST5400000000000': 'WV', 'ST5500000000000': 'WI', 'ST5600000000000': 'WY',
    'ST7200000000000': 'PR',
  };
  return stateMap[areaCode] || null;
}

// US map bounds (continental US focused)
export const US_BOUNDS: [[number, number], [number, number]] = [
  [24.0, -125.0], // Southwest corner
  [50.0, -66.0]   // Northeast corner
];
