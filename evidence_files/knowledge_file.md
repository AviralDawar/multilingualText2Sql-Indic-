# Knowledge File: IHDS 2005 Birth History (Coded Columns and Value Mappings)

This file is a compact reference for interpreting the coded birth-history tables. It focuses on what each header means, what coded values mean, and how to map geographic identifiers.

## Missing-Value Conventions

- In the original ICPSR codebook, some variables also use negative special codes such as `-1`, `-2`, `-3`, etc. These should be interpreted using the variable-specific maps below.


## Core Interpretation Rules

- `stateid` is a state code.
- `distid` is not globally unique by itself. To resolve a district label, use the combined district key `stateid * 100 + distid`. Example: state `3`, district `9` -> combined code `309` -> `Ludhiana`.
- `bh6` determines whether age-at-death variables are applicable:
  - `1` = child living with respondent
  - `2` = child living elsewhere
  - `3` = child dead
- `*c` variables are usually imputed or standardized date/age versions used by IHDS.
- `*f` variables are flag variables describing how the corresponding `*c` or date variable was obtained or imputed.
- `CMC` means Century Month Code. Convert to year-month as: `year = 1900 + floor((cmc-1)/12)`, `month = ((cmc-1) mod 12) + 1`.

## Column Meanings

- `stateid`: State code
- `distid`: District code within state
- `psuid`: PSU / village-neighborhood code
- `hhid`: Household ID
- `hhsplitid`: Split household ID
- `bh2`: Birth ID
- `bh3`: Birth: sex
- `bh4`: Birth: raw date field
- `bh5`: Birth: approximate age
- `bh6`: Birth: living status
- `bh7`: Birth: age at death
- `ro0`: Roster ID for the child
- `bh4c`: Imputed date of birth for child (CMC)
- `bh4f`: Flag for date of birth for child
- `bh7c`: Imputed age at death for child
- `bh7f`: Flag for age at death for child
- `cd2c`: Imputed date of interview (CMC)
- `ew6c`: Imputed date of birth for mother (CMC)
- `ew6f`: Flag for date of birth for mother
- `ew5c`: Imputed age for mother
- `mh1bc`: Imputed date of marriage (CMC)
- `mh1bf`: Flag for date of marriage
- `mh1ac`: Imputed age at marriage
- `mh2bc`: Imputed date of gauna (CMC)
- `mh2bf`: Flag for date of gauna
- `mh2ac`: Imputed age at gauna
- `mh18bc`: Imputed date of first marriage (CMC)
- `mh18bf`: Flag for date of first marriage
- `mh18ac`: Imputed age at first marriage
- `mh19bc`: Imputed date of first gauna (CMC)
- `mh19bf`: Flag for date of first gauna
- `mh19ac`: Imputed age at first gauna
- `imp_bh5`: Audit flag: bh5 was synthetically filled
- `imp_ro0`: Audit flag: ro0 was synthetically filled
- `imp_bh7`: Audit flag: bh7 was synthetically filled
- `imp_bh7c`: Audit flag: bh7c was synthetically filled
- `imp_bh7f`: Audit flag: bh7f was synthetically filled
- `imp_note`: Audit note for imputation strategy

## Value Mappings by Column

### `bh3`

- `1` -> Boy
- `2` -> Girl

### `bh4`

- `-7` -> Inconsistent
- `-6` -> Out of range
- `-5` -> Valid skip
- `-4` -> Invalid skip
- `-3` -> Don't know
- `-2` -> Valid skip
- `-1` -> Valid blank

### `bh6`

- `1` -> Living with respondent
- `2` -> Living elsewhere
- `3` -> Dead

### `bh4f`

- `1` -> Month and year
- `2` -> Month and age - year imputed
- `3` -> Year and age - month imputed
- `4` -> Year and age - year ignored
- `5` -> Year - age, month imputed
- `6` -> Age - year, month imputed
- `7` -> Month - age, year imputed
- `8` -> None - all imputed

### `bh7f`

- `0` -> No flag
- `1` -> Age at death exceed date of interview
- `5` -> Age at death < last vaccination

### `ew6f`

- `1` -> Month and year
- `2` -> Month and age - year imputed
- `3` -> Year and age - month imputed
- `4` -> Year and age - year ignored
- `5` -> Year - age, month imputed
- `6` -> Age - year, month imputed
- `7` -> Month - age, year imputed
- `8` -> None - all imputed

### `mh1bf`

- `1` -> Month and year
- `2` -> Month and age - year imputed
- `3` -> Year and age - month imputed
- `4` -> Year and age - year ignored
- `5` -> Year - age, month imputed
- `6` -> Age - year, month imputed
- `7` -> Month - age, year imputed
- `8` -> None - all imputed

### `mh2bf`

- `1` -> Month and year
- `2` -> Month and age - year imputed
- `3` -> Year and age - month imputed
- `4` -> Year and age - year ignored
- `5` -> Year - age, month imputed
- `6` -> Age - year, month imputed
- `7` -> Month - age, year imputed
- `8` -> None - all imputed

### `mh18bf`

- `1` -> Month and year
- `2` -> Month and age - year imputed
- `3` -> Year and age - month imputed
- `4` -> Year and age - year ignored
- `5` -> Year - age, month imputed
- `6` -> Age - year, month imputed
- `7` -> Month - age, year imputed
- `8` -> None - all imputed

### `mh19bf`

- `1` -> Month and year
- `2` -> Month and age - year imputed
- `3` -> Year and age - month imputed
- `4` -> Year and age - year ignored
- `5` -> Year - age, month imputed
- `6` -> Age - year, month imputed
- `7` -> Month - age, year imputed
- `8` -> None - all imputed

### `imp_bh5`

- `0` -> No
- `1` -> Yes

### `imp_ro0`

- `0` -> No
- `1` -> Yes

### `imp_bh7`

- `0` -> No
- `1` -> Yes

### `imp_bh7c`

- `0` -> No
- `1` -> Yes

### `imp_bh7f`

- `0` -> No
- `1` -> Yes


## State Mapping (`stateid`)

- `1` -> Jammu & Kashmir
- `2` -> Himachal Pradesh
- `3` -> Punjab
- `4` -> Chandigarh
- `5` -> Uttaranchal
- `6` -> Haryana
- `7` -> Delhi
- `8` -> Rajasthan
- `9` -> Uttar Pradesh
- `10` -> Bihar
- `11` -> Sikkim
- `12` -> Arunachal Pradesh
- `13` -> Nagaland
- `14` -> Manipur
- `15` -> Mizoram
- `16` -> Tripura
- `17` -> Meghalaya
- `18` -> Assam
- `19` -> West Bengal
- `20` -> Jharkhand
- `21` -> Orissa
- `22` -> Chhatishgarh
- `23` -> Madhya Pradesh
- `24` -> Gujarat
- `25` -> Daman & Diu
- `26` -> Dadra+Nagar Haveli
- `27` -> Maharashtra
- `28` -> Andhra Pradesh
- `29` -> Karnataka
- `30` -> Goa
- `31` -> Lakshadweep
- `32` -> Kerala
- `33` -> Tamil Nadu
- `34` -> Pondicherry

## District Mapping Rule (`distid`)

Use `district_combined_code = stateid * 100 + distid`, then map via the district lookup.

Examples:
- `3` + `9` -> `309` -> `Ludhiana`
- `33` + `1` -> `3301` -> `Thiruvallur`
- `34` + `0` may still remain unresolved in the local lookup

## District Mapping Table (combined key -> district name)

- `102` -> Baramula
- `103` -> Srinagar
- `105` -> Pulwama
- `112` -> Rajauri
- `113` -> Jammu
- `201` -> Chamba
- `202` -> Kangra
- `204` -> Kullu
- `205` -> Mandi
- `206` -> Hamirpur
- `208` -> Bilaspur
- `209` -> Solan
- `210` -> Sirmaur
- `211` -> Shimla
- `301` -> Gurdaspur
- `302` -> Amritsar
- `303` -> Kapurthala
- `304` -> Jalandhar
- `305` -> Hoshiarpur
- `307` -> Rupnagar
- `308` -> Fatehgarh
- `309` -> Ludhiana
- `310` -> Moga
- `311` -> Firozpur
- `316` -> Sangrur
- `317` -> Patiala
- `400` -> Chandigarh
- `505` -> Dehradun
- `508` -> Bageshwar
- `509` -> Almora
- `511` -> Nainital
- `512` -> Udham Singh Nagar
- `513` -> Hardwar
- `601` -> Panchkula
- `602` -> Ambala
- `603` -> Yamunanagar
- `604` -> Kurukshetra
- `605` -> Kaithal
- `606` -> Karnal
- `608` -> Sonipat
- `609` -> Jind
- `610` -> Fatehabad
- `612` -> Hissar
- `613` -> Bhiwani
- `617` -> Rewari
- `618` -> Gurgaon
- `619` -> Faridabad
- `701` -> North West
- `702` -> North
- `703` -> North East
- `704` -> East
- `707` -> West
- `708` -> South West
- `709` -> South
- `710` -> Delhi Municipal Corp
- `711` -> New Delhi Municipal C
- `712` -> Kirari Suleman Nagar
- `803` -> Bikaner
- `804` -> Churu
- `806` -> Alwar
- `807` -> Bharatpur
- `808` -> Dhaulpur
- `809` -> Karauli
- `810` -> Sawai Madhopur
- `811` -> Dausa
- `812` -> Jaipur
- `813` -> Sikar
- `814` -> Nagaur
- `815` -> Jodhpur
- `818` -> Jalor
- `820` -> Pali
- `821` -> Ajmer
- `824` -> Bhilwara
- `825` -> Rajsamand
- `826` -> Udaipur
- `829` -> Chittaurgarh
- `830` -> Kota
- `831` -> Baran
- `832` -> Jhalawar
- `901` -> Saharanpur
- `902` -> Muzaffarnagar
- `903` -> Bijnor
- `904` -> Moradabad
- `905` -> Rampur
- `906` -> Jyotiva Phule Nagar
- `907` -> Meerut
- `909` -> Ghaziabad
- `910` -> Gautam Buddha Nagar
- `913` -> Hathras
- `914` -> Mathura
- `915` -> Agra
- `919` -> Budaun
- `920` -> Bareilly
- `923` -> Kheri
- `924` -> Sitapur
- `928` -> Lucknow
- `929` -> Farrukabad
- `930` -> Kannauj
- `933` -> Kanpur Dehat
- `936` -> Jhansi
- `940` -> Banda
- `941` -> Chitrakoot
- `942` -> Fatehpur
- `944` -> Kaushambi
- `945` -> Allahabad
- `946` -> Barabanki
- `947` -> Faizabad
- `948` -> Ambedkar Nagar
- `949` -> Sultanpur
- `950` -> Bahraich
- `951` -> Sharawasti
- `954` -> Siddharathnagar
- `958` -> Gorakhpur
- `959` -> Kushinagar
- `960` -> Deoria
- `962` -> Mau
- `963` -> Baliya
- `965` -> Ghazipur
- `966` -> Chandauli
- `967` -> Varanasi
- `968` -> Sant Ravidas Nagar
- `1002` -> Purbi Champaran
- `1005` -> Madhubani
- `1006` -> Supaul
- `1009` -> Purnia
- `1012` -> Saharsa
- `1014` -> Muzaffar Pur
- `1016` -> Siwan
- `1017` -> Saran
- `1022` -> Bhagal Pur
- `1023` -> Banka
- `1026` -> Sheikhpura
- `1027` -> Nalanda
- `1028` -> Patna
- `1030` -> Buxar
- `1031` -> Kaimur (Bhabua)
- `1032` -> Rohtas
- `1035` -> Gaya
- `1100` -> Sikkim
- `1300` -> Nagaland
- `1400` -> Manipur
- `1500` -> Mizoram
- `1600` -> Tripura
- `1700` -> Meghalaya
- `1802` -> Dhubri
- `1803` -> Goalpara
- `1806` -> Kamrup
- `1809` -> Marigaon
- `1814` -> Tinsukia
- `1817` -> Jorhat
- `1819` -> Karbi Amglong
- `1821` -> Cachar
- `1901` -> Darjiling
- `1902` -> Jalapiguri
- `1904` -> Uttar Dinajpur
- `1906` -> Maldah
- `1907` -> Murshidabad
- `1908` -> Birbhum
- `1909` -> Barddhaman
- `1910` -> Nadia
- `1911` -> North 24 Parganas
- `1912` -> Hugli
- `1913` -> Bankura
- `1916` -> Haora
- `1917` -> Kolkata
- `1918` -> South 24 Parganas
- `2002` -> Palamu
- `2012` -> Dhanbad
- `2013` -> Bokaro
- `2014` -> Ranchi
- `2017` -> Pashchimi Singbhum
- `2018` -> Purbi Singhbhum
- `2101` -> Bargarh
- `2102` -> Jharsuguda
- `2103` -> Sambalpur
- `2105` -> Sundargarh
- `2106` -> Kendujhar
- `2107` -> Mayurbhanj
- `2108` -> Baleshwar
- `2112` -> Cuttack
- `2113` -> Jajapur
- `2114` -> Dhenkanal
- `2115` -> Anugul
- `2116` -> Nayagarh
- `2117` -> Khordha
- `2118` -> Puri
- `2119` -> Ganjam
- `2120` -> Gajapati
- `2121` -> Kandhamal
- `2122` -> Baudh
- `2123` -> Sonapur
- `2124` -> Balangir
- `2126` -> Kalahandi
- `2127` -> Rayagada
- `2128` -> Nabarangapur
- `2129` -> Koraput
- `2130` -> Malkangiri
- `2201` -> Koriya
- `2202` -> Sarguja
- `2203` -> Jashpur
- `2204` -> Raigarh
- `2205` -> Korba
- `2206` -> Janjgir
- `2207` -> Bilas Pur
- `2208` -> Kawardha
- `2209` -> Rajnandgaon
- `2210` -> Durg
- `2211` -> Raipur
- `2212` -> Mahasamund
- `2213` -> Dhamtari
- `2214` -> Kanker
- `2215` -> Bastar
- `2301` -> Sheopur
- `2302` -> Morena
- `2304` -> Gwalior
- `2305` -> Datia
- `2308` -> Tikamgarh
- `2309` -> Chhatarpur
- `2310` -> Panna
- `2313` -> Satna
- `2315` -> Umaria
- `2316` -> Shahdol
- `2317` -> Sidhi
- `2320` -> Ratlam
- `2321` -> Ujjain
- `2322` -> Shajapur
- `2323` -> Dewas
- `2325` -> Dhar
- `2326` -> Indore
- `2327` -> West Nimar
- `2328` -> Barwani
- `2329` -> East Nimar
- `2330` -> Rajgarh
- `2332` -> Bhopal
- `2335` -> Betul
- `2336` -> Harda
- `2337` -> Hoshangabad
- `2338` -> Katni
- `2339` -> Jabalpur
- `2341` -> Dindori
- `2342` -> Mandla
- `2344` -> Seoni
- `2401` -> Kachchh
- `2403` -> Patan
- `2404` -> Mahesana
- `2406` -> Gandhinagar
- `2407` -> Ahmedabad
- `2408` -> Surendranagar
- `2409` -> Rajkot
- `2410` -> Jamnagar
- `2412` -> Junagadh
- `2413` -> Amreli
- `2414` -> Bhavnagar
- `2415` -> Anand
- `2416` -> Kheda
- `2419` -> Vadodara
- `2420` -> Narmada
- `2421` -> Bharuch
- `2422` -> Surat
- `2600` -> Dadra & Nagar Haveli
- `2701` -> Nandurbar
- `2702` -> Dhule
- `2703` -> Jalgaon
- `2705` -> Akola
- `2706` -> Washim
- `2707` -> Amarawti
- `2708` -> Wardha
- `2709` -> Nagpur
- `2710` -> Bhandara
- `2711` -> Gondiya
- `2713` -> Chandrapur
- `2714` -> Yavatmal
- `2715` -> Nanded
- `2716` -> Hingoli
- `2717` -> Parbhani
- `2718` -> Jalna
- `2720` -> Nasik
- `2721` -> Thane
- `2723` -> Mumbai
- `2725` -> Pune
- `2726` -> Ahmadnagar
- `2727` -> Bid
- `2729` -> Osmanabad
- `2730` -> Solapur
- `2731` -> Satara
- `2732` -> Ratnagiri
- `2734` -> Kolhapur
- `2801` -> Adilabad
- `2802` -> Nizamabad
- `2803` -> Karimnagar
- `2804` -> Medak
- `2805` -> Hyderabad
- `2806` -> Rangareddi
- `2807` -> Mahbubnagar
- `2810` -> Khammam
- `2813` -> Visakhapatnam
- `2814` -> East Godavari
- `2815` -> West Godavari
- `2816` -> Krishna
- `2818` -> Prakasam
- `2819` -> Nellore
- `2820` -> Cuddapah
- `2821` -> Kurnool
- `2822` -> Anantapur
- `2823` -> Chittoor
- `2901` -> Belgaum
- `2902` -> Bagalkot
- `2903` -> Bijapur
- `2905` -> Bidar
- `2906` -> Raichur
- `2907` -> Koppal
- `2908` -> Gadag
- `2909` -> Dharwad
- `2910` -> Uttar Kannad
- `2911` -> Haveri
- `2912` -> Bellary
- `2913` -> Chitradurga
- `2914` -> Davanagere
- `2915` -> Shimoga
- `2916` -> Udupi
- `2917` -> Chikmagalur
- `2918` -> Tumkur
- `2919` -> Kolar
- `2920` -> Bangalore
- `2921` -> Bangalore Rural
- `2922` -> Mandya
- `2924` -> Dakshin Kannada
- `2925` -> Kodagu
- `2926` -> Mysore
- `2927` -> Chamarajanagar
- `3001` -> North Goa
- `3002` -> South Goa
- `3202` -> Kannur
- `3204` -> Kozhikode
- `3205` -> Malappuram
- `3206` -> Palakkad
- `3207` -> Thrissur
- `3208` -> Ernakulam
- `3209` -> Idukki
- `3211` -> Alappuzha
- `3212` -> Pathanamthitta
- `3213` -> Kollam
- `3214` -> Thiruvananthapuram
- `3301` -> Thiruvallur
- `3302` -> Chennai
- `3303` -> Kancheepuram
- `3304` -> Vellore
- `3305` -> Dharampuri
- `3306` -> Tiruvannamalai
- `3309` -> Namakkal
- `3310` -> Erode
- `3312` -> Coimbatore
- `3313` -> Dindigul
- `3314` -> Karur
- `3315` -> Tiruchchirappalli
- `3316` -> Perambalur
- `3317` -> Ariyalur
- `3323` -> Sivaganga
- `3324` -> Madurai
- `3325` -> Theni
- `3327` -> Ramanathapuram
- `3328` -> Thoothukkudi
- `3329` -> Tirunelveli
- `3330` -> Kanniyakumari
- `3400` -> Pondicherry

## District Codes Without a Local Label

- `stateid=3`, `distid=6` -> unresolved locally (1 rows)
- `stateid=8`, `distid=5` -> unresolved locally (206 rows)
- `stateid=9`, `distid=34` -> unresolved locally (184 rows)
- `stateid=12`, `distid=0` -> unresolved locally (45 rows)
- `stateid=21`, `distid=9` -> unresolved locally (31 rows)
- `stateid=23`, `distid=12` -> unresolved locally (71 rows)
- `stateid=25`, `distid=0` -> unresolved locally (8 rows)
- `stateid=28`, `distid=17` -> unresolved locally (6 rows)
- `stateid=29`, `distid=29` -> unresolved locally (18 rows)
- `stateid=32`, `distid=10` -> unresolved locally (14 rows)

