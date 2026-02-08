//
// Created by Henry Roeland on 08/02/2026.
//

#ifndef NATIVEPG_TYPES_DATETIME_HPP
#define NATIVEPG_TYPES_DATETIME_HPP

#include <chrono>

namespace nativepg::types {

/*
| Type        | Category | OID  | Storage size | Precision | Minimum value                  | Maximum value                  |
|-------------|----------|------|--------------|-----------|--------------------------------|--------------------------------|
| date        | base     | 1082 | 4 bytes      | 1 day     | 4713-01-01 BC                  | 5874897-12-31                  |
| time        | base     | 1083 | 8 bytes      | 1 µs      | 00:00:00                       | 24:00:00                       |
| timetz      | base     | 1266 | 12 bytes     | 1 µs      | 00:00:00-15:59                 | 24:00:00+15:59                 |
| timestamp   | base     | 1114 | 8 bytes      | 1 µs      | 4713-01-01 00:00:00 BC         | 294276-12-31 23:59:59.999999   |
| timestamptz | base     | 1184 | 8 bytes      | 1 µs      | 4713-01-01 00:00:00+00 BC      | 294276-12-31 23:59:59.999999+00|
| interval    | base     | 1186 | 16 bytes     | 1 µs      | -178000000 years               | 178000000 years                |
| tsrange     | range    | 3908 | variable     | 1 µs      | timestamp min                  | timestamp max                  |
| tstzrange   | range    | 3910 | variable     | 1 µs      | timestamptz min                | timestamptz max                |
| daterange   | range    | 3912 | variable     | 1 day     | date min                       | date max                       |
 */

// Type mapping
using pg_date = std::chrono::sys_days;

using pg_time = std::chrono::microseconds;
struct pg_timetz {
    std::chrono::microseconds time_since_midnight;
    std::chrono::seconds utc_offset;
};

using pg_timestamp = std::chrono::local_time<std::chrono::microseconds>;
using pg_timestamptz = std::chrono::sys_time<std::chrono::microseconds>;

struct pg_interval {
    int months;
    int days;
    std::chrono::microseconds time;
};

}

#endif  // NATIVEPG_TYPES_DATETIME_HPP
