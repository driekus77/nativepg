
#ifndef NATIVEPG_FIELD_TRAITS_DATETIME_HPP
#define NATIVEPG_FIELD_TRAITS_DATETIME_HPP

#include <chrono>

#include <boost/system/error_code.hpp>


namespace nativepg::detail {
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

template <class T>
struct field_is_compatible;

// Special type for capturing Postgresql timetz in C++20
struct pg_timetz {
    std::chrono::microseconds time_since_midnight;
    std::chrono::seconds utc_offset;
};

// DATE
template <>
struct field_is_compatible<std::chrono::sys_days>
{
    static inline boost::system::error_code call(const protocol::field_description& desc)
    {
        return desc.type_oid == 1082 ? boost::system::error_code() : client_errc::incompatible_field_type;
    }
};

// TIME
template <>
struct field_is_compatible<std::chrono::microseconds>
{
    static inline boost::system::error_code call(const protocol::field_description& desc)
    {
        return desc.type_oid == 1083 ? boost::system::error_code() : client_errc::incompatible_field_type;
    }
};

// TIMETZ
template <>
struct field_is_compatible<pg_timetz>
{
    static inline boost::system::error_code call(const protocol::field_description& desc)
    {
        return desc.type_oid == 1266 ? boost::system::error_code() : client_errc::incompatible_field_type;
    }
};


template <class T>
struct field_parse;

// DATE
template <>
struct field_parse<std::chrono::sys_days>
{
    static boost::system::error_code call(
        std::optional<std::span<const unsigned char>> from,
        const protocol::field_description& desc,
        std::chrono::sys_days& to
    );
};

// TIME
template <>
struct field_parse<std::chrono::microseconds>
{
    static boost::system::error_code call(
        std::optional<std::span<const unsigned char>> from,
        const protocol::field_description& desc,
        std::chrono::microseconds& to
    );
};

// TIMETZ
template <>
struct field_parse<pg_timetz>
{
    static boost::system::error_code call(
        std::optional<std::span<const unsigned char>> from,
        const protocol::field_description& desc,
        pg_timetz& to
    );
};

}
#endif  // NATIVEPG_FIELD_TRAITS_DATETIME_HPP
