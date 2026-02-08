
#ifndef NATIVEPG_TYPES_DATETIME_HPP
#define NATIVEPG_TYPES_DATETIME_HPP

#include <boost/endian/conversion.hpp>
#include <boost/system/error_code.hpp>

#include <chrono>

namespace nativepg::types {

using boost::system::error_code;


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



// DATE => std::chrono::sys_days. (TEXT)
template <class T = std::chrono::sys_days>
constexpr error_code parse_text_date(std::span<const unsigned char> from, T& to) noexcept
{
    if (from.size() != 10)
        return client_errc::protocol_value_error;

    const char* first = reinterpret_cast<const char*>(from.data());
    const char* pos = first;

    int year{}, month{}, day{};

    // Parse YYYY
    auto res = std::from_chars(pos, pos + 4, year);
    if (res.ec != std::errc{}) return std::make_error_code(res.ec);
    pos += 4;

    if (*pos != '-')
        return client_errc::protocol_value_error;
    ++pos;

    // Parse MM
    res = std::from_chars(pos, pos + 2, month);
    if (res.ec != std::errc{}) return std::make_error_code(res.ec);
    pos += 2;

    if (*pos != '-')
        return client_errc::protocol_value_error;
    ++pos;

    // Parse DD
    res = std::from_chars(pos, pos + 2, day);
    if (res.ec != std::errc{}) return std::make_error_code(res.ec);

    // Specify year, month, day
    std::chrono::year y{year};
    std::chrono::month m{static_cast<unsigned>(month)};
    std::chrono::day d{static_cast<unsigned>(day)};

    // Compose year_month_day
    std::chrono::year_month_day ymd{y, m, d};

    to = std::chrono::sys_days{ymd};

    return error_code{};
}

// DATE => std::chrono::sys_days. (BINARY)
template <class T = std::chrono::sys_days>
constexpr error_code parse_binary_date(std::span<const unsigned char> from, T& to) noexcept
{
    if (from.size() != 4)
        return client_errc::protocol_value_error;

    // Load big-endian int32 directly
    int32_t days_since_2000 = boost::endian::endian_load<int32_t, 4, boost::endian::order::big>(from.data());

    // PostgreSQL zero = 2000-01-01
    constexpr std::chrono::sys_days pg_epoch{std::chrono::year{2000}/1/1};

    to = pg_epoch + std::chrono::days{days_since_2000};

    return error_code{};
}

// TIME => std::chrono::microseconds (TEXT)
template <class T = std::chrono::microseconds>
constexpr error_code parse_text_time(std::span<const unsigned char> from, T& to) noexcept
{
    if (from.empty())
        return client_errc::protocol_value_error;

    const char* first = reinterpret_cast<const char*>(from.data());
    const char* last = first + from.size();

    auto pos = first;

    // Parse HH
    int hours{};
    auto err = std::from_chars(pos, last, hours);
    if (err.ec != std::errc{})
        return std::make_error_code(err.ec);
    pos = err.ptr;

    // Validate and Skip :
    if (pos >= last || *pos != ':')
        return client_errc::protocol_value_error;
    pos++;

    // Parse MM
    int minutes{};
    err = std::from_chars(pos, last, minutes);
    if (err.ec != std::errc{})
        return std::make_error_code(err.ec);
    pos = err.ptr;

    // Validate and Skip :
    if (pos >= last || *pos != ':')
        return client_errc::protocol_value_error;
    pos++;

    // Parse SS
    int seconds{};
    err = std::from_chars(pos, last, seconds);
    if (err.ec != std::errc{})
        return std::make_error_code(err.ec);
    pos = err.ptr;

    int us = 0;
    // fraction part is optional
    if (pos < last && *pos == '.')
    {
        pos++;
        const char* start = pos;
        int fraction = 0;
        err = std::from_chars(pos, last, fraction);
        if (err.ec != std::errc{})
            return std::make_error_code(err.ec);
        int n = err.ptr - start; // number of digits
        pos = err.ptr;

        // Scale
        if (n > 6) {
            for (int i = 0; i < n - 6; ++i) fraction /= 10;
        } else {
            for (int i = 0; i < 6 - n; ++i) fraction *= 10;
        }
        us = fraction;
    }

    to = std::chrono::hours{hours} + std::chrono::minutes{minutes} + std::chrono::seconds{seconds} + std::chrono::microseconds{us};

    return error_code{};
}

// TIME => std::chrono::microseconds (BINARY).
template <class T = std::chrono::microseconds>
constexpr error_code parse_binary_time(std::span<const unsigned char> from, T& to) noexcept
{
    auto us = boost::endian::endian_load<int64_t, sizeof(int64_t), boost::endian::order::big>(from.data());

    to = std::chrono::microseconds{us};

    return error_code{};
}



// TIMETZ => pg_timetz (TEXT)
template <class T = types::pg_timetz>
constexpr error_code parse_text_timetz(std::span<const unsigned char> from, T& to) noexcept
{
    if (from.size() < 11)
        return client_errc::protocol_value_error;

    const char* first = reinterpret_cast<const char*>(from.data());
    const char* last = first + from.size();
    const char* dot = static_cast<const char*>(std::memchr(first, '.', from.size()));
    const char* sign = static_cast<const char*>(std::memchr(first, '+', from.size()));
    if (!sign)
        sign = static_cast<const char*>(std::memchr(first, '-', from.size()));
    const char* pos = first;

    // --- Parse HH ---
    int hours{};
    auto err = std::from_chars(pos, pos + 2, hours);
    if (err.ec != std::errc{}) return std::make_error_code(err.ec);
    pos += 2;

    if (*pos != ':') return client_errc::protocol_value_error;
    ++pos;

    // --- Parse MM ---
    int minutes{};
    err = std::from_chars(pos, pos + 2, minutes);
    if (err.ec != std::errc{}) return std::make_error_code(err.ec);
    pos += 2;

    if (*pos != ':') return client_errc::protocol_value_error;
    ++pos;

    // --- Parse SS ---
    int seconds{};
    err = std::from_chars(pos, pos + 2, seconds);
    if (err.ec != std::errc{}) return std::make_error_code(err.ec);
    pos += 2;

    // --- Parse fractional seconds (optional) ---
    int us = 0;
    if (dot && *pos == '.') {
        ++pos; // skip '.'

        const char* fraction_end = sign ? sign : last + 1;
        int fraction = 0;
        err = std::from_chars(pos, fraction_end, fraction);
        if (err.ec != std::errc{}) return std::make_error_code(err.ec);

        int n = fraction_end - pos;
        int scale = 1000000;
        for (int i = 0; i < n; ++i) scale /= 10;
        us = fraction * scale;

        pos += n;
    }

    // --- Parse UTC offset (±HH:MM) if present ---
    std::chrono::seconds offset{0};
    if (pos < last && sign) {
        if (*pos != '+' && *pos != '-') return client_errc::protocol_value_error;

        // skip sign
        int sign_factor = *sign == '+' ? 1 : -1;
        ++pos;

        // Parse HH
        int offset_h{};
        err = std::from_chars(pos, pos + 2, offset_h);
        if (err.ec != std::errc{}) return std::make_error_code(err.ec);
        pos += 2;

        // Parse MM
        int offset_m{};
        if (pos < last)
        {
            if (*pos != ':') return client_errc::protocol_value_error;
            ++pos;

            err = std::from_chars(pos, pos + 2, offset_m);
            if (err.ec != std::errc{}) return std::make_error_code(err.ec);
            pos += 2;
        }

        offset = std::chrono::hours{offset_h} + std::chrono::minutes{offset_m};
        offset *= sign_factor;
    }

    // --- Fill struct ---
    to = T{
        std::chrono::hours{hours} + std::chrono::minutes{minutes} +
        std::chrono::seconds{seconds} + std::chrono::microseconds{us},
        offset
    };

    return error_code{};
}

// TIMETZ => pg_timetz (BINARY)
template <class T = types::pg_timetz>
constexpr error_code parse_binary_timetz(std::span<const unsigned char> from, T& to) noexcept
{
    if (from.size() != 12) // 8 + 4 bytes
        return client_errc::protocol_value_error;

    const unsigned char* ptr = from.data();

    // --- Load time (int64) big endian ---
    std::int64_t time_us =
        boost::endian::endian_load<std::int64_t, 8, boost::endian::order::big>(ptr);

    // --- Load UTC offset (int32) big endian ---
    std::int32_t offset_west_s =
        boost::endian::endian_load<std::int32_t, 4, boost::endian::order::big>(ptr + 8);

    to.time_since_midnight = std::chrono::microseconds{time_us};

    // PostgreSQL stores seconds WEST of UTC → negate
    to.utc_offset = std::chrono::seconds{-offset_west_s};

    return error_code{};
}

// TIMESTAMP => pg_timestamp / std::chrono::local_time<microseconds> (TEXT)
template <class T = types::pg_timestamp>
constexpr error_code parse_text_timestamp(std::span<const unsigned char> from, T& to) noexcept
{
    if (from.size() < 19) // minimum: YYYY-MM-DD HH:MM:SS
        return client_errc::protocol_value_error;

    // Parse date part using the function above
    std::chrono::sys_days date;
    auto ec = parse_text_date(from.subspan(0, 10), date);
    if (ec) return ec;

    // Parse time part (reuse existing time parser)
    std::chrono::microseconds tod;
    ec = parse_text_time(from.subspan(11), tod);
    if (ec) return ec;

    to = T{date.time_since_epoch() + tod};
    return {};
}

// TIMESTAMP => pg_timestamp (BINARY)
template <class T = types::pg_timestamp>
constexpr error_code parse_binary_timestamp(
    std::span<const unsigned char> from, T& to) noexcept
{
    if (from.size() != 8)
        return client_errc::protocol_value_error;

    // Load big-endian int64: microseconds since 2000-01-01
    std::int64_t us_since_2000 =
        boost::endian::endian_load<std::int64_t, 8, boost::endian::order::big>(
            from.data());

    // PostgreSQL epoch
    constexpr std::chrono::sys_days pg_epoch{
        std::chrono::year{2000}/1/1
    };

    // Cast sys_days → microseconds duration
    auto us_since_epoch = std::chrono::time_point_cast<std::chrono::microseconds>(pg_epoch)
                          .time_since_epoch()
                        + std::chrono::microseconds{us_since_2000};

    to = T{us_since_epoch};

    return {};
}

// TIMESTAMPTZ => pg_timestamptz (TEXT)
template <class T = types::pg_timestamptz>
constexpr error_code parse_text_timestamptz(std::span<const unsigned char> from, T& to) noexcept
{
    if (from.size() < 19) // minimum: YYYY-MM-DD HH:MM:SS
        return client_errc::protocol_value_error;

    // Parse date part using the function above
    std::chrono::sys_days date;
    auto ec = parse_text_date(from.subspan(0, 10), date);
    if (ec) return ec;

    // Parse time part (reuse existing time parser)
    types::pg_timetz ts;
    ec = parse_text_timetz(from.subspan(11), ts);
    if (ec) return ec;

    to = T{date.time_since_epoch() + ts.time_since_midnight};

    return {};
}

// TIMESTAMPTZ => pg_timestamp (BINARY)
template <class T = types::pg_timestamptz>
constexpr error_code parse_binary_timestamptz(
    std::span<const unsigned char> from, T& to) noexcept
{
    if (from.size() != 8)
        return client_errc::protocol_value_error;

    // Load big-endian int64: microseconds since 2000-01-01
    std::int64_t us_since_2000 =
        boost::endian::endian_load<std::int64_t, 8, boost::endian::order::big>(
            from.data());

    // PostgreSQL epoch
    constexpr std::chrono::sys_days pg_epoch{
        std::chrono::year{2000}/1/1
    };

    // Cast sys_days → microseconds duration
    auto us_since_epoch = std::chrono::time_point_cast<std::chrono::microseconds>(pg_epoch)
                          .time_since_epoch()
                        + std::chrono::microseconds{us_since_2000};

    to = T{us_since_epoch};

    return {};
}


// INTERVAL  => pg_interval (TEXT)
template <class T = types::pg_interval>
constexpr error_code parse_text_interval(std::span<const unsigned char> from, T& to) noexcept
{
    using namespace std::chrono;

    to = {}; // zero everything

    if (from.empty()) return client_errc::protocol_value_error;

    std::string_view sv{reinterpret_cast<const char*>(from.data()), from.size()};
    size_t pos = 0;

    auto skip_ws = [&]() {
        while (pos < sv.size() && std::isspace(static_cast<unsigned char>(sv[pos]))) ++pos;
    };

    auto parse_int = [&](int& val) -> bool {
        skip_ws();
        if (pos >= sv.size()) return false;
        int sign = 1;
        if (sv[pos] == '-') { sign = -1; ++pos; }
        else if (sv[pos] == '+') { ++pos; }

        const char* start = sv.data() + pos;
        const char* end = sv.data() + sv.size();
        auto res = std::from_chars(start, end, val);
        if (res.ec != std::errc{}) return false;
        pos += res.ptr - start;
        val *= sign;
        return true;
    };

    while (pos < sv.size()) {
        skip_ws();
        if (pos >= sv.size()) break;

        // Check for time part HH:MM:SS (may have a sign)
        // A time part MUST have a colon at some point before the next space or end of string.
        size_t next_ws = sv.find_first_of(" \t\n\r", pos);
        std::string_view part = sv.substr(pos, next_ws == std::string_view::npos ? std::string_view::npos : next_ws - pos);

        if (part.find(':') != std::string_view::npos) {
            int sign = 1;
            if (sv[pos] == '-') { sign = -1; ++pos; }
            else if (sv[pos] == '+') { ++pos; }

            microseconds time_part;
            // The time part is what remains in 'part' after potentially skipping the sign
            size_t sign_offset = (sv[pos-1] == '-' || sv[pos-1] == '+') ? 1 : 0;
            auto ec = parse_text_time(std::span{reinterpret_cast<const unsigned char*>(sv.data() + pos), part.size() - sign_offset}, time_part);
            if (ec) return ec;

            to.time += time_part * sign;
            pos += part.size() - sign_offset;
        } else {
            // Otherwise parse "value unit"
            int val = 0;
            if (!parse_int(val)) return client_errc::protocol_value_error;

            skip_ws();
            size_t unit_start = pos;
            while (pos < sv.size() && std::isalpha(static_cast<unsigned char>(sv[pos]))) ++pos;
            std::string_view unit = sv.substr(unit_start, pos - unit_start);

            if (unit == "year" || unit == "years") to.months += val * 12;
            else if (unit == "mon" || unit == "mons") to.months += val;
            else if (unit == "day" || unit == "days") to.days += val;
            else if (unit == "hour" || unit == "hours") to.time += hours{val};
            else if (unit == "minute" || unit == "minutes") to.time += minutes{val};
            else if (unit == "second" || unit == "seconds") to.time += seconds{val};
            else return client_errc::protocol_value_error;
        }
        skip_ws();
    }

    return {};
}

// INTERVAL => pg_interval (BINARY)
template <class T = types::pg_interval>
constexpr error_code parse_binary_interval(
    std::span<const unsigned char> from, T& to) noexcept
{
    if (from.size() != 16)
        return client_errc::protocol_value_error;

    // PostgreSQL binary interval:
    // 8 bytes: time (microseconds, int64)
    // 4 bytes: days (int32)
    // 4 bytes: months (int32)
    to.time = std::chrono::microseconds{
        boost::endian::endian_load<std::int64_t, 8, boost::endian::order::big>(from.data())
    };
    to.days = boost::endian::endian_load<std::int32_t, 4, boost::endian::order::big>(from.data() + 8);
    to.months = boost::endian::endian_load<std::int32_t, 4, boost::endian::order::big>(from.data() + 12);

    return {};
}


}

#endif  // NATIVEPG_TYPES_DATETIME_HPP
