//
// Copyright (c) 2025 Ruben Perez Hidalgo (rubenperez038 at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <boost/assert.hpp>
#include <boost/endian/conversion.hpp>
#include <boost/system/error_code.hpp>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <span>
#include <chrono>
#include <iostream>
#include <locale>
#include <sstream>
#include <cstring>

#include "nativepg/client_errc.hpp"
#include "nativepg/detail/field_traits.hpp"
#include "nativepg/response.hpp"

using namespace nativepg;
using boost::system::error_code;

// Parsing concrete fields
namespace {


template <class T>
error_code parse_text_int(std::span<const unsigned char> from, T& to)
{
    const char* first = reinterpret_cast<const char*>(from.data());
    const char* last = first + from.size();
    auto err = std::from_chars(first, last, to);
    if (err.ec != std::errc{})
        return std::make_error_code(err.ec);
    if (err.ptr != last)
        return client_errc::extra_bytes;
    return error_code();
}

template <class T>
error_code parse_binary_int(std::span<const unsigned char> from, T& to)
{
    if (from.size() != sizeof(T))
        return client_errc::protocol_value_error;
    to = boost::endian::endian_load<T, sizeof(T), boost::endian::order::big>(from.data());
    return {};
}

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
    if (from.size() < 8)
        // Postgresql delivers wrong format
        return client_errc::protocol_value_error;

    const char* first = reinterpret_cast<const char*>(from.data());
    const char* last = first + from.size();

    auto pos = first;

    // Parse HH
    int hours{};
    auto err = std::from_chars(pos, pos + 2, hours);
    if (err.ec != std::errc{})
        return std::make_error_code(err.ec);
    else
        pos += 2;

    // Validate and Skip :
    if (*pos != ':')
        return client_errc::protocol_value_error;
    else
        pos++;

    // Parse MM
    int minutes{};
    err = std::from_chars(pos, pos + 2, minutes);
    if (err.ec != std::errc{})
        return std::make_error_code(err.ec);
    else
        pos += 2;

    // Validate and Skip :
    if (*pos != ':')
        return client_errc::protocol_value_error;
    else
        pos++;

    // Parse SS
    int seconds{};
    err = std::from_chars(pos, pos + 2, seconds);
    if (err.ec != std::errc{})
        return std::make_error_code(err.ec);
    else
        pos += 2;

    int us = 0;
    // fraction part is optional
    if ((last - pos) > 1 )
    {
        // Validate and Skip .
        if (*pos != '.')
            return client_errc::protocol_value_error;
        else
            pos++;

        // Parse rest as fraction (Optional)
        int fraction = 0;
        err = std::from_chars(pos, last, fraction);
        if (err.ec != std::errc{})
            return std::make_error_code(err.ec);
        int n = last - pos; // number of digits

        // Scale faster without std::pow
        int scale = 1000000;
        for (int i = 0; i < n; ++i) scale /= 10;
        us = fraction * scale;
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

}  // namespace

boost::system::error_code detail::field_parse<std::int16_t>::call(
    std::optional<std::span<const unsigned char>> from,
    const protocol::field_description& desc,
    std::int16_t& to
)
{
    if (!from.has_value())
        return client_errc::unexpected_null;
    BOOST_ASSERT(desc.type_oid == int2_oid);
    return desc.fmt_code == protocol::format_code::text ? parse_text_int(*from, to)
                                                        : parse_binary_int(*from, to);
}

boost::system::error_code detail::field_parse<std::int32_t>::call(
    std::optional<std::span<const unsigned char>> from,
    const protocol::field_description& desc,
    std::int32_t& to
)
{
    if (!from.has_value())
        return client_errc::unexpected_null;
    auto data = *from;
    switch (desc.type_oid)
    {
        case int2_oid:
        {
            std::int16_t value{};
            auto ec = desc.fmt_code == protocol::format_code::text ? parse_text_int(data, value)
                                                                   : parse_binary_int(data, value);
            to = value;
            return ec;
        }
        case int4_oid:
            return desc.fmt_code == protocol::format_code::text ? parse_text_int(data, to)
                                                                : parse_binary_int(data, to);
        default: BOOST_ASSERT(false); return {};
    }
}

boost::system::error_code detail::field_parse<std::int64_t>::call(
    std::optional<std::span<const unsigned char>> from,
    const protocol::field_description& desc,
    std::int64_t& to
)
{
    if (!from.has_value())
        return client_errc::unexpected_null;
    auto data = *from;
    switch (desc.type_oid)
    {
        case int2_oid:
        {
            std::int16_t value{};
            auto ec = desc.fmt_code == protocol::format_code::text ? parse_text_int(data, value)
                                                                   : parse_binary_int(data, value);
            to = value;
            return ec;
        }
        case int4_oid:
        {
            std::int32_t value{};
            auto ec = desc.fmt_code == protocol::format_code::text ? parse_text_int(data, value)
                                                                   : parse_binary_int(data, value);
            to = value;
            return ec;
        }
        case int8_oid:
            return desc.fmt_code == protocol::format_code::text ? parse_text_int(data, to)
                                                                : parse_binary_int(data, to);
        default: BOOST_ASSERT(false); return {};
    }
}


// DATE => std::chrono::sys_days
boost::system::error_code detail::field_parse<std::chrono::sys_days>::call(
    std::optional<std::span<const unsigned char>> from,
    const protocol::field_description& desc,
    std::chrono::sys_days& to
)
{
    if (!from.has_value())
        return client_errc::unexpected_null;
    BOOST_ASSERT(desc.type_oid == 1082);
    return desc.fmt_code == protocol::format_code::text ?
        parse_text_date(*from, to) :
        parse_binary_date(*from, to);
}

// TIME => std::chrono::microseconds
boost::system::error_code detail::field_parse<std::chrono::microseconds>::call(
    std::optional<std::span<const unsigned char>> from,
    const protocol::field_description& desc,
    std::chrono::microseconds& to
)
{
    if (!from.has_value())
        return client_errc::unexpected_null;
    BOOST_ASSERT(desc.type_oid == 1083);
    return desc.fmt_code == protocol::format_code::text ?
        parse_text_time(*from, to) :
        parse_binary_time(*from, to);
}

// TIMETZ => pg_timetz
boost::system::error_code detail::field_parse<types::pg_timetz>::call(
    std::optional<std::span<const unsigned char>> from,
    const protocol::field_description& desc,
    types::pg_timetz& to
)
{
    if (!from.has_value())
        return client_errc::unexpected_null;
    BOOST_ASSERT(desc.type_oid == 1266);
    return desc.fmt_code == protocol::format_code::text ?
        parse_text_timetz(*from, to) :
        parse_binary_timetz(*from, to);
}

// TIMESTAMP => pg_timestamp
boost::system::error_code detail::field_parse<types::pg_timestamp>::call(
    std::optional<std::span<const unsigned char>> from,
    const protocol::field_description& desc,
    types::pg_timestamp& to
)
{
    if (!from.has_value())
        return client_errc::unexpected_null;
    BOOST_ASSERT(desc.type_oid == 1114);
    return desc.fmt_code == protocol::format_code::text ?
        parse_text_timestamp(*from, to) :
        parse_binary_timestamp(*from, to);
}

// TIMESTAMPTZ => pg_timestamptz
boost::system::error_code detail::field_parse<types::pg_timestamptz>::call(
    std::optional<std::span<const unsigned char>> from,
    const protocol::field_description& desc,
    types::pg_timestamptz& to
)
{
    if (!from.has_value())
        return client_errc::unexpected_null;
    BOOST_ASSERT(desc.type_oid == 1184);
    return desc.fmt_code == protocol::format_code::text ?
        parse_text_timestamptz(*from, to) :
        parse_binary_timestamptz(*from, to);
}

boost::system::error_code detail::compute_pos_map(
    const protocol::row_description& meta,
    std::span<const std::string_view> name_table,
    std::span<pos_map_entry> output
)
{
    // Name table should be the same size as the pos map
    BOOST_ASSERT(name_table.size() == output.size());

    // Set all positions to "invalid"
    for (auto& elm : output)
        elm = {invalid_pos, {}};

    // Look up every DB field in the name table
    std::size_t db_index = 0u;
    for (const auto& field : meta.field_descriptions)
    {
        auto it = std::find(name_table.begin(), name_table.end(), field.name);
        if (it != name_table.end())
        {
            auto cpp_index = static_cast<std::size_t>(it - name_table.begin());
            output[cpp_index] = {db_index, field};
        }
        ++db_index;
    }

    // If there is any unmapped field, it is an error
    if (std::find_if(output.begin(), output.end(), [](const pos_map_entry& ent) {
            return ent.db_index == invalid_pos;
        }) != output.end())
    {
        return client_errc::field_not_found;
    }

    return {};
}

handler_setup_result detail::resultset_setup(const request& req, std::size_t offset)
{
    const auto msgs = req.messages().subspan(offset);
    bool describe_found = false, execute_found = false;
    auto it = msgs.begin();

    // Skip any leading syncs
    while (it != msgs.end() && (*it == request_message_type::sync || *it == request_message_type::flush))
        ++it;

    // The original message may be a query. In this case, it must be the only message
    if (*it == request_message_type::query)
    {
        ++it;
        return {static_cast<std::size_t>(it - req.messages().begin())};
    }

    // Otherwise, it must be an extended query sequence:
    //   optional parse
    //   optional bind
    //   exactly one describe portal
    //   exactly one execute
    // There may be flush messages, but no sync messages in between
    //   (otherwise, error behavior becomes unreliable)
    for (; it != msgs.end() && !execute_found; ++it)
    {
        switch (*it)
        {
            // Ignore parse, bind and flush messages
            case request_message_type::sync: continue;
            case request_message_type::flush:
            case request_message_type::parse:
            case request_message_type::bind: continue;
            case request_message_type::describe:
                if (describe_found)
                    return handler_setup_result(client_errc::incompatible_response_type);
                else
                    describe_found = true;
                break;
            case request_message_type::execute:
                if (!describe_found || execute_found)
                    return handler_setup_result(client_errc::incompatible_response_type);
                else
                    execute_found = true;
                break;
            default: return handler_setup_result(client_errc::incompatible_response_type);
        }
    }

    // Skip any further sync messages
    while (it != msgs.end() && (*it == request_message_type::sync || *it == request_message_type::flush))
        ++it;

    // If we got the execute message, we're good
    return execute_found ? handler_setup_result{static_cast<std::size_t>(it - req.messages().begin())}
                         : handler_setup_result{client_errc::incompatible_response_type};
}
