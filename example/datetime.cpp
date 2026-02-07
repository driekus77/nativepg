//
// Copyright (c) 2025 Ruben Perez Hidalgo (rubenperez038 at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/describe/class.hpp>

#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <string_view>
#include <vector>
#include <chrono>
#include <format>

#include "nativepg/connection.hpp"
#include "nativepg/extended_error.hpp"
#include "nativepg/request.hpp"
#include "nativepg/response.hpp"

namespace asio = boost::asio;
using namespace nativepg;

struct time_row
{
    std::chrono::microseconds t;
};
BOOST_DESCRIBE_STRUCT(time_row, (), (t))

static void print_err(const char* prefix, const extended_error& err)
{
    std::cout << prefix << err.code.what() << ": " << err.diag.message() << '\n';
}

static asio::awaitable<void> test_text_format_time_parsing(connection& conn)
{
    // Start timing this method
    auto start = std::chrono::high_resolution_clock::now();

    // Compose our request
    request req;
    req.add_query("SELECT TIME '12:32:06.342156' as t", {});

    // Structures to parse the response into
    std::vector<time_row> select_vec;
    response res{into(select_vec)};

    auto [err] = co_await conn.async_exec(req, res, asio::as_tuple);

    // Finish timing this method
    auto finish = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(finish - start);

    // Print results
    if (err.extended_error::code != boost::system::errc::success)
        std::cerr << "Operation with text format results in Error: " << err.code.what() << ": " << err.diag.message() << " (in " << duration << ")" << std::endl;
    else
        std::cout << "Operation with text format select result: " << std::format("{0:%T}", select_vec[0].t) << " (in " << duration << ")" << std::endl;
}


static asio::awaitable<void> test_binary_format_time_parsing(connection& conn)
{
    // Start timing this operation
    auto start = std::chrono::high_resolution_clock::now();

    // Compose our request
    statement<std::chrono::microseconds> stmt{"bintest"};
    request req;
    req.add_prepare("SELECT $1::text::time as t", {"bintest"} )
        .add_execute("bintest", {"12:34:23.43535"}, request::param_format::text, protocol::format_code::binary, 1);

    // Structures to parse the response into
    std::vector<time_row> time_vec;
    response res{into(time_vec)};

    auto [err] = co_await conn.async_exec(req, res, asio::as_tuple);

    // Finish timing this method
    auto finish = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(finish - start);

    // Print results
    if (err.extended_error::code != boost::system::errc::success)
        std::cerr << "Operation with binary format results in Error: " << err.code.what() << ": " << err.diag.message() << " (in " << duration << ")" << std::endl;
    else
        std::cout << "Operation with binary format select result: " << std::format("{0:%T}", time_vec[0].t) << " (in " << duration << ")" << std::endl;
}


static asio::awaitable<void> co_main()
{
    // Create a connection
    connection conn{co_await asio::this_coro::executor};

    // Connect
    co_await conn.async_connect(
        {.hostname = "localhost", .username = "henry", .password = "", .database = "postgres"}
    );
    std::cout << "Startup complete\n";

    co_await test_text_format_time_parsing(conn);

    co_await test_binary_format_time_parsing(conn);

    std::cout << "Done\n";
}

int main()
{
    asio::io_context ctx;

    asio::co_spawn(ctx, co_main(), [](std::exception_ptr exc) {
        if (exc)
            std::rethrow_exception(exc);
    });

    ctx.run();
}