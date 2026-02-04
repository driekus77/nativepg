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

struct empty {};
BOOST_DESCRIBE_STRUCT(empty, (), ())

struct myrow
{
    std::int64_t    id;
    std::string     name;
    std::chrono::sys_time<std::chrono::milliseconds> t;
};
BOOST_DESCRIBE_STRUCT(myrow, (), (id, name, t))

static void print_err(const char* prefix, const extended_error& err)
{
    std::cout << prefix << err.code.what() << ": " << err.diag.message() << '\n';
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

    // Compose our request
    request req;
    req.add_query("CREATE TABLE IF NOT EXISTS mdtt ( id bigserial primary key, name text not null, t time )", {});
    req.add_query("INSERT INTO  mdtt (name, t) VALUES ('birthday', now())", {});
    req.add_query("SELECT id, name, t FROM mdtt WHERE name = 'birthday'", {});
    req.add_query("DROP TABLE IF EXISTS mdtt", {});

    // Structures to parse the response into
    std::vector<empty> ignore_;
    std::vector<myrow> select_vec;
    response res{
        into(ignore_),
        into(ignore_),
        into(select_vec),
        into(ignore_)
    };

    auto [err] = co_await conn.async_exec(req, res, asio::as_tuple);
    if (err.code != boost::system::errc::success)
    {
        print_err("Operation result: ", err);
    }
    else
    {
        for (auto& row : select_vec)
        {
            std::cout << "Select result: " << std::format("{0:%T}", row.t) << std::endl;
        }
    }

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