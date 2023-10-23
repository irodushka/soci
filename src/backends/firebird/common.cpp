//
// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, Rafal Bobrowski
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#include "soci/soci-platform.h"
#include "firebird/common.h"
#include "soci/soci-backend.h"
#include "soci-compiler.h"
#include <cstddef>
#include <cstring>
#include <cstdio>
#include <sstream>
#include <iostream>
#include <string>

using namespace Firebird;

namespace soci
{

namespace details
{

namespace firebird
{

void tmEncode(unsigned type, std::tm * src, void * dst)
{
    switch (type)
    {
        // In Interbase v6 DATE represents a date-only data type,
        // in InterBase v5 DATE represents a date+time data type.
    case SQL_TIMESTAMP:
        isc_encode_timestamp(src, static_cast<ISC_TIMESTAMP*>(dst));
        break;
    case SQL_TYPE_TIME:
        isc_encode_sql_time(src, static_cast<ISC_TIME*>(dst));
        break;
    case SQL_TYPE_DATE:
        isc_encode_sql_date(src, static_cast<ISC_DATE*>(dst));
        break;
    default:
        std::ostringstream msg;
        msg << "Unexpected type of date/time field (" << type << ")";
        throw soci_error(msg.str());
    }
}

void tmDecode(unsigned type, void * src, std::tm * dst)
{
    switch (type)
    {
    case SQL_TIMESTAMP:
        isc_decode_timestamp(static_cast<ISC_TIMESTAMP*>(src), dst);
        break;
    case SQL_TYPE_TIME:
        isc_decode_sql_time(static_cast<ISC_TIME*>(src), dst);
        break;
    case SQL_TYPE_DATE:
        isc_decode_sql_date(static_cast<ISC_DATE*>(src), dst);
        break;
    default:
        std::ostringstream msg;
        msg << "Unexpected type of date/time field (" << type << ")";
        throw soci_error(msg.str());
    }
}

void setTextParam(char const * s, std::size_t size, unsigned char * sqlbuf, unsigned sqltype, unsigned sqllen, int sqlscale)
{
    if (sqltype == SQL_VARYING || sqltype == SQL_TEXT)
    {
        if (size > static_cast<std::size_t>(sqllen))
        {
            std::ostringstream msg;
            msg << "Value \"" << s << "\" is too long ("
                << size << " bytes) to be stored in column of size "
                << sqllen << " bytes";
            throw soci_error(msg.str());
        }

        unsigned short const sz = static_cast<unsigned short>(size);

        if (sqltype == SQL_VARYING)
        {
            std::memcpy(sqlbuf, &sz, sizeof(short));
            std::memcpy(sqlbuf + sizeof(short), s, sz);
        }
        else // sqltype == SQL_TEXT
        {
            std::memcpy(sqlbuf, s, sz);
            if (sz < sqllen)
            {
                std::memset(sqlbuf+sz, ' ', sqllen - sz);
            }
        }
    }
    else if (sqltype == SQL_SHORT)
    {
        parse_decimal<short, unsigned short>(sqlscale, sqltype, sqlbuf, s);
    }
    else if (sqltype == SQL_LONG)
    {
        parse_decimal<int, unsigned int>(sqlscale, sqltype, sqlbuf, s);
    }
    else if (sqltype == SQL_INT64)
    {
        parse_decimal<long long, unsigned long long>(sqlscale, sqltype, sqlbuf, s);
    }
    else if (sqltype == SQL_TIMESTAMP
            || sqltype == SQL_TYPE_DATE)
    {
        unsigned short year, month, day, hour, min, sec;
        if (std::sscanf(s, "%hu-%hu-%hu %hu:%hu:%hu",
                    &year, &month, &day, &hour, &min, &sec) != 6)
        {
            if (std::sscanf(s, "%hu-%hu-%huT%hu:%hu:%hu",
                        &year, &month, &day, &hour, &min, &sec) != 6)
            {
                hour = min = sec = 0;
                if (std::sscanf(s, "%hu-%hu-%hu", &year, &month, &day) != 3)
                {
                    throw soci_error("Could not parse timestamp value.");
                }
            }
        }
        std::tm t;
        t.tm_year = year - 1900;
        t.tm_mon = month - 1;
        t.tm_mday = day;
        t.tm_hour = hour;
        t.tm_min = min;
        t.tm_sec = sec;
        tmEncode(sqltype, &t, sqlbuf);
    }
    else if (sqltype == SQL_TYPE_TIME)
    {
        unsigned short hour, min, sec;
        if (std::sscanf(s, "%hu:%hu:%hu", &hour, &min, &sec) != 3)
        {
            throw soci_error("Could not parse timestamp value.");
        }
        std::tm t;
        t.tm_hour = hour;
        t.tm_min = min;
        t.tm_sec = sec;
        tmEncode(sqltype, &t, sqlbuf);
    }
    else
    {
        throw soci_error("Unexpected string type.");
    }
}

std::string getTextParam(unsigned char* sqlbuf, unsigned sqltype, unsigned sqllen, int sqlscale)
{
    short size;
    std::size_t offset = 0;

    if (sqltype == SQL_VARYING)
    {
        GCC_WARNING_SUPPRESS(cast-align)

        size = *reinterpret_cast<short*>(sqlbuf);

        GCC_WARNING_RESTORE(cast-align)

        offset = sizeof(short);
    }
    else if (sqltype == SQL_TEXT)
    {
        size = sqllen;
    }
    else if (sqltype == SQL_SHORT)
    {
        return format_decimal<short>(sqlbuf, sqlscale);
    }
    else if (sqltype == SQL_LONG)
    {
        return format_decimal<int>(sqlbuf, sqlscale);
    }
    else if (sqltype == SQL_INT64)
    {
        return format_decimal<long long>(sqlbuf, sqlscale);
    }
    else
        throw soci_error("Unexpected string type");

    return std::string((char*)sqlbuf + offset, size);
}

} // namespace firebird

} // namespace details

} // namespace soci
