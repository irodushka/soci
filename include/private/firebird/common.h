//
// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, Rafal Bobrowski
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef SOCI_FIREBIRD_COMMON_H_INCLUDED
#define SOCI_FIREBIRD_COMMON_H_INCLUDED

#include "soci/firebird/soci-firebird.h"
#include "soci-compiler.h"
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <limits>
#include <sstream>
#include <iomanip>
#include <string>
#include <vector>
#include <algorithm>

namespace soci
{

namespace details
{

namespace firebird
{

void tmEncode(unsigned type, std::tm * src, void * dst);

void tmDecode(unsigned type, void * src, std::tm * dst);

void setTextParam(char const * s, std::size_t size, unsigned char * sqlbuf, unsigned sqltype, unsigned sqllen, int sqlscale);

std::string getTextParam(unsigned char* sqlbuf, unsigned sqltype, unsigned sqllen, int sqlscale);

template <typename IntType>
const char *str2dec(const char * s, IntType &out, short &scale)
{
    int sign = 1;
    if ('+' == *s)
        ++s;
    else if ('-' == *s)
    {
        sign = -1;
        ++s;
    }
    scale = 0;
    bool period = false;
    IntType res = 0;
    for (out = 0; *s; ++s, out = res)
    {
        if (*s == '.')
        {
            if (period)
                return s;
            period = true;
            continue;
        }
        int d = *s - '0';
        if (d < 0 || d > 9)
            return s;
        res = res * 10 + static_cast<IntType>(d * sign);
        if (1 == sign)
        {
            if (res < out)
                return s;
        }
        else
        {
            if (res > out)
                return s;
        }
        if (period)
            ++scale;
    }
    return s;
}

template <typename T>
SOCI_STATIC_INLINE
T round_for_isc(T value)
{
  return value;
}

SOCI_STATIC_INLINE
double round_for_isc(double value)
{
  // Unfortunately all the rounding functions are C99 and so are not supported
  // by MSVC, so do it manually.
  return value < 0 ? value - 0.5 : value + 0.5;
}

//helper template to generate proper code based on compile time type check
template<bool cond> struct cond_to_isc {};
template<> struct cond_to_isc<false>
{
    static void checkInteger(short scale, short type)
    {
        if( scale >= 0 && (type == SQL_SHORT || type == SQL_LONG || type == SQL_INT64) )
            throw soci_error("Can't convert non-integral value to integral column type");
    }
};
template<> struct cond_to_isc<true>
{
    static void checkInteger(short scale,short type) { SOCI_UNUSED(scale) SOCI_UNUSED(type) }
};

template<typename T1>
void to_isc(void * val, unsigned char* sqlbuf, unsigned sqltype, int sqlscale, short x_scale = 0)
{
    T1 value = *reinterpret_cast<T1*>(val);
    short scale = sqlscale + x_scale;
    auto type = sqltype;
    long long divisor = 1, multiplier = 1;

    cond_to_isc<std::numeric_limits<T1>::is_integer>::checkInteger(scale,type);

    for (int i = 0; i > scale; --i)
        multiplier *= 10;
    for (int i = 0; i < scale; ++i)
        divisor *= 10;

    switch (type)
    {
    case SQL_SHORT:
        {
            short tmp = static_cast<short>(round_for_isc(value*multiplier)/divisor);
            std::memcpy(sqlbuf, &tmp, sizeof(short));
        }
        break;
    case SQL_LONG:
        {
            int tmp = static_cast<int>(round_for_isc(value*multiplier)/divisor);
            std::memcpy(sqlbuf, &tmp, sizeof(int));
        }
        break;
    case SQL_INT64:
        {
            long long tmp = static_cast<long long>(round_for_isc(value*multiplier)/divisor);
            std::memcpy(sqlbuf, &tmp, sizeof(long long));
        }
        break;
    case SQL_FLOAT:
        {
            float sql_value = static_cast<float>(value);
            std::memcpy(sqlbuf, &sql_value, sizeof(float));
        }
        break;
    case SQL_DOUBLE:
        {
            double sql_value = static_cast<double>(value);
            std::memcpy(sqlbuf, &sql_value, sizeof(double));
        }
        break;
    default:
        throw soci_error("Incorrect data type for numeric conversion");
    }
}

template<typename IntType, typename UIntType>
void parse_decimal(int sqlscale, unsigned sqltype, unsigned char* sqlbuf, const char * s)
{
    short scale;
    UIntType t1;
    IntType t2;
    if (!*str2dec(s, t1, scale)) {
        to_isc<IntType>((void*)&t1, sqlbuf, sqltype, sqlscale, scale);
    }    
    else if (!*str2dec(s, t2, scale)) {
        to_isc<IntType>((void*)&t2, sqlbuf, sqltype, sqlscale, scale);
    }    
    else
        throw soci_error("Could not parse decimal value.");
}

template<typename IntType>
std::string format_decimal(const void *sqldata, int sqlscale)
{
    IntType x = *reinterpret_cast<const IntType *>(sqldata);
    std::stringstream out;
    out << x;
    std::string r = out.str();
    if (sqlscale < 0)
    {
        if (static_cast<int>(r.size()) - (x < 0) <= -sqlscale)
        {
            r = std::string(size_t(x < 0), '-') +
                std::string(-sqlscale - (r.size() - (x < 0)) + 1, '0') +
                r.substr(size_t(x < 0), std::string::npos);
        }
        return r.substr(0, r.size() + sqlscale) + '.' +
            r.substr(r.size() + sqlscale, std::string::npos);
    }
    return r + std::string(sqlscale, '0');
}


template<bool cond> struct cond_from_isc {};
template<> struct cond_from_isc<true> {
    static void checkInteger(short scale)
    {
        std::ostringstream msg;
        msg << "Can't convert value with scale " << -scale
            << " to integral type";
        throw soci_error(msg.str());
    }
};
template<> struct cond_from_isc<false>
{
    static void checkInteger(short scale) { SOCI_UNUSED(scale) }
};

template<typename T1>
T1 from_isc(unsigned char* sqlbuf, unsigned sqltype, int sqlscale)
{
    short scale = sqlscale;
    T1 tens = 1;

    if (scale < 0)
    {
        cond_from_isc<std::numeric_limits<T1>::is_integer>::checkInteger(scale);
        for (int i = 0; i > scale; --i)
        {
            tens *= 10;
        }
    }

    GCC_WARNING_SUPPRESS(cast-align)

    switch (sqltype)
    {
    case SQL_SHORT:
        return static_cast<T1>(*reinterpret_cast<short*>(sqlbuf)/tens);
    case SQL_LONG:
        return static_cast<T1>(*reinterpret_cast<int*>(sqlbuf)/tens);
    case SQL_INT64:
        return static_cast<T1>(*reinterpret_cast<long long*>(sqlbuf)/tens);
    case SQL_FLOAT:
        return static_cast<T1>(*reinterpret_cast<float*>(sqlbuf));
    case SQL_DOUBLE:
        return static_cast<T1>(*reinterpret_cast<double*>(sqlbuf));
    default:
        throw soci_error("Incorrect data type for numeric conversion");
    }

    GCC_WARNING_RESTORE(cast-align)
}

template <typename T>
SOCI_STATIC_INLINE
std::size_t getVectorSize(void *p)
{
    std::vector<T> *v = static_cast<std::vector<T> *>(p);
    return v->size();
}

template <typename T>
SOCI_STATIC_INLINE
void resizeVector(void *p, std::size_t sz)
{
    std::vector<T> *v = static_cast<std::vector<T> *>(p);
    v->resize(sz);
}

} // namespace firebird

} // namespace details

} // namespace soci

#endif // SOCI_FIREBIRD_COMMON_H_INCLUDED
