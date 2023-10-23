//
// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, Rafal Bobrowski
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_FIREBIRD_SOURCE
#include "soci/firebird/soci-firebird.h"
#include "firebird/common.h"

using namespace Firebird;
using namespace soci;
using namespace soci::details;
using namespace soci::details::firebird;

SOCI_INLINE void firebird_vector_use_type_backend::pre_use(indicator const * ind)
{
    inds_ = ind;
}

namespace
{
template <typename T>
SOCI_INLINE T* getUseVectorValue(void *v, std::size_t index)
{
    std::vector<T> *src =
        static_cast<std::vector<T> *>(v);

    std::vector<T> &v_ = *src;
    return &(v_[index]);
}
}

void firebird_vector_use_type_backend::exchangeData(std::size_t row)
{
    // first prepare indicators
    *sqlnullptr_ = 0;
    if (inds_ != NULL)
    {
        switch (inds_[row])
        {
        case i_null:
            *sqlnullptr_ = -1;
            break;
        case i_ok:
            *sqlnullptr_ = 0;
            break;
        default:
            throw soci_error("Use element used with non-supported indicator type.");
        }
    }

    // then set parameters for query execution
    switch (type_)
    {
        // simple cases
    case x_char:
        setTextParam(getUseVectorValue<char>(data_, row), 1, buf_, sqltype_, sqllen_, sqlscale_);
        break;
    case x_short:
        to_isc<short>(
            static_cast<void*>(getUseVectorValue<short>(data_, row)),
            buf_, sqltype_, sqlscale_);
        break;
    case x_integer:
        to_isc<int>(
            static_cast<void*>(getUseVectorValue<int>(data_, row)),
            buf_, sqltype_, sqlscale_);
        break;
    case x_long_long:
        to_isc<long long>(
            static_cast<void*>(getUseVectorValue<long long>(data_, row)),
            buf_, sqltype_, sqlscale_);
        break;
    case x_double:
        to_isc<double>(
            static_cast<void*>(getUseVectorValue<double>(data_, row)),
            buf_, sqltype_, sqlscale_);
        break;

        // cases that require adjustments and buffer management
    case x_stdstring:
        {
            std::string *tmp = getUseVectorValue<std::string>(data_, row);
            setTextParam(tmp->c_str(), tmp->size(), buf_, sqltype_, sqllen_, sqlscale_);
        }
        break;
    case x_stdtm:
        tmEncode(sqltype_,
            getUseVectorValue<std::tm>(data_, row), buf_);
        break;
        //  Not supported
        //  case x_cstring:
        //  case x_blob:
    default:
        throw soci_error("Use element used with non-supported type.");
    } // switch
}

std::size_t firebird_vector_use_type_backend::size()
{
    std::size_t sz = 0; // dummy initialization to please the compiler
    switch (type_)
    {
        // simple cases
    case x_char:
        sz = getVectorSize<char> (data_);
        break;
    case x_short:
        sz = getVectorSize<short> (data_);
        break;
    case x_integer:
        sz = getVectorSize<int> (data_);
        break;
    case x_long_long:
        sz = getVectorSize<long long> (data_);
        break;
    case x_double:
        sz = getVectorSize<double> (data_);
        break;
    case x_stdstring:
        sz = getVectorSize<std::string> (data_);
        break;
    case x_stdtm:
        sz = getVectorSize<std::tm> (data_);
        break;

    default:
        throw soci_error("Use vector element used with non-supported type.");
    }

    return sz;
}
