//
// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, Rafal Bobrowski
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_FIREBIRD_SOURCE
#include "soci/firebird/soci-firebird.h"
#include "firebird/error-firebird.h"
#include <cctype>
#include <sstream>
#include <iostream>

using namespace Firebird;
using namespace soci;
using namespace soci::details;
using namespace soci::details::firebird;

firebird_statement_backend::firebird_statement_backend(firebird_session_backend &session)
    : session_(session), stmtp_(nullptr), in_meta_{nullptr}, out_meta_{nullptr},
      in_buffer_{nullptr}, out_buffer_{nullptr}, cursor_{nullptr},
        boundByName_(false), boundByPos_(false), rowsFetched_(0), endOfRowSet_(false), rowsAffectedBulk_(-1LL),
            intoType_(eStandard), useType_(eStandard), procedure_(false)
{
    session_.statements_.add( this );
}

firebird_statement_backend::~firebird_statement_backend()
{
    session_.statements_.erase( this );
}

void firebird_statement_backend::close_cursor()
{
    try
    {
        if( cursor_ ) {
            cursor_->close( &session_.status_ );
            cursor_ = nullptr;
        }
    }
    catch (const FbException& error)
    {
        if( cursor_ ) {
            cursor_->release();
            cursor_ = nullptr;
        }
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

void firebird_statement_backend::clean_up()
{
    struct Finalizer {
        firebird_statement_backend* parent_;
        Finalizer( firebird_statement_backend* parent ) : parent_{parent} {}
        ~Finalizer() {

            auto _release = []( auto * obj ) {
                if( obj ) {
                    obj->release();
                    obj = nullptr;
                }
            };

            _release( parent_->stmtp_ );
            _release( parent_->in_meta_ );
            _release( parent_->out_meta_ );

            if( parent_->in_buffer_  ) { delete[] parent_->in_buffer_ ; parent_->in_buffer_  = nullptr; }
            if( parent_->out_buffer_ ) { delete[] parent_->out_buffer_; parent_->out_buffer_ = nullptr; }
        }
    };

    rowsAffectedBulk_ = -1LL;
    close_cursor();

    Finalizer finalizer( this );
    try
    {
        if( stmtp_ ) {
            stmtp_->free( &session_.status_ );
            stmtp_ = nullptr;
        }
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

void firebird_statement_backend::rewriteParameters(
    std::string const & src, std::vector<char> & dst)
{
    std::vector<char>::iterator dst_it = dst.begin();

    // rewrite the query by transforming all named parameters into
    // the Firebird question marks (:abc -> ?, etc.)

    enum { eNormal, eInQuotes, eInName } state = eNormal;

    std::string name;
    int position = 0;

    for (std::string::const_iterator it = src.begin(), end = src.end();
        it != end; ++it)
    {
        switch (state)
        {
        case eNormal:
            if (*it == '\'')
            {
                *dst_it++ = *it;
                state = eInQuotes;
            }
            else if (*it == ':')
            {
                state = eInName;
            }
            else // regular character, stay in the same state
            {
                *dst_it++ = *it;
            }
            break;
        case eInQuotes:
            if (*it == '\'')
            {
                *dst_it++ = *it;
                state = eNormal;
            }
            else // regular quoted character
            {
                *dst_it++ = *it;
            }
            break;
        case eInName:
            if (std::isalnum(*it) || *it == '_')
            {
                name += *it;
            }
            else // end of name
            {
                names_.insert(std::pair<std::string, int>(name, position++));
                name.clear();
                *dst_it++ = '?';
                *dst_it++ = *it;
                state = eNormal;
            }
            break;
        }
    }

    if (state == eInName)
    {
        names_.insert(std::pair<std::string, int>(name, position++));
        *dst_it++ = '?';
    }

    *dst_it = '\0';
}

void firebird_statement_backend::rewriteQuery(
    std::string const &query, std::vector<char> &buffer)
{
    // buffer for temporary query
    std::vector<char> tmpQuery;
    std::vector<char>::iterator qItr;

    // buffer for query with named parameters changed to standard ones
    std::vector<char> rewQuery(query.size() + 1);

    // take care of named parameters in original query
    rewriteParameters(query, rewQuery);

    std::string const prefix("execute procedure ");
    std::string const prefix2("select * from ");

    // for procedures, we are preparing statement to determine
    // type of procedure.
    if (procedure_)
    {
        tmpQuery.resize(prefix.size() + rewQuery.size());
        qItr = tmpQuery.begin();
        std::copy(prefix.begin(), prefix.end(), qItr);
        qItr += prefix.size();
    }
    else
    {
        tmpQuery.resize(rewQuery.size());
        qItr = tmpQuery.begin();
    }

    // prepare temporary query
    std::copy(rewQuery.begin(), rewQuery.end(), qItr);

    int stType = 0;
    unsigned out_count = 0;
    try
    {
        auto * tmpStmtp = session_.dbhp_->prepare( &session_.status_, session_.current_transaction(), 0,
                                                   &tmpQuery[0], SQL_DIALECT_V6, IStatement::PREPARE_PREFETCH_NONE );
        // get statement type
        stType = tmpStmtp->getType(&session_.status_);
        out_count = tmpStmtp->getOutputMetadata(&session_.status_)->getCount(&session_.status_);
        tmpStmtp->free(&session_.status_);
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }

    // take care of special cases
    if (procedure_)
    {
        // for procedures that return values, we need to use correct syntax
        if ( out_count != 0 )
        {
            // this is "select" procedure, so we have to change syntax
            buffer.resize(prefix2.size() + rewQuery.size());
            qItr = buffer.begin();
            std::copy(prefix2.begin(), prefix2.end(), qItr);
            qItr += prefix2.size();
            std::copy(rewQuery.begin(), rewQuery.end(), qItr);

            // that won't be needed anymore
            procedure_ = false;

            return;
        }
    }
    else
    {
        // this is not procedure, so syntax is ok except for named
        // parameters in ddl
        if (stType == isc_info_sql_stmt_ddl)
        {
            // this statement is a DDL - we can't rewrite named parameters
            // so, we will use original query
            buffer.resize(query.size() + 1);
            std::copy(query.begin(), query.end(), buffer.begin());

            // that won't be needed anymore
            procedure_ = false;

            return;
        }
    }

    // here we know, that temporary query is OK, so we leave it as is
    buffer.resize(tmpQuery.size());
    std::copy(tmpQuery.begin(), tmpQuery.end(), buffer.begin());

    // that won't be needed anymore
    procedure_ = false;
}

void firebird_statement_backend::prepare(std::string const & query,
                                         statement_type /* eType */)
{
    //std::cerr << "prepare: query=" << query << std::endl;
    // clear named parametes
    names_.clear();

    std::vector<char> queryBuffer;

    // modify query's syntax and prepare buffer for use with
    // firebird's api
    rewriteQuery(query, queryBuffer);

    unsigned out_count = 0;

    if( stmtp_ ) clean_up();

    try
    {
        stmtp_ = session_.dbhp_->prepare( &session_.status_, session_.current_transaction(), 0,
                                            &queryBuffer[0], SQL_DIALECT_V6, IStatement::PREPARE_PREFETCH_METADATA );
        
        in_meta_ = stmtp_->getInputMetadata(&session_.status_);
        in_buffer_ = new unsigned char[in_meta_->getMessageLength(&session_.status_)];
        
        out_meta_ = stmtp_->getOutputMetadata(&session_.status_);
        out_buffer_ = new unsigned char[out_meta_->getMessageLength(&session_.status_)];

        out_count = out_meta_->getCount(&session_.status_);
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }

    // prepare buffers for indicators
    inds_.clear();
    inds_.resize( out_count );

    // reset types of into buffers
    intoType_ = eStandard;
    intos_.resize(0);

    // reset types of use buffers
    useType_ = eStandard;
    uses_.resize(0);
}


namespace
{
    SOCI_INLINE void checkSize(std::size_t actual, std::size_t expected,
        std::string const & name)
    {
        if (actual != expected)
        {
            std::ostringstream msg;
            msg << "Incorrect number of " << name << " variables. "
                << "Expected " << expected << ", got " << actual;
            throw soci_error(msg.str());
        }
    }
}

statement_backend::exec_fetch_result
firebird_statement_backend::execute(int number)
{
    std::size_t usize = uses_.size();
    unsigned in_count = 0, out_count = 0;

    try
    {
        in_count  = in_meta_->getCount(&session_.status_);
        out_count = out_meta_->getCount(&session_.status_);
        
        // do we have enough into variables ?
        checkSize(intos_.size(), out_count, "into");
        // do we have enough use variables ?
        checkSize(usize, in_count, "use");

        // do we have parameters ?
        if ( in_count )
        {
            if (useType_ == eStandard)
            {
                for (std::size_t col=0; col<usize; ++col)
                {
                    static_cast<firebird_standard_use_type_backend*>(uses_[col])->exchangeData();
                }
            }
        }

        close_cursor();

        if (useType_ == eVector)
        {
            long long rowsAffectedBulkTemp = 0;

            // Here we have to explicitly loop to achieve the
            // effect of inserting or updating with vector use elements.
            std::size_t rows = static_cast<firebird_vector_use_type_backend*>(uses_[0])->size();
            for (std::size_t row=0; row < rows; ++row)
            {
                // first we have to prepare input parameters
                for (std::size_t col=0; col<usize; ++col)
                {
                    static_cast<firebird_vector_use_type_backend*>(uses_[col])->exchangeData(row);
                }

                // then execute query
                try
                {
                    stmtp_->execute(&session_.status_, session_.current_transaction(), in_meta_, in_buffer_, NULL, NULL);
                    rowsAffectedBulkTemp += get_affected_rows();
                }
                catch (const FbException& error)
                {
                    rowsAffectedBulk_ = rowsAffectedBulkTemp;
                    throw;
                }
                // soci does not allow bulk insert/update and bulk select operations
                // in same query. So here, we know that into elements are not
                // vectors. So, there is no need to fetch data here.
            }
            rowsAffectedBulk_ = rowsAffectedBulkTemp;
        }
        // use elements aren't vectors
        else if ( ( stmtp_->getFlags(&session_.status_) & IStatement::FLAG_HAS_CURSOR ) == 0 ) // no resultset
        {
            stmtp_->execute(&session_.status_, session_.current_transaction(), in_meta_, in_buffer_, out_meta_, out_buffer_);
            // special case when sp is returning some data without cursor
            if( out_count ) {
                for (unsigned i = 0; i < out_count; ++i)
                {
                    inds_[i].resize(1);
                }
                exchangeData(true, 0);
                return ef_success;
            }
        }
        else {
            cursor_ = stmtp_->openCursor(&session_.status_, session_.current_transaction(), in_meta_, in_buffer_, out_meta_, 0);
        }
    }    
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }

    // Successfully re-executing the statement must reset the "end of rowset"
    // flag, we might be able to fetch data again now.
    endOfRowSet_ = false;

    if ( out_count )
    {
        // query may return some data
        if (number > 0)
        {
            // number contains size of input variables, so we may fetch() data here
            return fetch(number);
        }
        else
        {
            // execute(0) was meant to only perform the query
            return ef_success;
        }
    }
    else
    {
        // query can't return any data
        return ef_no_data;
    }
}

statement_backend::exec_fetch_result
firebird_statement_backend::fetch(int number)
{
    if (endOfRowSet_ || !cursor_ )
        return ef_no_data;

    try
    {
        auto out_count = out_meta_->getCount(&session_.status_);
        for (size_t i = 0; i < out_count; ++i)
        {
            inds_[i].resize(number > 0 ? number : 1);
        }

        // Here we have to explicitly loop to achieve the effect of fetching
        // vector into elements. After each fetch, we have to exchange data
        // with into buffers.
        rowsFetched_ = 0;
        for (int i = 0; i < number; ++i)
        {
            auto fetch_stat = cursor_->fetchNext(&session_.status_, out_buffer_);

            // there is more data to read
            if (fetch_stat == IStatus::RESULT_OK)
            {
                ++rowsFetched_;
                exchangeData(true, i);
            }
            else if (fetch_stat == IStatus::RESULT_NO_DATA)
            {
                endOfRowSet_ = true;
                return ef_no_data;
            }
        } // for

        return ef_success;
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

// here we put data fetched from database into user buffers
void firebird_statement_backend::exchangeData(bool gotData, int row)
{
    if (gotData)
    {
        try
        {
            auto num = out_meta_->getCount(&session_.status_);

            for (size_t i = 0; i < num; ++i)
            {
                // first save indicators
                short* null_flag = (short*)&out_buffer_[out_meta_->getNullOffset(&session_.status_,i)];

                if( out_meta_->isNullable(&session_.status_,i) == false )
                {
                    // there is no indicator for this column
                    inds_[i][row] = i_ok;
                }
                else if (*null_flag == 0)
                {
                    inds_[i][row] = i_ok;
                }
                else if (*null_flag == -1)
                {
                    inds_[i][row] = i_null;
                }
                else
                {
                    throw soci_error("Unknown state in firebird_statement_backend::exchangeData()");
                }

                // then deal with data
                if (inds_[i][row] != i_null)
                {
                    if (intoType_ == eVector)
                    {
                        static_cast<firebird_vector_into_type_backend*>(
                            intos_[i])->exchangeData(row);
                    }
                    else
                    {
                        static_cast<firebird_standard_into_type_backend*>(
                            intos_[i])->exchangeData();
                    }
                }
            }
        }
        catch (const FbException& error)
        {
            char buf[1024];
            session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
            throw firebird_soci_error( std::string(buf) );
        }
    }
}

long long firebird_statement_backend::get_affected_rows()
{
    if (rowsAffectedBulk_ >= 0)
    {
        return rowsAffectedBulk_;
    }
    
    try
    {
        return stmtp_->getAffectedRecords(&session_.status_);
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

std::string firebird_statement_backend::get_parameter_name(int index) const
{
    for (std::map<std::string, int>::const_iterator i = names_.begin();
         i != names_.end();
         ++i)
    {
        if (i->second == index)
            return i->first;
    }

    return std::string();
}

std::string firebird_statement_backend::rewrite_for_procedure_call(
    std::string const &query)
{
    procedure_ = true;
    return query;
}

int firebird_statement_backend::prepare_for_describe()
{
    try
    {
        return static_cast<int>(out_meta_->getCount(&session_.status_));
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

void firebird_statement_backend::describe_column(int colNum,
                                                data_type & type, std::string & columnName)
{
    try
    {
        columnName.assign( out_meta_->getAlias(&session_.status_, colNum - 1) );

        switch ( out_meta_->getType(&session_.status_, colNum - 1) )
        {
        case SQL_TEXT:
        case SQL_VARYING:
            type = dt_string;
            break;
        case SQL_TYPE_DATE:
        case SQL_TYPE_TIME:
        case SQL_TIMESTAMP:
            type = dt_date;
            break;
        case SQL_FLOAT:
        case SQL_DOUBLE:
            type = dt_double;
            break;
        case SQL_SHORT:
        case SQL_LONG:
            if (out_meta_->getScale(&session_.status_, colNum - 1) < 0)
            {
                if (session_.get_option_decimals_as_strings())
                    type = dt_string;
                else
                    type = dt_double;
            }
            else
            {
                type = dt_integer;
            }
            break;
        case SQL_INT64:
            if (out_meta_->getScale(&session_.status_, colNum - 1) < 0)
            {
                if (session_.get_option_decimals_as_strings())
                    type = dt_string;
                else
                    type = dt_double;
            }
            else
            {
                type = dt_long_long;
            }
            break;
            /* case SQL_BLOB:
            case SQL_ARRAY:*/
        default:
            std::ostringstream msg;
            msg << "Type of column ["<< colNum << "] \"" << columnName
                << "\" is not supported for dynamic queries";
            throw soci_error(msg.str());
            break;
        }
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

firebird_standard_into_type_backend * firebird_statement_backend::make_into_type_backend()
{
    return new firebird_standard_into_type_backend(*this);
}

firebird_standard_use_type_backend * firebird_statement_backend::make_use_type_backend()
{
    return new firebird_standard_use_type_backend(*this);
}

firebird_vector_into_type_backend * firebird_statement_backend::make_vector_into_type_backend()
{
    return new firebird_vector_into_type_backend(*this);
}

firebird_vector_use_type_backend * firebird_statement_backend::make_vector_use_type_backend()
{
    return new firebird_vector_use_type_backend(*this);
}
