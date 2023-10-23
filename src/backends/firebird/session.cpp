//
// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, Rafal Bobrowski
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_FIREBIRD_SOURCE
#include "soci/firebird/soci-firebird.h"
#include "firebird/error-firebird.h"
#include "soci/session.h"
#include <locale>
#include <map>
#include <sstream>
#include <string>

using namespace soci;
using namespace soci::details::firebird;
using namespace Firebird;

namespace
{

// Helpers of explodeISCConnectString() for reading words from a string. "Word"
// here is defined very loosely as just a sequence of non-space characters.
//
// All these helper functions update the input iterator to point to the first
// character not consumed by them.

// Advance the input iterator until the first non-space character or end of the
// string.
void skipWhiteSpace(std::string::const_iterator& i, std::string::const_iterator const &end)
{
    std::locale const loc;
    for (; i != end; ++i)
    {
        if (!std::isspace(*i, loc))
            break;
    }
}

// Return the string of all characters until the first space or the specified
// delimiter.
//
// Throws if the first non-space character after the end of the word is not the
// delimiter. However just returns en empty string, without throwing, if
// nothing is left at all in the string except for white space.
std::string
getWordUntil(std::string const &s, std::string::const_iterator &i, char delim)
{
    std::string::const_iterator const end = s.end();
    skipWhiteSpace(i, end);

    // We need to handle this case specially because it's not an error if
    // nothing at all remains in the string. But if anything does remain, then
    // we must have the delimiter.
    if (i == end)
        return std::string();

    // Simply put anything until the delimiter into the word, stopping at the
    // first white space character.
    std::string word;
    std::locale const loc;
    for (; i != end; ++i)
    {
        if (*i == delim)
            break;

        if (std::isspace(*i, loc))
        {
            skipWhiteSpace(i, end);
            if (i == end || *i != delim)
            {
                std::ostringstream os;
                os << "Expected '" << delim << "' at position "
                   << (i - s.begin() + 1)
                   << " in Firebird connection string \""
                   << s << "\".";

                throw soci_error(os.str());
            }

            break;
        }

        word += *i;
    }

    if (i == end)
    {
        std::ostringstream os;
        os << "Expected '" << delim
           << "' not found before the end of the string "
           << "in Firebird connection string \""
           << s << "\".";

        throw soci_error(os.str());
    }

    ++i;    // Skip the delimiter itself.

    return word;
}

// Return a possibly quoted word, i.e. either just a sequence of non-space
// characters or everything inside a double-quoted string.
//
// Throws if the word is quoted and the closing quote is not found. However
// doesn't throw, just returns an empty string if there is nothing left.
std::string
getPossiblyQuotedWord(std::string const &s, std::string::const_iterator &i)
{
    std::string::const_iterator const end = s.end();
    skipWhiteSpace(i, end);

    std::string word;

    if (i != end && *i == '"')
    {
        for (;;)
        {
            if (++i == end)
            {
                std::ostringstream os;
                os << "Expected '\"' not found before the end of the string "
                      "in Firebird connection string \""
                   << s << "\".";

                throw soci_error(os.str());
            }

            if (*i == '"')
            {
                ++i;
                break;
            }

            word += *i;
        }
    }
    else // Not quoted.
    {
        std::locale const loc;
        for (; i != end; ++i)
        {
            if (std::isspace(*i, loc))
                break;

            word += *i;
        }
    }

    return word;
}

// retrieves parameters from the uniform connect string which is supposed to be
// in the form "key=value[ key2=value2 ...]" and the values may be quoted to
// allow including spaces into them. Notice that currently there is no way to
// include both a space and a double quote in a value.
std::map<std::string, std::string>
explodeISCConnectString(std::string const &connectString)
{
    std::map<std::string, std::string> parameters;

    std::string key, value;
    for (std::string::const_iterator i = connectString.begin(); ; )
    {
        key = getWordUntil(connectString, i, '=');
        if (key.empty())
            break;

        value = getPossiblyQuotedWord(connectString, i);

        parameters.insert(std::pair<std::string, std::string>(key, value));
    }

    return parameters;
}

// extracts given parameter from map previusly build with explodeISCConnectString
bool getISCConnectParameter(std::map<std::string, std::string> const & m, std::string const & key,
    std::string & value)
{
    std::map <std::string, std::string> :: const_iterator i;
    value.clear();

    i = m.find(key);

    if (i != m.end())
    {
        value = i->second;
        return true;
    }
    else
    {
        return false;
    }
}

} // namespace anonymous

firebird_session_backend::firebird_session_backend(
    connection_parameters const & parameters, Firebird::IMaster* master) : dbhp_(nullptr)
                                         , prov_{master->getDispatcher()}
                                         , master_{master}
                                         , status_{master_->getStatus()}
                                         , statements_{}
                                         , trhp_(nullptr)
                                         , decimals_as_strings_(false)
{
    // extract connection parameters
    std::map<std::string, std::string>
        params(explodeISCConnectString(parameters.get_connect_string()));

    std::string param;
    IUtil* utl    = master_->getUtilInterface();
    IXpbBuilder* dpb = nullptr;

    try
    {
        dpb = utl->getXpbBuilder(&status_, IXpbBuilder::DPB, NULL, 0);

        // preparing connection options
        int connect_timeout = 0;

        if (getISCConnectParameter(params, "user", param))
        {
            dpb->insertString(&status_, isc_dpb_user_name, param.c_str());
        }

        if (getISCConnectParameter(params, "password", param))
        {
            dpb->insertString(&status_, isc_dpb_password, param.c_str());
        }

        if (getISCConnectParameter(params, "role", param))
        {
            dpb->insertString(&status_, isc_dpb_sql_role_name, param.c_str());
        }

        if (getISCConnectParameter(params, "charset", param))
        {
            dpb->insertString(&status_, isc_dpb_lc_ctype, param.c_str());
        }

        if (parameters.get_option("connect_timeout", param) ||
                getISCConnectParameter(params, "connect_timeout", param))
        {
            connect_timeout = std::stol(param); // throws
        }
        if (connect_timeout > 0) {
            dpb->insertInt(&status_, isc_dpb_connect_timeout, connect_timeout);
        }
        if (getISCConnectParameter(params, "service", param) == false)
        {
            throw soci_error("Service name not specified.");
        }

        // connecting data base
        dbhp_ = prov_->attachDatabase( &status_, param.c_str(), dpb->getBufferLength(&status_), dpb->getBuffer(&status_) );
        dpb->dispose();
    }
    catch (const FbException& error)
    {
        if( dpb ) dpb->dispose();
        char buf[1024];
        utl->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }

    if (getISCConnectParameter(params, "decimals_as_strings", param))
    {
        decimals_as_strings_ = param == "1" || param == "Y" || param == "y";
    }
}


void firebird_session_backend::begin()
{
    if ( !trhp_ )
    {
        try
        {
            trhp_ = dbhp_->startTransaction(&status_, 0, NULL);
        }
        catch (const FbException& error)
        {
            char buf[1024];
            master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
            throw firebird_soci_error( std::string(buf) );
        }
    }
}

void firebird_session_backend::start_transaction(const transaction_parameters &tp)
{
    if ( !trhp_ )
    {
        IUtil* utl    = master_->getUtilInterface();
        IXpbBuilder* tpb = nullptr;
        
        try
        {
            tpb = utl->getXpbBuilder(&status_, IXpbBuilder::TPB, NULL, 0);

            switch(tp.access_mode) {
            case tp_access_mode::READ_WRITE:
                tpb->insertTag(&status_, isc_tpb_write);
                break;
            case tp_access_mode::READ_ONLY:
                tpb->insertTag(&status_, isc_tpb_read);
                break;
            }

            switch(tp.isolation_level) {
            case tp_isolation_level::SNAPSHOT:
                tpb->insertTag(&status_, isc_tpb_concurrency);
                break;
            case tp_isolation_level::SNAPSHOT_TABLE_STABILITY:
                tpb->insertTag(&status_, isc_tpb_consistency);
                break;
            case tp_isolation_level::READ_COMMITTED_RECORD_VERSION:
                tpb->insertTag(&status_, isc_tpb_read_committed);
                tpb->insertTag(&status_, isc_tpb_rec_version);
                break;
            case tp_isolation_level::READ_COMMITTED_NO_RECORD_VERSION:
                tpb->insertTag(&status_, isc_tpb_read_committed);
                tpb->insertTag(&status_, isc_tpb_no_rec_version);
                break;
            }

            switch(tp.lock_resolution) {
            case tp_lock_resolution::WAIT:
                tpb->insertTag(&status_, isc_tpb_wait);
                break;
            case tp_lock_resolution::NO_WAIT:
                tpb->insertTag(&status_, isc_tpb_nowait);
                break;
            }
            if (tp.lock_timeout>0) {
                tpb->insertInt(&status_, isc_tpb_lock_timeout, tp.lock_timeout);
            }

            for (const auto &reservation: tp.table_reservation) {
                switch(reservation.second) {
                case tp_reservation::PROTECTED_READ:
                    tpb->insertBytes(&status_, isc_tpb_lock_read, reservation.first.c_str(), reservation.first.length());
                    tpb->insertTag(&status_, isc_tpb_protected);
                    break;
                case tp_reservation::PROTECTED_WRITE:
                    tpb->insertBytes(&status_, isc_tpb_lock_write, reservation.first.c_str(), reservation.first.length());
                    tpb->insertTag(&status_, isc_tpb_protected);
                    break;
                case tp_reservation::SHARED_READ:
                    tpb->insertBytes(&status_, isc_tpb_lock_read, reservation.first.c_str(), reservation.first.length());
                    tpb->insertTag(&status_, isc_tpb_shared);
                    break;
                case tp_reservation::SHARED_WRITE:
                    tpb->insertBytes(&status_, isc_tpb_lock_write, reservation.first.c_str(), reservation.first.length());
                    tpb->insertTag(&status_, isc_tpb_shared);
                    break;
                }
            }
            
            trhp_ = dbhp_->startTransaction(&status_, tpb->getBufferLength(&status_), tpb->getBuffer(&status_));
            tpb->dispose();
        }
        catch (const FbException& error)
        {
            if( tpb ) tpb->dispose();
            char buf[1024];
            utl->formatStatus(buf, sizeof(buf), error.getStatus());
            throw firebird_soci_error( std::string(buf) );
        }
    }
}

firebird_session_backend::~firebird_session_backend()
{
    try {
        cleanUp();
    } catch (const firebird_soci_error &ex) {
        // @TODO report error some how
    }
}

bool firebird_session_backend::is_connected()
{
    const unsigned char req[] = { isc_info_ods_version, isc_info_end };
    unsigned char res[256];
    
    try
    {
        dbhp_->getInfo( &status_, sizeof(req), req, sizeof(res), res );
    }
    catch (const FbException& error)
    {
        char buf[1024];
        master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
    
    return status_.getState() == 0;
}

void firebird_session_backend::commit()
{
    struct Finalizer {
        firebird_session_backend* parent_;
        Finalizer( firebird_session_backend* parent ) : parent_{parent} {}
        ~Finalizer() {
            if( parent_->trhp_ ) {
                parent_->trhp_->release();
                parent_->trhp_ = nullptr;
            }
        }
    };

    if ( trhp_ )
    {
        Finalizer finalizer( this );
        try
        {
            statements_.closeCursorsAndBlobs();
            trhp_->commit(&status_);
            trhp_ = nullptr;
        }
        catch (const FbException& error)
        {
            char buf[1024];
            master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
            throw firebird_soci_error( std::string(buf) );
        }
    }
}

void firebird_session_backend::rollback()
{
    struct Finalizer {
        firebird_session_backend* parent_;
        Finalizer( firebird_session_backend* parent ) : parent_{parent} {}
        ~Finalizer() {
            if( parent_->trhp_ ) {
                parent_->trhp_->release();
                parent_->trhp_ = nullptr;
            }
        }
    };

    if ( trhp_ )
    {
        Finalizer finalizer( this );
        try
        {
            statements_.closeCursorsAndBlobs();
            trhp_->rollback(&status_);
            trhp_ = nullptr;
        }
        catch (const FbException& error)
        {
            char buf[1024];
            master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
            throw firebird_soci_error( std::string(buf) );
        }
    }
}

Firebird::ITransaction* firebird_session_backend::current_transaction()
{
    // It will do nothing if we're already inside a transaction.
    begin();
    return trhp_;
}

void firebird_session_backend::cleanUp()
{
    struct Finalizer {
        firebird_session_backend* parent_;
        Finalizer( firebird_session_backend* parent ) : parent_{parent} {}
        ~Finalizer() {
            if( parent_->dbhp_ ) {
                parent_->dbhp_->release();
                parent_->dbhp_ = nullptr;
            }
            parent_->prov_->release();
            parent_->status_.dispose();
        }
    };

    Finalizer finalizer( this );

    this->commit();

    try
    {
        dbhp_->detach(&status_);
        dbhp_ = nullptr;
    }
    catch (const FbException& error)
    {
        char buf[1024];
        master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

bool firebird_session_backend::get_next_sequence_value(
    session & s, std::string const & sequence, long long & value)
{
    // We could use isq_execute2() directly but this is even simpler.
    s << "select next value for " + sequence + " from rdb$database",
          into(value);

    return true;
}

firebird_statement_backend * firebird_session_backend::make_statement_backend()
{
    return new firebird_statement_backend(*this);
}

details::rowid_backend* firebird_session_backend::make_rowid_backend()
{
    throw soci_error("RowIDs are not supported");
}

firebird_blob_backend * firebird_session_backend::make_blob_backend()
{
    return new firebird_blob_backend(*this);
}
