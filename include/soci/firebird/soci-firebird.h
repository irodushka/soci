//
// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, Rafal Bobrowski
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//
//

#ifndef SOCI_FIREBIRD_H_INCLUDED
#define SOCI_FIREBIRD_H_INCLUDED

#ifdef _WIN32
# ifdef SOCI_DLL
#  ifdef SOCI_FIREBIRD_SOURCE
#   define SOCI_FIREBIRD_DECL __declspec(dllexport)
#  else
#   define SOCI_FIREBIRD_DECL __declspec(dllimport)
#  endif // SOCI_DLL
# endif // SOCI_FIREBIRD_SOURCE
#endif // _WIN32

//
// If SOCI_FIREBIRD_DECL isn't defined yet define it now
#ifndef SOCI_FIREBIRD_DECL
# define SOCI_FIREBIRD_DECL
#endif

#ifdef _WIN32
#include <ciso646> // To understand and/or/not on MSVC9
#endif
#include <soci/soci-backend.h>

#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

#include <firebird/Interface.h> // unused parameters here)

#ifndef _WIN32
#pragma GCC diagnostic pop
#endif

#include <firebird/Message.h>
#include <cstdlib>
#include <vector>
#include <string>
#include <algorithm>
#include <set>

#define SOCI_INLINE inline
#define SOCI_STATIC_INLINE static inline

namespace soci
{

std::size_t const stat_size = 20;

// size of buffer for error messages. All examples use this value.
// Anyone knows, where it is stated that 512 bytes is enough ?
std::size_t const SOCI_FIREBIRD_ERRMSG = 512;

class SOCI_FIREBIRD_DECL firebird_soci_error : public soci_error
{
public:
    firebird_soci_error(std::string const & msg,
        ISC_STATUS const * status = 0);

    ~firebird_soci_error() SOCI_NOEXCEPT SOCI_OVERRIDE {};

    std::vector<ISC_STATUS> status_;
};

enum BuffersType
{
    eStandard, eVector
};

struct firebird_blob_backend;
struct firebird_statement_backend;

///// Helpers ////////////////////////////////////////////////////
// Those helper classes incapsulate the common logic 
// for into & use backends

// Common helper
template <typename T>
struct firebird_params_helper : public T {

    static constexpr bool isVector   = std::is_same_v<details::vector_into_type_backend,T> ||
                                       std::is_same_v<details::vector_use_type_backend,T>;

    static constexpr bool isStandard = std::is_same_v<details::standard_into_type_backend,T> ||
                                       std::is_same_v<details::standard_use_type_backend,T>;

    static constexpr bool isUse = std::is_same_v<details::vector_use_type_backend,T> ||
                                  std::is_same_v<details::standard_use_type_backend,T>;

    static constexpr BuffersType backendType = isVector ? eVector : eStandard;

    static_assert( isVector || isStandard );

    SOCI_INLINE firebird_params_helper( firebird_statement_backend &st );

protected:
    firebird_statement_backend &statement_;
    void *data_;
    details::exchange_type type_;
    int position_;
    unsigned char *buf_;
    unsigned sqltype_;
    int sqlscale_;
    unsigned sqllen_;
    short* sqlnullptr_;

    Firebird::IMessageMetadata* firebird_meta_;
    unsigned char* firebird_buffer_;
    std::vector<void*> & params_;

    void prepare_field( void * data, details::exchange_type type );

    SOCI_INLINE void clean_up() SOCI_OVERRIDE
    {
        auto it = std::find( params_.begin(), params_.end(), static_cast<void*>(this) );
        if (it != params_.end())
            params_.erase(it);
    }
};

// Helper for input parameters (uses)
template <typename T>
struct firebird_use_type_helper : public firebird_params_helper<T> {

    static_assert( std::is_same_v<details::vector_use_type_backend,T> ||
                   std::is_same_v<details::standard_use_type_backend,T> );

    SOCI_INLINE firebird_use_type_helper( firebird_statement_backend &st ) :
        firebird_params_helper<T>( st ) {}

protected:
    //have to use internals due to different bind_by_pos/bind_by_name signature for standard & vectors (readOnly param)
    void bind_by_pos_internal(int &position, void *data, details::exchange_type type);
    void bind_by_name_internal(std::string const &name, void *data, details::exchange_type type);
};

// Helper for output parameters (intos)
template <typename T>
struct firebird_into_type_helper : public firebird_params_helper<T> {

    static_assert( std::is_same_v<details::vector_into_type_backend,T> ||
                   std::is_same_v<details::standard_into_type_backend,T> );

    SOCI_INLINE firebird_into_type_helper( firebird_statement_backend &st ) :
        firebird_params_helper<T>( st ) {}

protected:
    SOCI_INLINE void define_by_pos(int &position, void *data, details::exchange_type type) SOCI_OVERRIDE;
};

////// Helpers end //////////////////////////////

struct firebird_standard_into_type_backend : public firebird_into_type_helper<details::standard_into_type_backend>
{
    firebird_standard_into_type_backend(firebird_statement_backend &st)
        : firebird_into_type_helper<details::standard_into_type_backend>( st )
    {}

    SOCI_INLINE void pre_fetch() SOCI_OVERRIDE {} // nothing to do
    void post_fetch(bool gotData, bool calledFromFetch,
        indicator *ind) SOCI_OVERRIDE;

    virtual void exchangeData();

private:
    // Copy contents of a BLOB (represented by its id) in buf_ into the given
    // string.
    void copy_from_blob(std::string& out);
};

struct firebird_vector_into_type_backend : public firebird_into_type_helper<details::vector_into_type_backend>
{
    firebird_vector_into_type_backend(firebird_statement_backend &st)
        : firebird_into_type_helper<details::vector_into_type_backend>( st )
    {}

    SOCI_INLINE void pre_fetch() SOCI_OVERRIDE {} // nothing to do
    void post_fetch(bool gotData, indicator *ind) SOCI_OVERRIDE;

    void resize(std::size_t sz) SOCI_OVERRIDE;
    std::size_t size() SOCI_OVERRIDE;
    virtual void exchangeData(std::size_t row);
};

struct firebird_standard_use_type_backend : public firebird_use_type_helper<details::standard_use_type_backend>
{
    firebird_standard_use_type_backend(firebird_statement_backend &st)
        : firebird_use_type_helper<details::standard_use_type_backend>( st )
    {}

    SOCI_INLINE void bind_by_pos(int &position,
        void *data, details::exchange_type type, bool /*readOnly*/) SOCI_OVERRIDE
    { 
        this->bind_by_pos_internal( position, data, type );
    }

    SOCI_INLINE void bind_by_name(std::string const &name,
        void *data, details::exchange_type type, bool /*readOnly*/) SOCI_OVERRIDE
    {
        this->bind_by_name_internal( name, data, type );
    }

    void pre_use(indicator const *ind) SOCI_OVERRIDE;
    void post_use(bool gotData, indicator *ind) SOCI_OVERRIDE;
    virtual void exchangeData();

private:
    //void bind_internal( void * data, details::exchange_type type );
    // Allocate a temporary blob, fill it with the data from the provided
    // string and copy its ID into buf_.
    void copy_to_blob(const std::string& in);

    // This is used for types mapping to CLOB.
    firebird_blob_backend* blob_;
};

struct firebird_vector_use_type_backend : public firebird_use_type_helper<details::vector_use_type_backend>
{
    firebird_vector_use_type_backend(firebird_statement_backend &st)
        : firebird_use_type_helper<details::vector_use_type_backend>( st )
    {}

    SOCI_INLINE void bind_by_pos(int &position,
        void *data, details::exchange_type type) SOCI_OVERRIDE
    {
        this->bind_by_pos_internal( position, data, type );
    }

    SOCI_INLINE void bind_by_name(std::string const &name,
        void *data, details::exchange_type type) SOCI_OVERRIDE
    {
        this->bind_by_name_internal( name, data, type );
    }

    SOCI_INLINE void pre_use(indicator const *ind) SOCI_OVERRIDE;

    std::size_t size() SOCI_OVERRIDE;

    virtual void exchangeData(std::size_t row);

    indicator const *inds_;
};

struct firebird_session_backend;
struct firebird_statement_backend : details::statement_backend
{
    firebird_statement_backend(firebird_session_backend &session);
    ~firebird_statement_backend();

    SOCI_INLINE void alloc() SOCI_OVERRIDE {}
    void clean_up() SOCI_OVERRIDE;
    void prepare(std::string const &query,
        details::statement_type eType) SOCI_OVERRIDE;

    exec_fetch_result execute(int number) SOCI_OVERRIDE;
    exec_fetch_result fetch(int number) SOCI_OVERRIDE;

    long long get_affected_rows() SOCI_OVERRIDE;
    SOCI_INLINE int get_number_of_rows() SOCI_OVERRIDE { return rowsFetched_; };
    std::string get_parameter_name(int index) const SOCI_OVERRIDE;

    std::string rewrite_for_procedure_call(std::string const &query) SOCI_OVERRIDE;

    int prepare_for_describe() SOCI_OVERRIDE;
    void describe_column(int colNum, data_type &dtype,
        std::string &columnName) SOCI_OVERRIDE;

    firebird_standard_into_type_backend * make_into_type_backend() SOCI_OVERRIDE;
    firebird_standard_use_type_backend * make_use_type_backend() SOCI_OVERRIDE;
    firebird_vector_into_type_backend * make_vector_into_type_backend() SOCI_OVERRIDE;
    firebird_vector_use_type_backend * make_vector_use_type_backend() SOCI_OVERRIDE;

    firebird_session_backend &session_;

    Firebird::IStatement* stmtp_;

    Firebird::IMessageMetadata* in_meta_;
    Firebird::IMessageMetadata* out_meta_;
    unsigned char* in_buffer_;
    unsigned char* out_buffer_;

protected:
    Firebird::IResultSet* cursor_;

public:
    void close_cursor();

    bool boundByName_;
    bool boundByPos_;

    std::vector<std::vector<indicator> > inds_;
    std::vector<void*> intos_;
    std::vector<void*> uses_;

    // accessor methods (instead of "friends")
    SOCI_INLINE void setIntoType( BuffersType type ) { intoType_ = type; }
    SOCI_INLINE void setUsesType( BuffersType type ) { useType_  = type; }
    SOCI_INLINE int findParamByName( const std::string& name ) {
        auto it = names_.find(name);
        return it == names_.end() ? -1 : it->second;
    }

protected:
    int rowsFetched_;
    bool endOfRowSet_;

    long long rowsAffectedBulk_; // number of rows affected by the last bulk operation

    virtual void exchangeData(bool gotData, int row);
    virtual void rewriteQuery(std::string const & query,
        std::vector<char> & buffer);
    virtual void rewriteParameters(std::string const & src,
        std::vector<char> & dst);

    BuffersType intoType_;
    BuffersType useType_;

    // named parameters
    std::map <std::string, int> names_;

    bool procedure_;
};

struct firebird_blob_backend : details::blob_backend
{
    firebird_blob_backend(firebird_session_backend &session);

    ~firebird_blob_backend() SOCI_OVERRIDE;

    std::size_t get_len() SOCI_OVERRIDE;
    std::size_t read(std::size_t offset, char *buf,
        std::size_t toRead) SOCI_OVERRIDE;
    std::size_t write(std::size_t offset, char const *buf,
        std::size_t toWrite) SOCI_OVERRIDE;
    std::size_t append(char const *buf, std::size_t toWrite) SOCI_OVERRIDE;
    void trim(std::size_t newLen) SOCI_OVERRIDE;

    firebird_session_backend &session_;

    virtual void save();
    virtual void assign(ISC_QUAD const & bid)
    {
        cleanUp();

        bid_ = bid;
        from_db_ = true;
    }

    // BLOB id from in database
    ISC_QUAD bid_;

    // BLOB id was fetched from database (true)
    // or this is new BLOB
    bool from_db_;

    // BLOB handle
    Firebird::IBlob* bhp_;

protected:

    virtual void open();
    virtual long getBLOBInfo();
    virtual void load();
    virtual void writeBuffer(std::size_t offset, char const * buf,
        std::size_t toWrite);
    virtual void cleanUp();

    // buffer for BLOB data
    std::vector<char> data_;

    bool loaded_;
    long max_seg_size_;
};

struct firebird_session_backend : details::session_backend
{
    firebird_session_backend(connection_parameters const & parameters, Firebird::IMaster* master);

    ~firebird_session_backend() SOCI_OVERRIDE;

    bool is_connected() SOCI_OVERRIDE;

    void begin() SOCI_OVERRIDE;
    void start_transaction(transaction_parameters const &) SOCI_OVERRIDE;
    void commit() SOCI_OVERRIDE;
    void rollback() SOCI_OVERRIDE;

    bool get_next_sequence_value(session & s,
        std::string const & sequence, long long & value) SOCI_OVERRIDE;

    std::string get_dummy_from_table() const SOCI_OVERRIDE { return "rdb$database"; }

    std::string get_backend_name() const SOCI_OVERRIDE { return "firebird"; }

    void cleanUp();

    firebird_statement_backend * make_statement_backend() SOCI_OVERRIDE;
    details::rowid_backend* make_rowid_backend() SOCI_OVERRIDE;
    firebird_blob_backend * make_blob_backend() SOCI_OVERRIDE;

    bool get_option_decimals_as_strings() { return decimals_as_strings_; }

    // Returns the pointer to the current transaction handle, starting a new
    // transaction if necessary.
    //
    // The returned pointer should
    Firebird::ITransaction* current_transaction();

    Firebird::IAttachment* dbhp_;
    Firebird::IProvider* prov_;
    Firebird::IMaster* master_;
    Firebird::ThrowStatusWrapper status_;

    /*
        This class is used to track the living cycle of cursors& blobs, created on statement-backend
        The new FB OOAPI has a feature that when the transaction is closed, the cursors&blobs, opened 
        in that closed transaction, become invalid (closed internally). In that case we can get an exception
        in a statement's dtor, on the cursor/blob close() call.
        Therefore, we need to close all the cursors/blobs, associated with our connection, before closing the transaction.
    */
    struct Statements {
        Statements() : st_set_{} {}
        SOCI_INLINE bool add( firebird_statement_backend* st )   { auto res = st_set_.insert( st ); return res.second; }
        SOCI_INLINE bool erase( firebird_statement_backend* st ) { return st_set_.erase( st ) == 1; }
        SOCI_INLINE firebird_statement_backend* find( firebird_statement_backend* st ) {
            auto it = st_set_.find( st );
            return it == st_set_.end() ? nullptr : *it;
        }
        SOCI_INLINE void closeCursorsAndBlobs() {
            for( auto * st : st_set_ ) {
                st->close_cursor();
                //what about blobs - I see no long-living blobs now in SOCI.
                //TODO: add blob->close() logic if there are problems here.
            }
        }
        SOCI_INLINE size_t size() { return st_set_.size(); }
    private:
        std::set<firebird_statement_backend*> st_set_;
    };
    Statements statements_;

private:
    Firebird::ITransaction* trhp_;
    bool decimals_as_strings_;
};

struct firebird_backend_factory : backend_factory
{
    firebird_backend_factory() : master_{ Firebird::fb_get_master_interface() } {}
    ~firebird_backend_factory() { fb_shutdown(fb_shutrsn_app_stopped, 0); }

    firebird_session_backend * make_session(
        connection_parameters const & parameters) const SOCI_OVERRIDE;
private:
    Firebird::IMaster* master_;
};

// param helpers implementation here, stored in header to avoid templates linkage issues
#include "firebird/params_helper.h"

extern SOCI_FIREBIRD_DECL firebird_backend_factory const firebird;

extern "C"
{

// for dynamic backend loading
SOCI_FIREBIRD_DECL backend_factory const * factory_firebird();
SOCI_FIREBIRD_DECL void register_factory_firebird();

} // extern "C"

} // namespace soci

#endif // SOCI_FIREBIRD_H_INCLUDED
