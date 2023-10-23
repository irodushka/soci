//
// Copyright (C) 2004-2006 Maciej Sobczak, Stephen Hutton, Rafal Bobrowski
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_FIREBIRD_SOURCE
#include "soci/firebird/soci-firebird.h"
#include "firebird/error-firebird.h"

using namespace soci;
using namespace soci::details::firebird;
using namespace Firebird;

firebird_blob_backend::firebird_blob_backend(firebird_session_backend &session)
	  : session_(session), bid_{}, from_db_(false), bhp_(nullptr), data_(),
		loaded_(false), max_seg_size_(0)
{}

firebird_blob_backend::~firebird_blob_backend()
{
    cleanUp();
}

std::size_t firebird_blob_backend::get_len()
{
    if (from_db_ && !bhp_)
    {
        open();
    }

    return data_.size();
}

std::size_t firebird_blob_backend::read(
    std::size_t offset, char * buf, std::size_t toRead)
{
    if (from_db_ && (loaded_ == false))
    {
        // this is blob fetched from database, but not loaded yet
        load();
    }

    std::size_t size = data_.size();

    if (offset > size)
    {
        throw soci_error("Can't read past-the-end of BLOB data");
    }

    char * itr = buf;
    std::size_t limit = size - offset < toRead ? size - offset : toRead;
    std::size_t index = 0;

    while (index < limit)
    {
        *itr = data_[offset+index];
        ++index;
        ++itr;
    }

    return limit;
}

std::size_t firebird_blob_backend::write(std::size_t offset, char const * buf,
                                       std::size_t toWrite)
{
    if (from_db_ && (loaded_ == false))
    {
        // this is blob fetched from database, but not loaded yet
        load();
    }

    std::size_t size = data_.size();

    if (offset > size)
    {
        throw soci_error("Can't write past-the-end of BLOB data");
    }

    // make sure there is enough space in buffer
    if (toWrite > (size - offset))
    {
        data_.resize(size + (toWrite - (size - offset)));
    }

    writeBuffer(offset, buf, toWrite);

    return toWrite;
}

std::size_t firebird_blob_backend::append(
    char const * buf, std::size_t toWrite)
{
    if (from_db_ && (loaded_ == false))
    {
        // this is blob fetched from database, but not loaded yet
        load();
    }

    std::size_t size = data_.size();
    data_.resize(size + toWrite);

    writeBuffer(size, buf, toWrite);

    return toWrite;
}

void firebird_blob_backend::trim(std::size_t newLen)
{
    if (from_db_ && (loaded_ == false))
    {
        // this is blob fetched from database, but not loaded yet
        load();
    }

    data_.resize(newLen);
}

void firebird_blob_backend::writeBuffer(std::size_t offset,
                                      char const * buf, std::size_t toWrite)
{
    char const * itr = buf;
    char const * end_itr = buf + toWrite;

    while (itr!=end_itr)
    {
        data_[offset++] = *itr++;
    }
}

void firebird_blob_backend::open()
{
    if ( bhp_ )
    {
        // BLOB already opened
        return;
    }

    try
    {
        bhp_ = session_.dbhp_->openBlob(&session_.status_, session_.current_transaction(), &bid_, 0, NULL);
        // get basic blob info
        auto blob_size = getBLOBInfo();
        data_.resize(blob_size);
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

void firebird_blob_backend::cleanUp()
{
    from_db_ = false;
    loaded_ = false;
    max_seg_size_ = 0;
    data_.resize(0);

    try
    {
        if ( bhp_ ) bhp_->close(&session_.status_);
        bhp_ = nullptr;
    }
    catch (const FbException& error)
    {
        if( bhp_ ) {
            bhp_->release();
            bhp_ = nullptr;
        }
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

// loads blob data into internal buffer
void firebird_blob_backend::load()
{
    if ( !bhp_ )
    {
        open();
    }

    try
    {
        unsigned bytes;
        std::vector<char>::size_type total_bytes = 0;
        bool keep_reading = false;
        
        do
        {
            bytes = 0;
            // next segment of data
            // data_ is large-enough because we know total size of blob
            
            auto res = bhp_->getSegment(&session_.status_, static_cast<unsigned>(max_seg_size_), &data_[total_bytes], &bytes);
            total_bytes += bytes;
            
            keep_reading = ( res == IStatus::RESULT_OK || res == IStatus::RESULT_SEGMENT );
        }
        while (keep_reading);
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }

    loaded_ = true;
}

// this method saves BLOB content to database
// (a new BLOB will be created at this point)
// BLOB will be closed after save.
void firebird_blob_backend::save()
{
    try
    {
        if( bhp_ ) bhp_->close(&session_.status_);
        bhp_ = session_.dbhp_->createBlob(&session_.status_, session_.current_transaction(), &bid_, 0, NULL);

        if (data_.size() > 0)
        {
            // write data
            size_t size = data_.size();
            size_t offset = 0;
            // Segment Size : Specifying the BLOB segment is throwback to times past, when applications for working 
            // with BLOB data were written in C(Embedded SQL) with the help of the gpre pre - compiler.
            // Nowadays, it is effectively irrelevant.The segment size for BLOB data is determined by the client side and is usually larger than the data page size, 
            // in any case.
            do
            {
                unsigned short segmentSize = 0xFFFF; //last unsigned short number
                if (size - offset < segmentSize) //if content size is less than max segment size or last data segment is about to be written
                    segmentSize = static_cast<unsigned short>(size - offset); 
                //write segment
                bhp_->putSegment(&session_.status_, segmentSize, &data_[0]+offset);
                offset += segmentSize;
            } 
            while (offset < size);
        }

        cleanUp();
        from_db_ = true;
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

// retrives number of segments and total length of BLOB
// returns total length of BLOB
long firebird_blob_backend::getBLOBInfo()
{
    const unsigned char blob_items[] = {isc_info_blob_max_segment, isc_info_blob_total_length};
    unsigned char res_buffer[20];
    char *p, item;
    short length;
    long total_length = 0;

    try
    {
        bhp_->getInfo(&session_.status_, sizeof(blob_items), blob_items, sizeof(res_buffer), res_buffer);
    }
    catch (const FbException& error)
    {
        char buf[1024];
        session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }

    for (p = reinterpret_cast<char*>(res_buffer); *p != isc_info_end ;)
    {
        item = *p++;
        length = static_cast<short>(isc_vax_integer(p, 2));
        p += 2;
        switch (item)
        {
            case isc_info_blob_max_segment:
                max_seg_size_ = isc_vax_integer(p, length);
                break;
            case isc_info_blob_total_length:
                total_length = isc_vax_integer(p, length);
                break;
            case isc_info_truncated:
                throw soci_error("Fatal Error: BLOB info truncated!");
                break;
            default:
                break;
        }
        p += length;
    }

    return total_length;
}
