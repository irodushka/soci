#pragma once

// Helpers implementation

template <typename T>
SOCI_INLINE firebird_params_helper<T>::firebird_params_helper( firebird_statement_backend &st ):
        statement_{st},
        data_{nullptr},
        type_{},
        position_{0},
        buf_{nullptr},
        sqltype_{0},
        sqlscale_{0},
        sqllen_{0},
        sqlnullptr_{nullptr},
        firebird_meta_  { isUse ? statement_.in_meta_   : statement_.out_meta_   },
        firebird_buffer_{ isUse ? statement_.in_buffer_ : statement_.out_buffer_ },
        params_         { isUse ? statement_.uses_      : statement_.intos_      }
{

}

template <typename T>
void firebird_params_helper<T>::prepare_field( void * data, details::exchange_type type )
{
    data_ = data;
    type_ = type;
    if( isUse ) statement_.setUsesType( backendType ); else statement_.setIntoType( backendType );

    params_.push_back(static_cast<void*>(this));

    auto & status = statement_.session_.status_;
    try
    {
        const auto offset      = firebird_meta_->getOffset(&status, position_);
        const auto null_offset = firebird_meta_->getNullOffset(&status, position_);

        buf_      = firebird_buffer_ + offset;
        sqltype_  = firebird_meta_->getType(&status, position_);
        sqlscale_ = firebird_meta_->getScale(&status, position_);
        sqllen_   = firebird_meta_->getLength(&status, position_);
        sqlnullptr_ = (short*)(firebird_buffer_ + null_offset);
    }
    catch (const Firebird::FbException& error)
    {
        char buf[1024];
        statement_.session_.master_->getUtilInterface()->formatStatus(buf, sizeof(buf), error.getStatus());
        throw firebird_soci_error( std::string(buf) );
    }
}

template <typename T>
void firebird_use_type_helper<T>::bind_by_pos_internal(int &position, void *data, details::exchange_type type)
{
    if (this->statement_.boundByName_)
    {
        throw soci_error(
         "Binding for use elements must be either by position or by name.");
    }

    this->position_ = position - 1;
    ++position;

    this->prepare_field( data, type );
    this->statement_.boundByPos_ = true;
}

template <typename T>
void firebird_use_type_helper<T>::bind_by_name_internal(std::string const &name, void *data, details::exchange_type type)
{
    if (this->statement_.boundByPos_)
    {
        throw soci_error(
         "Binding for use elements must be either by position or by name.");
    }

    this->position_ = this->statement_.findParamByName( name );
    if( this->position_ == -1 ) {
        throw soci_error(
         "Missing use element for bind by name (" + name + ")");
    }
    this->prepare_field( data, type );
    this->statement_.boundByName_ = true;
}

template <typename T>
SOCI_INLINE void firebird_into_type_helper<T>::define_by_pos(int &position, void *data, details::exchange_type type)
{
    this->position_ = position - 1;
    ++position;
    
    this->prepare_field( data, type );
}
