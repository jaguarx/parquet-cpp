// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef PARQUET_PLAIN_ENCODING_H
#define PARQUET_PLAIN_ENCODING_H

#include "encodings.h"

namespace parquet_cpp {

class PlainDecoder : public Decoder {
 public:
  PlainDecoder(const parquet::Type::type& type)
    : Decoder(type, parquet::Encoding::PLAIN), data_(NULL), len_(0) {
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    data_ = data;
    len_ = len;
  }

  virtual int GetSize() {
    return len_;
  }

  virtual int skip(int values) {
    int byte_size = 0;
    switch (type_) {
    case parquet::Type::BOOLEAN: byte_size = 1; break;
    case parquet::Type::INT32:  byte_size = 4; break;
    case parquet::Type::INT64:  byte_size = 8; break;
    case parquet::Type::FLOAT:  byte_size = 4; break;
    case parquet::Type::DOUBLE: byte_size = 8; break;
    case parquet::Type::BYTE_ARRAY: return _skip_byte_array(values); break;
    default: ParquetException::NYI("unable to determine byte size");
    }
    int num_not_null_values = len_ / byte_size;
    int num_aval_values = std::min(num_not_null_values, num_values_);
    values = std::min(values, num_aval_values);
    int size = values * byte_size;
    if (len_ < size)  ParquetException::EofException();
    data_ += size;
    len_ -= size;
    num_values_ -= values;
    return values;
  }

  int GetValues(void* buffer, int max_values, int byte_size) {
    int num_not_null_values = len_ / byte_size;
    int num_aval_values = std::min(num_not_null_values, num_values_);
    max_values = std::min(max_values, num_aval_values);
    int size = max_values * byte_size;
    if (len_ < size)  ParquetException::EofException();
    memcpy(buffer, data_, size);
    data_ += size;
    len_ -= size;
    num_values_ -= max_values;
    return max_values;
  }

  virtual int GetInt32(int32_t* buffer, int max_values) {
    return GetValues(buffer, max_values, sizeof(int32_t));
  }

  virtual int GetInt64(int64_t* buffer, int max_values) {
    return GetValues(buffer, max_values, sizeof(int64_t));
  }

  virtual int GetFloat(float* buffer, int max_values) {
    return GetValues(buffer, max_values, sizeof(float));
  }

  virtual int GetDouble(double* buffer, int max_values) {
    return GetValues(buffer, max_values, sizeof(double));
  }

  virtual int GetByteArray(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    int values = 0;
    for (int i = 0; i < max_values && len_ > 0; ++i) {
      buffer[i].len = *reinterpret_cast<const uint32_t*>(data_);
      if (len_ < sizeof(uint32_t) + buffer[i].len) ParquetException::EofException();
      buffer[i].ptr = data_ + sizeof(uint32_t);
      data_ += sizeof(uint32_t) + buffer[i].len;
      len_ -= sizeof(uint32_t) + buffer[i].len;
      values ++;
    }
    num_values_ -= values;
    return values;
  }

 private:
  int _skip_byte_array(int max_values) {
    max_values = std::min(max_values, num_values_);
    int values = 0;
    for (int i = 0; i < max_values && len_ > 0; ++i) {
      uint32_t val_len = *reinterpret_cast<const uint32_t*>(data_);
      if (len_ < sizeof(uint32_t) + val_len) ParquetException::EofException();
      data_ += sizeof(uint32_t) + val_len;
      len_ -= sizeof(uint32_t) + val_len;
      values ++;
    }
    num_values_ -= values;
    return max_values;
  }
  const uint8_t* data_;
  int len_;
};

}

#endif

