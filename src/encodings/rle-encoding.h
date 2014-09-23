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

#ifndef PARQUET_CPP_RLE_ENCODING_H
#define PARQUET_CPP_RLE_ENCODING_H

#include "impala/rle-encoding.h"

namespace parquet_cpp {
// wrap around impala::RleDecoder

// Decoder class for RLE encoded data.
class RleDecoder : public Decoder {
 public:
  RleDecoder(const parquet::Type::type& type, int bit_width)
    : Decoder(type, parquet::Encoding::PLAIN), data_(NULL), len_(0),
      bit_width_(bit_width) {
  }

  virtual ~RleDecoder() {}

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    uint32_t num_bytes = *reinterpret_cast<const uint32_t*>(data);
    data_ = data + sizeof(uint32_t);;
    len_ = sizeof(uint32_t) + num_bytes;
    decoder_.reset(new impala::RleDecoder(data_, num_bytes, bit_width_));
  }

  virtual int GetSize() {
    return len_;
  }

  virtual int skip(int values) {
    int buf;
    int i = 0;
    for(int i = 0; i < values; ++i) {
      if ( 1 != GetInt32(&buf, 1) ) {
        return i;
      }
    }
    return i;
  }

  virtual int GetInt32(int32_t* buffer, int max_values) {
    for ( int i=0; i < max_values; ++i) {
      if (!decoder_->Get(buffer+i))
        return i;
    }
    return max_values;
  }

 private:
  boost::scoped_ptr<impala::RleDecoder> decoder_;
  const uint8_t* data_;
  int len_;
  int bit_width_;
};

};

#endif
