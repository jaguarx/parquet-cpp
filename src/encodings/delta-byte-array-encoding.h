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

#ifndef PARQUET_DELTA_BYTE_ARRAY_ENCODING_H
#define PARQUET_DELTA_BYTE_ARRAY_ENCODING_H

#include "encodings.h"

namespace parquet_cpp {

class DeltaByteArrayDecoder : public Decoder {
 public:
  DeltaByteArrayDecoder()
    : Decoder(parquet::Type::BYTE_ARRAY, parquet::Encoding::DELTA_BYTE_ARRAY),
      prefix_len_decoder_(parquet::Type::INT32),
      suffix_decoder_(),
      len_(0) {
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    len_ = len;
    prefix_len_decoder_.SetData(num_values, data, len);
    int prefix_len_length = prefix_len_decoder_.GetSize();
    data += prefix_len_length;
    len -= prefix_len_length;
    suffix_decoder_.SetData(num_values, data, len);
    num_values_ = prefix_len_decoder_.GetNumValues();
  }

  virtual int GetSize() {
    return len_;
  }

  virtual int skip(int values) {
    values = std::min(values, num_values_);
    int prefix_lens[values];
    prefix_len_decoder_.GetInt32(prefix_lens, values);
    suffix_decoder_.skip(values);
    num_values_ -= values;
    return values;
  }

  // TODO: this doesn't work and requires memory management. We need to allocate
  // new strings to store the results.
  virtual int GetByteArray(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    int prefix_lens[max_values];
    prefix_len_decoder_.GetInt32(prefix_lens, max_values);
    suffix_decoder_.GetByteArray(buffer, max_values);
    for (int  i = 0; i < max_values; ++i) {
      int prefix_len = prefix_lens[i];
      ByteArray& suffix = buffer[i];
      ByteArray item;
      item.len = prefix_len + suffix.len;

      uint8_t* result = reinterpret_cast<uint8_t*>(malloc(item.len));
      memcpy(result, last_value_.ptr, prefix_len);
      memcpy(result + prefix_len, suffix.ptr, suffix.len);

      item.ptr = result;
      buffer[i] = item;
      last_value_ = item;
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  DeltaBitPackDecoder prefix_len_decoder_;
  DeltaLengthByteArrayDecoder suffix_decoder_;
  ByteArray last_value_;
  int len_;
};

}

#endif

