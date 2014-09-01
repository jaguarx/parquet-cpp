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

#ifndef PARQUET_DELTA_BIT_PACK_ENCODING_H
#define PARQUET_DELTA_BIT_PACK_ENCODING_H

#include "encodings.h"
#include "bitpack.h"

namespace parquet_cpp {

class DeltaBitPackDecoder : public Decoder {
 public:
  DeltaBitPackDecoder(const parquet::Type::type& type)
    : Decoder(type, parquet::Encoding::DELTA_BINARY_PACKED),len_(0) {
    if (type != parquet::Type::INT32 && type != parquet::Type::INT64) {
      throw ParquetException("Delta bit pack encoding should only be for integer data.");
    }
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    data_ = data;
    decoder_ = impala::BitReader(data, len);
    values_current_block_ = 0;
    values_current_mini_block_ = 0;
    len_ = -len;
    readBlockHeader();
    GetSize();
  }

  virtual int GetSize() {
    if (len_ < 0) {
      int i = 1;
      len_ = 0 - len_;
      int len = len_ - decoder_.bytes_left();
      while ( i < values_current_block_) {
        int64_t dummy;
        impala::BitReader rd = impala::BitReader(data_ + len,  len_ - len);
        rd.GetZigZagVlqInt(&dummy);
        int value_bytes = 0;
        for (int j = 0; j < num_mini_blocks_; ++j) {
          uint8_t bit_width = 0;
          if (!rd.GetAligned<uint8_t>(1, &bit_width)) {
            ParquetException::EofException();
          }
          value_bytes += (bit_width * values_per_mini_block_) >> 3;
          i += values_per_mini_block_;
        }
        len = len_ - rd.bytes_left();
        len += value_bytes;
      }
      len_ = len;
    }
    return len_;
  }

  virtual int GetInt32(int32_t* buffer, int max_values) {
    return GetInternal(buffer, max_values);
  }

  virtual int GetInt64(int64_t* buffer, int max_values) {
    return GetInternal(buffer, max_values);
  }

 private:
  void readBlockHeader() {
    if (!decoder_.GetVlqInt(&block_size_)) ParquetException::EofException();
    if (!decoder_.GetVlqInt(&num_mini_blocks_)) ParquetException::EofException();
    if (!decoder_.GetVlqInt(&values_current_block_)) {
      ParquetException::EofException();
    }
    if (!decoder_.GetZigZagVlqInt(&last_value_)) ParquetException::EofException();
    values_per_mini_block_ = block_size_ / num_mini_blocks_;
    is_first_value_ = true;
    mini_block_buffer_.resize(values_per_mini_block_);
  }

  void InitBlock() {
    delta_bit_widths_.resize(num_mini_blocks_);

    if (!decoder_.GetZigZagVlqInt(&min_delta_)) ParquetException::EofException();
    for (int i = 0; i < num_mini_blocks_; ++i) {
      if (!decoder_.GetAligned<uint8_t>(1, &delta_bit_widths_[i])) {
        ParquetException::EofException();
      }
    }
    mini_block_idx_ = 0;
    delta_bit_width_ = delta_bit_widths_[0];
    values_current_mini_block_ = values_per_mini_block_;
  }

  bool decodeMiniBlock() {
    int bytes = (delta_bit_width_ * values_per_mini_block_) >> 3;
    uint8_t mini_block[bytes];
    decoder_.GetAligned<uint8_t>(bytes, mini_block);
    uint32_t deltas[values_per_mini_block_];
    int i = 0;
    int r = 0;
    uint8_t* pblock = mini_block;
    do {
      r = unpack_8<uint32_t>(delta_bit_width_, pblock, deltas+i);
      if (r > 0) { pblock += r; i+=8; }
    } while(r > 0 && i < values_per_mini_block_);
    if ( r < 0) return false;
    values_current_mini_block_ = 0;
    for(i=0; i<values_per_mini_block_;++i) {
      int delta = deltas[i] + min_delta_;
      last_value_ += delta;
      mini_block_buffer_[values_current_mini_block_] = last_value_;
      values_current_mini_block_ ++;
    }
    return false;
  }

  template <typename T>
  int GetInternal(T* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    int i = 0;
    if (is_first_value_ && max_values > 0) {
      is_first_value_ = false;
      buffer[0] = last_value_;
      ++i;
    }
    for (; i < max_values; ++i) {
      if (UNLIKELY(values_current_mini_block_ == 0)) {
        ++mini_block_idx_;
        if (mini_block_idx_ < delta_bit_widths_.size()) {
          delta_bit_width_ = delta_bit_widths_[mini_block_idx_];
          values_current_mini_block_ = values_per_mini_block_;
          decodeMiniBlock();
        } else {
          InitBlock();
          decodeMiniBlock();
          --i;
          continue;
        }
      }

      // TODO: the key to this algorithm is to decode the entire miniblock at once.
      /*
      int64_t delta;
      if (!decoder_.GetValue(delta_bit_width_, &delta)) ParquetException::EofException();
      delta += min_delta_;
      last_value_ += delta; */
      buffer[i] = mini_block_buffer_[values_per_mini_block_ - values_current_mini_block_];
      --values_current_mini_block_;
    }
    num_values_ -= max_values;
    return max_values;
  }

  impala::BitReader decoder_;
  const uint8_t* data_;
  uint64_t block_size_;
  uint64_t values_current_block_;
  uint64_t num_mini_blocks_;
  uint64_t values_per_mini_block_;
  uint64_t values_current_mini_block_;

  int64_t min_delta_;
  int mini_block_idx_;
  std::vector<uint8_t> delta_bit_widths_;
  std::vector<uint32_t> mini_block_buffer_;
  int delta_bit_width_;

  bool is_first_value_;
  int64_t last_value_;
  int len_;
};

}

#endif

