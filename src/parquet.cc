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

#include "parquet/parquet.h"
#include "encodings/encodings.h"
#include "compression/codec.h"
#include "impala/bit-util.h"
#include "read_support.h"

#include <string>
#include <algorithm>
#include <string.h>

#include <thrift/protocol/TDebugProtocol.h>

const int DATA_PAGE_SIZE = 64 * 1024;

using namespace boost;
using namespace parquet;
using namespace std;
using impala::BitUtil;

namespace parquet_cpp {

inline size_t value_byte_size(Type::type t) {
  switch (t) {
  case parquet::Type::BOOLEAN: return 1;
  case parquet::Type::INT32: return sizeof(int32_t);
  case parquet::Type::INT64: return sizeof(int64_t);
  case parquet::Type::FLOAT: return sizeof(float);
  case parquet::Type::DOUBLE: return sizeof(double);
  case parquet::Type::BYTE_ARRAY: return sizeof(ByteArray);
  default:
    ParquetException::NYI("Unsupported type");
  }
}

InMemoryInputStream::InMemoryInputStream(const uint8_t* buffer, int64_t len) :
  buffer_(buffer), len_(len), offset_(0) {
}

const uint8_t* InMemoryInputStream::Peek(int num_to_peek, int* num_bytes) {
  *num_bytes = ::min(static_cast<int64_t>(num_to_peek), len_ - offset_);
  return buffer_ + offset_;
}

const uint8_t* InMemoryInputStream::Read(int num_to_read, int* num_bytes) {
  const uint8_t* result = Peek(num_to_read, num_bytes);
  offset_ += *num_bytes;
  return result;
}

ColumnReader::~ColumnReader() {
}

ColumnReader::ColumnReader(const ColumnMetaData* metadata,
    //const SchemaElement* schema, 
    InputStream* stream,
    const ColumnDescriptor& desc)
    //int max_repetition_level,
    ///int max_definition_level)
  : metadata_(metadata),
    column_(desc),
    //schema_(schema),
    stream_(stream),
    //max_repetition_level_(max_repetition_level),
    //max_definition_level_(max_definition_level),
    current_decoder_(NULL),
    num_buffered_values_(0),
    num_decoded_values_(0),
    buffered_values_offset_(0),
    saved_rep_level_(-1) {
  max_repetition_level_ = column_.max_rep_level;
  max_definition_level_ = column_.max_def_level;
  switch (metadata->codec) {
    case CompressionCodec::UNCOMPRESSED:
      break;
    case CompressionCodec::SNAPPY:
      decompressor_.reset(new SnappyCodec());
      break;
    default:
      ParquetException::NYI("Reading compressed data");
  }

  config_ = Config::DefaultConfig();
  values_buffer_.resize(config_.batch_size * value_byte_size(metadata->type));
}

void ColumnReader::BatchDecode() {
  buffered_values_offset_ = 0;
  uint8_t* buf= &values_buffer_[0];
  int batch_size = config_.batch_size;
  switch (metadata_->type) {
    case parquet::Type::BOOLEAN:
      num_decoded_values_ =
          current_decoder_->GetBool(reinterpret_cast<bool*>(buf), batch_size);
      break;
    case parquet::Type::INT32:
      num_decoded_values_ =
          current_decoder_->GetInt32(reinterpret_cast<int32_t*>(buf), batch_size);
      break;
    case parquet::Type::INT64:
      num_decoded_values_ =
          current_decoder_->GetInt64(reinterpret_cast<int64_t*>(buf), batch_size);
      break;
    case parquet::Type::FLOAT:
      num_decoded_values_ =
          current_decoder_->GetFloat(reinterpret_cast<float*>(buf), batch_size);
      break;
    case parquet::Type::DOUBLE:
      num_decoded_values_ =
          current_decoder_->GetDouble(reinterpret_cast<double*>(buf), batch_size);
      break;
    case parquet::Type::BYTE_ARRAY:
      num_decoded_values_ =
          current_decoder_->GetByteArray(reinterpret_cast<ByteArray*>(buf), batch_size);
      break;
    default:
      ParquetException::NYI("Unsupported type.");
  }
}

// PLAIN_DICTIONARY is deprecated but used to be used as a dictionary index
// encoding.
static bool IsDictionaryIndexEncoding(const Encoding::type& e) {
  return e == Encoding::RLE_DICTIONARY || e == Encoding::PLAIN_DICTIONARY;
}

bool ColumnReader::ReadNewPage() {
  // Loop until we find the next data page.

  while (true) {
    int bytes_read = 0;
    const uint8_t* buffer = stream_->Peek(DATA_PAGE_SIZE, &bytes_read);
    if (bytes_read == 0) return false;
    uint32_t header_size = bytes_read;
    DeserializeThriftMsg(buffer, &header_size, &current_page_header_);
    stream_->Read(header_size, &bytes_read);

    int compressed_len = current_page_header_.compressed_page_size;
    int uncompressed_len = current_page_header_.uncompressed_page_size;

    // Read the compressed data page.
    buffer = stream_->Read(compressed_len, &bytes_read);
    if (bytes_read != compressed_len) ParquetException::EofException();


    if (current_page_header_.type == PageType::DICTIONARY_PAGE ||
        current_page_header_.type == PageType::DATA_PAGE) {
      // Uncompress it if we need to
      if (decompressor_ != NULL) {
        // Grow the uncompressed buffer if we need to.
        if (uncompressed_len > decompression_buffer_.size()) {
          decompression_buffer_.resize(uncompressed_len);
        }
        decompressor_->Decompress(
            compressed_len, buffer, uncompressed_len, &decompression_buffer_[0]);
        buffer = &decompression_buffer_[0];
      }
    }

    if (current_page_header_.type == PageType::DICTIONARY_PAGE) {
      unordered_map<Encoding::type, shared_ptr<Decoder> >::iterator it =
          decoders_.find(Encoding::RLE_DICTIONARY);
      if (it != decoders_.end()) {
        throw ParquetException("Column cannot have more than one dictionary.");
      }

      PlainDecoder dictionary(column_.element.type);
      dictionary.SetData(current_page_header_.dictionary_page_header.num_values,
          buffer, uncompressed_len);
      shared_ptr<Decoder> decoder(new DictionaryDecoder(column_.element.type, &dictionary));
      decoders_[Encoding::RLE_DICTIONARY] = decoder;
      current_decoder_ = decoders_[Encoding::RLE_DICTIONARY].get();
      continue;
    } else if (current_page_header_.type == PageType::DATA_PAGE) {
      // Read a data page.
      const DataPageHeader& data_page_header = current_page_header_.data_page_header;
      num_buffered_values_ = data_page_header.num_values;

      // repetition levels
      if (max_repetition_level_ > 0) {
        int num_repetition_bytes = *reinterpret_cast<const uint32_t*>(buffer);
        int bitwidth = BitUtil::NumRequiredBits(max_repetition_level_);
        buffer += sizeof(uint32_t);
        repetition_level_decoder_.reset(
            new impala::RleDecoder(buffer, num_repetition_bytes, bitwidth));
        buffer += num_repetition_bytes;
        uncompressed_len -= sizeof(uint32_t);
        uncompressed_len -= num_repetition_bytes;
      }

      // definition levels.
      if (max_definition_level_ > 0) {
        int num_definition_bytes = *reinterpret_cast<const uint32_t*>(buffer);
        int bitwidth = BitUtil::NumRequiredBits(max_definition_level_);
        buffer += sizeof(uint32_t);
        definition_level_decoder_.reset(
            new impala::RleDecoder(buffer, num_definition_bytes, bitwidth));
        buffer += num_definition_bytes;
        uncompressed_len -= sizeof(uint32_t);
        uncompressed_len -= num_definition_bytes;
      }

      // Get a decoder object for this page or create a new decoder if this is the
      // first page with this encoding.
      Encoding::type encoding = data_page_header.encoding;
      if (IsDictionaryIndexEncoding(encoding)) encoding = Encoding::RLE_DICTIONARY;

      unordered_map<Encoding::type, shared_ptr<Decoder> >::iterator it =
          decoders_.find(encoding);
      if (it != decoders_.end()) {
        current_decoder_ = it->second.get();
      } else {
        shared_ptr<Decoder> decoder;
        switch (encoding) {
          case Encoding::PLAIN: {
            if (column_.element.type == Type::BOOLEAN) {
              decoder.reset(new BoolDecoder());
            } else {
              decoder.reset(new PlainDecoder(column_.element.type));
            }
            decoders_[encoding] = decoder;
            current_decoder_ = decoder.get();
            break;
          }
          case Encoding::RLE_DICTIONARY:
            throw ParquetException("Dictionary page must be before data page.");

          case Encoding::DELTA_BINARY_PACKED: {
            decoder.reset(new DeltaBitPackDecoder(column_.element.type));
            decoders_[encoding] = decoder;
            current_decoder_ = decoder.get();
            break;
          }
          case Encoding::DELTA_BYTE_ARRAY: {
            decoder.reset(new DeltaByteArrayDecoder());
            decoders_[encoding] = decoder;
            current_decoder_ = decoder.get();
            break;
          }
          case Encoding::DELTA_LENGTH_BYTE_ARRAY:
            ParquetException::NYI("Unsupported encoding");

          default:
            throw ParquetException("Unknown encoding type.");
        }
      }
      current_decoder_->SetData(num_buffered_values_, buffer, uncompressed_len);
      return true;
    } else if (current_page_header_.type == PageType::DATA_PAGE_V2) {
      // Read data page v2
      const DataPageHeaderV2& data_page_header = current_page_header_.data_page_header_v2;
      num_buffered_values_ = data_page_header.num_values - data_page_header.num_nulls;

      int repetition_levels_byte_length = data_page_header.repetition_levels_byte_length;
      // repetition levels
      if (data_page_header.repetition_levels_byte_length > 0) {
        int bitwidth = BitUtil::NumRequiredBits(max_repetition_level_);
        repetition_level_decoder_.reset(new impala::RleDecoder(buffer, 
            data_page_header.repetition_levels_byte_length, bitwidth));
        buffer += data_page_header.repetition_levels_byte_length;
        uncompressed_len -= data_page_header.repetition_levels_byte_length;
        compressed_len -= data_page_header.repetition_levels_byte_length;
      }

      //definition levels.
      if (data_page_header.definition_levels_byte_length > 0) {
        int bitwidth = BitUtil::NumRequiredBits(max_definition_level_);
        definition_level_decoder_.reset(new impala::RleDecoder(buffer, 
            data_page_header.definition_levels_byte_length, bitwidth));
        buffer += data_page_header.definition_levels_byte_length;
        uncompressed_len -= data_page_header.definition_levels_byte_length;
        compressed_len -= data_page_header.definition_levels_byte_length;
      }

      if ((!data_page_header.__isset.is_compressed) || data_page_header.is_compressed) {
        if (uncompressed_len > decompression_buffer_.size()) {
          decompression_buffer_.resize(uncompressed_len);
        }
        if (decompressor_.get() != NULL) {
          decompressor_->Decompress(
              compressed_len, buffer, uncompressed_len, &decompression_buffer_[0]);
          buffer = &decompression_buffer_[0];
        }
      }

      // Get a decoder object for this page or create a new decoder if this is the
      // first page with this encoding.
      Encoding::type encoding = data_page_header.encoding;
      if (IsDictionaryIndexEncoding(encoding)) encoding = Encoding::RLE_DICTIONARY;

      unordered_map<Encoding::type, shared_ptr<Decoder> >::iterator it =
        decoders_.find(encoding);
      if (it != decoders_.end()) {
        current_decoder_ = it->second.get();
      } else {
        shared_ptr<Decoder> decoder;
        switch (encoding) {
          case Encoding::PLAIN: {
            if (column_.element.type == Type::BOOLEAN) {
              decoder.reset(new BoolDecoder());
            } else {
              decoder.reset(new PlainDecoder(column_.element.type));
            }
            decoders_[encoding] = decoder;
            current_decoder_ = decoder.get();
            break;
          }
          case Encoding::RLE_DICTIONARY:
            throw ParquetException("Dictionary page must be before data page.");

          case Encoding::DELTA_BINARY_PACKED: {
            decoder.reset(new DeltaBitPackDecoder(column_.element.type));
            decoders_[encoding] = decoder;
            current_decoder_ = decoder.get();
            break;
          }
          case Encoding::DELTA_BYTE_ARRAY: {
            decoder.reset(new DeltaByteArrayDecoder());
            decoders_[encoding] = decoder;
            current_decoder_ = decoder.get();
            break;
          }
          case Encoding::DELTA_LENGTH_BYTE_ARRAY:
            ParquetException::NYI("Unsupported encoding");

          default:
            throw ParquetException("Unknown encoding type.");
        }
      }
      current_decoder_->SetData(num_buffered_values_, buffer, uncompressed_len);
      return true;
    } else {
      // We don't know what this page type is. We're allowed to skip non-data pages.
      continue;
    }
  }
  return true;
}

int ColumnReader::DecodeValues(uint8_t* val_, vector<uint8_t>& buf, int count) {
  switch (column_.element.type) {
  case Type::BOOLEAN: return current_decoder_->GetBool((bool*)val_, count);
  case Type::INT32: return current_decoder_->GetInt32((int32_t*)val_, count);
  case Type::INT64: return current_decoder_->GetInt64((int64_t*)val_, count);
  case Type::DOUBLE: return current_decoder_->GetDouble((double*)val_, count);
  case Type::BYTE_ARRAY: {
    ByteArray* val = (ByteArray*)val_;
    count = current_decoder_->GetByteArray(val, count);
    size_t pos = buf.size();
    size_t buf_size = 0;
    for (int i=0; i<count; ++i)
      buf_size += val[i].len;
    buf.resize(buf.size() + buf_size);
    for (int i=0; i<count; ++i)
      memcpy(&buf[pos], val[i].ptr, val[i].len);
    return count;
  }
  default:
    ParquetException::NYI("Unsupported type");
  }
  return -1;
}

bool ColumnReader::ReadDefinitionRepetitionLevels(int* def_level, int* rep_level) {
  *def_level = nextDefinitionLevel();
  *rep_level = nextRepetitionLevel();
  --num_buffered_values_;
  return *def_level < max_definition_level_;
}

bool ColumnReader::peekDefinitionRepetitionLevels(int* def_level, int* rep_level) {
  *def_level = nextDefinitionLevel();
  *rep_level = nextRepetitionLevel();
  return true;
}

int ColumnReader::nextDefinitionLevel() {
  if (max_definition_level_ > 0) {
    int val = 0;
    if (!definition_level_decoder_->Get(&val)) {
      return 0;
    }
    return val;
  }
  return 0;
}

int ColumnReader::peekRepetitionLevel() {
  if (saved_rep_level_ >= 0)
    return saved_rep_level_;
  saved_rep_level_ = nextRepetitionLevel();
  return saved_rep_level_;
}

int ColumnReader::nextRepetitionLevel() {
  if (saved_rep_level_ >= 0) {
    int t = saved_rep_level_;
    saved_rep_level_ = -1;
    return t;
  }
  if (max_repetition_level_ > 0) {
    int rep_lvl = 0;
    if (!repetition_level_decoder_->Get(&rep_lvl)) {
      return 0;
    }
    return rep_lvl;
  }
  return 0;
}

int ColumnReader::skipValue(int values) {
  int buf_bump = std::min(values, 
          num_decoded_values_ - buffered_values_offset_);
  buffered_values_offset_ += buf_bump;

  if (values > buf_bump) {
    return current_decoder_->skip(values - buf_bump);
  }
  return values;
}

int ColumnReader::copyValues(vector<uint8_t>& buf, int max_values) {
  int buf_bump = std::min(max_values,
	    num_decoded_values_ - buffered_values_offset_);
  max_values -= buf_bump;
  uint8_t* pvalbuf = &values_buffer_[0];
  int values = 0;
  num_buffered_values_ -= buf_bump;
  switch (metadata_->type) {
    case parquet::Type::BOOLEAN: {
      buf.resize(max_values);
      bool* pa = reinterpret_cast<bool*>(pvalbuf) + buffered_values_offset_;
      buf.assign(pa, pa+buf_bump);
      values = current_decoder_->GetBool(
    		  reinterpret_cast<bool*>(&buf[0])+buf_bump, max_values);
    }; break;
    case parquet::Type::INT32: {
      buf.resize(max_values*sizeof(int32_t));
      int32_t* pa = reinterpret_cast<int32_t*>(pvalbuf) + buffered_values_offset_;
      buf.assign(pa, pa+buf_bump);
      values = current_decoder_->GetInt32(
    		  reinterpret_cast<int32_t*>(&buf[0])+buf_bump, max_values);
    }; break;
    case parquet::Type::INT64: {
        buf.resize(max_values*sizeof(int64_t));
        int64_t* pa = reinterpret_cast<int64_t*>(pvalbuf) + buffered_values_offset_;
        buf.assign(pa, pa+buf_bump);
        values = current_decoder_->GetInt64(
        		reinterpret_cast<int64_t*>(&buf[0])+buf_bump, max_values);
      }; break;
    case parquet::Type::FLOAT:  {
        buf.resize(max_values*sizeof(float));
        float* pa = reinterpret_cast<float*>(pvalbuf) + buffered_values_offset_;
        buf.assign(pa, pa+buf_bump);
        values = current_decoder_->GetFloat(
        		reinterpret_cast<float*>(&buf[0])+buf_bump, max_values);
      }; break;
    case parquet::Type::DOUBLE: {
        buf.resize(max_values*sizeof(double));
        double* pa = reinterpret_cast<double*>(pvalbuf) + buffered_values_offset_;
        buf.assign(pa, pa+buf_bump);
        values = current_decoder_->GetDouble(
        		reinterpret_cast<double*>(&buf[0])+buf_bump, max_values);
      }; break;
    case parquet::Type::BYTE_ARRAY:{
        buf.resize(max_values*sizeof(ByteArray));
        double* pa = reinterpret_cast<double*>(pvalbuf) + buffered_values_offset_;
        buf.assign(pa, pa+buf_bump);
        values = current_decoder_->GetByteArray(
        		reinterpret_cast<ByteArray*>(&buf[0])+buf_bump, max_values);
      }; break;
    default:
      ParquetException::NYI("Unsupported type.");
  }
  num_buffered_values_ -= values;
  return values;
}

int ColumnReader::skipCurrentRecord() {
  if (!HasNext())
    return 0;

  int def_lvl = 0, rep_lvl = 0;
  int values = 0;
  //ReadDefinitionRepetitionLevels(&def_lvl, &rep_lvl);
  def_lvl = nextDefinitionLevel();
  rep_lvl = nextRepetitionLevel();
  --num_buffered_values_;
  if (def_lvl == max_definition_level_) values ++;

  do {
    rep_lvl = nextRepetitionLevel();
    if (rep_lvl > 0) {
      def_lvl = nextDefinitionLevel();
      --num_buffered_values_;
      if(def_lvl == max_definition_level_)
        values ++;
    }
  } while (rep_lvl > 0);
  if (values > 0) {
    skipValue(values);
  }
  return (HasNext())? 1: 0;
}

int ColumnReader::decodeRepetitionLevels(
    vector<int32_t>& buf,
    int value_count)
{
  int values = min(num_buffered_values_, value_count);
  buf.resize(values);
  for (int i=0; i<values; ++i) {
    if (!repetition_level_decoder_->Get(&buf[i]))
      break;
  }
  return values;
}

// definition_levels
int ColumnReader::decodeValues(std::vector<uint8_t>& buf,
                 std::vector<int32_t>& definition_levels,
                 int value_count)
{
  int values = min(num_buffered_values_, value_count);
  definition_levels.resize(values);
  int num_nonnulls = values;
  if (max_definition_level_ > 0) {
    int num_nulls = 0;
    for(int i=0; i<values; ++i) {
      int v = 0;
      if (!definition_level_decoder_->Get(&definition_levels[i])) {
        break;
      }
      if (definition_levels[i] < max_definition_level_)
        num_nulls ++;
    }
    num_nonnulls = values - num_nulls;
  } else {
    memset(&definition_levels[0], sizeof(int32_t)*values, 0);
  }
  copyValues(buf, num_nonnulls);
  return values;
}

int ColumnReader::GetValueBatch(ValueBatch& batch, int max_values) {
  batch.max_def_level_ = max_definition_level_;
  batch.rep_levels_.resize(max_values);
  batch.def_levels_.resize(max_values);
  batch.resize(max_values);

  int def_values = 0;
  int num_nonnulls = 0;

  if (max_definition_level_ > 0) {
    int num_nulls = 0;
    int state = 0, start_p = 0;
    for (int i=0; i<max_values; ++i) {
      if (!definition_level_decoder_->Get(&batch.def_levels_[i]))
        break;
      def_values ++;
      bool is_null = batch.def_levels_[i] < max_definition_level_;
      if (is_null) num_nulls ++;
      switch (state) {
      case 0:
        if (is_null) state = 2;
        else { state = 1; start_p = i; }
        break;
      case 1:
        if (is_null) {
          DecodeValues(batch.valueAddress(start_p), batch.buffer_, i - start_p);
          state = 2;
        } break;
      case 2:
        if (!is_null) { state = 1; start_p = i; }
      }
    }
    if ( state == 1 )
      DecodeValues(batch.valueAddress(start_p), batch.buffer_, def_values - start_p);
  } else {
    def_values = max_values;
    memset(&batch.def_levels_[0], 0, sizeof(int32_t) * def_values);
    def_values = DecodeValues(batch.valueAddress(0), batch.buffer_, max_values);
  }
  int rep_values = def_values;
  if (repetition_level_decoder_) {
    rep_values = decodeRepetitionLevels(batch.rep_levels_, rep_values);
  } else {
    rep_values = max_values;
    memset(&batch.rep_levels_[0], 0, sizeof(int32_t) * rep_values);
  }
  return def_values;
}
int ColumnReader::GetRecordValueBatch(ValueBatch& batch, 
  vector<int>& record_offsets, int num_records)
{
  int num_values = num_records;
  if (max_repetition_level_ > 0) {
    vector<int32_t> buf;
    buf.reserve(num_records);
    record_offsets.reserve(num_records);
    do {
      int rep_val = 0;
      if (!repetition_level_decoder_->Get(&rep_val)) break;
      if (rep_val == 0) record_offsets.push_back((int32_t)buf.size());
      buf.push_back(rep_val);
    } while (record_offsets.size() < num_records);
    num_values = buf.size();
    batch.rep_levels_.swap(buf);
  }

  batch.resize(num_values);

  if (max_definition_level_ > 0) {
    int state = 0, start_p = 0;
    int values = 0;
    for (int i=0; i<num_values; ++i) {
      if (!definition_level_decoder_->Get(&batch.def_levels_[i]))
        break;
      values ++; 
      bool is_null = batch.def_levels_[i] < max_definition_level_;
      switch (state) {
      case 0: if (is_null) state = 2;
              else { state = 1; start_p = i; } break;
      case 1: if (is_null) {
                DecodeValues(batch.valueAddress(start_p), batch.buffer_, i - start_p);
                state = 2;
              } break;
      case 2: if (!is_null) { state = 1; start_p = i; }
      }
    }
    if ( state == 1 )
      DecodeValues(batch.valueAddress(start_p), batch.buffer_, values - start_p);
    num_values = values;
  } else {
    memset(&batch.def_levels_[0], 0, sizeof(int32_t) * num_values);
    num_values = DecodeValues(batch.valueAddress(0), batch.buffer_, num_values);
  }
  if (max_repetition_level_ == 0) {
    batch.rep_levels_.resize(num_values);
    memset(&batch.rep_levels_[0], 0, sizeof(int32_t) * num_values);
    record_offsets.resize(num_values);
    for (int i=0; i<num_values; ++i)
      record_offsets[i] = i;
  }
  return num_values;
}

void ValueBatch::BindDescriptor(ColumnDescriptor& desc) {
  value_byte_size_ = value_byte_size(desc.element.type);
  max_def_level_ = desc.max_def_level;
}


SchemaHelper::SchemaHelper(const string& file_path) {
  parquet::FileMetaData metadata;
  if (!GetFileMetadata(file_path, &metadata))
    throw ParquetException("unable to open file");
  //schema = metadata.schema;
  columns.resize(metadata.schema.size());
  for (int i=0; i<metadata.schema.size(); ++i)
    columns[i].element = metadata.schema[i];
  init_();
}

SchemaHelper::SchemaHelper(vector<parquet::SchemaElement>& _schema) {
  columns.resize(_schema.size());
  for (int i=0; i<_schema.size(); ++i)
    columns[i].element = _schema[i];
  init_();
}

void SchemaHelper::init_() {
  _child_to_parent.resize(columns.size());
  _parent_to_child.resize(columns.size());
  _element_paths.resize(columns.size());
  _rebuild_tree(ROOT_NODE, 0, 0, "");
}

int SchemaHelper::_rebuild_tree(int fid, int rep_level, int def_level, 
  const string& path)
{
  const SchemaElement&  parent = columns[fid].element;
  string& fullpath = _element_paths[fid];
  if (fid != ROOT_NODE) {
    if (parent.repetition_type == FieldRepetitionType::REPEATED)
      rep_level += 1;
    if (parent.repetition_type != FieldRepetitionType::REQUIRED)
      def_level += 1;
    if (path.length() > 0) {
      fullpath.append(path);
      fullpath.append(".");
    }
    fullpath.append(parent.name);
    _path_to_id[fullpath] = fid;
  }
  columns[fid].max_def_level = def_level;
  columns[fid].max_rep_level = rep_level;
  if (!parent.__isset.num_children)
    return 1;

  int num_children = parent.num_children;
  int chd = fid + 1;
  while (num_children > 0) {
    num_children -= 1;
    _child_to_parent[chd] = fid;
    _parent_to_child[fid].push_back(chd);
    chd += _rebuild_tree(chd, rep_level, def_level, fullpath);
  }
  return chd - fid;
}

int SchemaHelper::_build_child_fsm(int fid){
  const vector<int>& child_ids = _parent_to_child[fid];
  for (int i=0; i<child_ids.size(); ++i) {
    int cid = child_ids[i];
    SchemaElement& element = columns[cid].element;
    int rep_level = columns[cid].max_rep_level;

    vector<edge_t>& subedges = _edges[cid];
    subedges.resize(rep_level+1);
    for (int r = 0; r <= rep_level; ++r) {
      if ( i + 1 == child_ids.size() ) {
        subedges[r].next = fid;
        subedges[r].type = (fid == ROOT_NODE)? STAY : FOLLOW;
      } else {
        subedges[r].next = child_ids[i+1];
        subedges[r].type = STAY;
      }
    }
    if (element.repetition_type == FieldRepetitionType::REPEATED)
      subedges[rep_level] = edge_t(cid, STAY);
    if (element.__isset.num_children)
      _build_child_fsm(cid);
  }
  return 0;
}

void SchemaHelper::BuildFullFSM() {
  if (_edges.size() != columns.size()) {
    _edges.resize(columns.size());
    _build_child_fsm(ROOT_NODE);
  }
}

void SchemaHelper::BuildFSM(const vector<string>& fields, SchemaFSM& fsm) {
  BuildFullFSM();
  vector<int> fids;
  if (fields.size() == 0) {
    for (int i=0; i < columns.size(); ++i) {
      SchemaElement& element = columns[i].element;
      if (!element.__isset.num_children)
        fids.push_back(i);
    }
  } else {
    for(int i=0; i<fields.size(); ++i) {
      map<string, int>::const_iterator itr = 
          _path_to_id.find(fields[i]);
      if (itr != _path_to_id.end())
        fids.push_back(itr->second);
    }
    sort(fids.begin(), fids.end());
  }
  vector<vector<int> > all_edges(columns.size());
  for (int i = 0; i < fids.size(); ++i) {
    int id = fids[i];
    int rep_lvl = GetMaxRepetitionLevel(id);
    vector<int>& edge = all_edges[id];
    edge.resize(1 + rep_lvl);
    for( int r = 0; r <= rep_lvl; ++r) {
      int tsid = _compress_state(id, r, fids);
      edge[r] = tsid;
    }
  }
  all_edges[ROOT_NODE].push_back(fids[0]);
  fsm.init(all_edges);
}

int SchemaHelper::_follow_fsm(int field_id, int rep_lvl) {
  vector<edge_t>& edge = _edges[field_id];
  edge_t ts = edge[rep_lvl];
  int ts_id = ts.next;
  if (ts_id == ROOT_NODE) return ts_id;
  if (ts.type == FOLLOW)
    return _follow_fsm(ts_id, rep_lvl);
  else {
    //SchemaElement& element = columns[ts_id];
    while (columns[ts_id].element.__isset.num_children) {
      ts_id = ts_id + 1;
    }
    return ts_id;
  }
}

int SchemaHelper::_compress_state(int fid, int rep_lvl, 
  const vector<int>& fields)
{
  if (!binary_search(fields.begin(), fields.end(), fid))
    return ROOT_NODE;
  int tsid = _follow_fsm(fid, rep_lvl);
  while ((tsid != ROOT_NODE) && (!binary_search(fields.begin(), fields.end(), tsid))) {
    fid = tsid;
    tsid = _follow_fsm(fid, rep_lvl);
  }
  return tsid;
}

void SchemaFSM::dump(ostream& oss) const {
  for (int i=0; i<states_.size(); ++i) {
    const vector<int>& substates = states_[i];
    if (substates.size() == 0)
      continue;
    oss << i << " : {" ;
    for (int j=0; j < substates.size(); ++j) {
      oss << " " << j << ":" << substates[j];
    }
    oss << "}\n";
  }
}

/*
ColumnChunkGenerator::ColumnChunkGenerator(const string& file_path, const string& col_path):
  file_path_(file_path), row_group_idx_(0), col_path_(col_path)
{
  if (!GetFileMetadata(file_path_, &metadata_))
    throw ParquetException("unable to open file");
  helper_.reset(new SchemaHelper(metadata_.schema));
  col_idx_ = helper_->GetElementId(col_path);
  if (col_idx_ < 0)
    throw ParquetException("invalid column");

  max_definition_level_ = helper_->GetMaxDefinitionLevel(col_idx_);
  max_repetition_level_ = helper_->GetMaxRepetitionLevel(col_idx_);
}
*/
ParquetFileReader::ParquetFileReader(const string& path)
 : file_path_(path) 
{
  if (!GetFileMetadata(file_path_, &metadata_))
    throw ParquetException("unable to open file");

  readers_.resize(metadata_.schema.size());
  chunk_generators_.resize(metadata_.schema.size());
  values_.resize(metadata_.schema.size());
  helper_.reset(new SchemaHelper(metadata_.schema));
}

const ColumnDescriptor& 
ParquetFileReader::GetColumnDescriptor(int column_id) {
  return helper_->columns[column_id];
}

ValueBatch& ParquetFileReader::GetColumnValues(int column_id) {
  return values_[column_id];
}

int ParquetFileReader::LoadColumnData(int fid, int num_records) {
  if (readers_[fid].get() == NULL) {
    chunk_generators_[fid].reset(new ColumnChunkGenerator(file_path_, metadata_, 
      *helper_));
    chunk_generators_[fid]->selectColumn(fid);
    if (!chunk_generators_[fid]->next(readers_[fid])) 
      return 0;
    if (!readers_[fid]->HasNext())
      return 0;
  }
  int records_remains = num_records;
  int values = 0;
  vector<int> offsets;
  const ColumnDescriptor& desc = helper_->columns[fid];
  ColumnReader& reader = *readers_[fid];
  do {
    values += reader.GetRecordValueBatch(values_[fid], offsets, records_remains);
    if (offsets.size() == num_records) break;
    if (!chunk_generators_[fid]->next(readers_[fid])) break;
    records_remains = num_records - offsets.size();
  } while (records_remains > 0);
  return values;
}

/*
ColumnChunkGenerator::ColumnChunkGenerator(const string& file_path)
 : file_path_(file_path), row_group_idx_(0) {
  if (!GetFileMetadata(file_path_, &metadata_))
    throw ParquetException("unable to open file");
  
} */

ColumnChunkGenerator::ColumnChunkGenerator(const string& file_path, 
  parquet::FileMetaData& fmd, SchemaHelper& helper):
  file_path_(file_path), metadata_(fmd), helper_(helper), row_group_idx_(0)
{
/*
  if (!GetFileMetadata(file_path_, &metadata_))
    throw ParquetException("unable to open file");
  if (col_idx >= metadata_.schema.size())
    throw ParquetException("invalid column");

  helper_.reset(new SchemaHelper(metadata_.schema));
*/
  //col_idx_ = col_idx;
  //max_definition_level_ = helper_->GetMaxDefinitionLevel(col_idx_);
  //max_repetition_level_ = helper_->GetMaxRepetitionLevel(col_idx_);
  //col_path_ = helper_.GetElementPath(col_idx);
}


string build_path_name(const vector<string>& path_in_schema) {
  stringstream ss;
  for (int i=0; i<path_in_schema.size(); ++i) {
    if (i>0)
      ss << ".";
    ss << path_in_schema[i];
  }
  return ss.str();
}

void ColumnChunkGenerator::selectColumn(int col_idx) {
  col_idx_ = col_idx;
  col_path_ = helper_.GetElementPath(col_idx);
}

bool ColumnChunkGenerator::next(shared_ptr<ColumnReader>& reader)
{
  if (row_group_idx_ >= metadata_.row_groups.size())
    return false;
  const RowGroup& row_group = metadata_.row_groups[row_group_idx_];
  for (int col_idx = 0; col_idx < row_group.columns.size(); ++col_idx) {
    const ColumnChunk& col = row_group.columns[col_idx];
    string row_grp_pathname = build_path_name(col.meta_data.path_in_schema);
    if (col_path_.compare(row_grp_pathname) != 0) {
      continue;
    }
    string file_path = file_path_;
    if (col.file_path.length() > 0)
      file_path = col.file_path;
    size_t col_start = col.meta_data.data_page_offset;
    if (col.meta_data.__isset.dictionary_page_offset) {
      if (col_start > col.meta_data.dictionary_page_offset) {
        col_start = col.meta_data.dictionary_page_offset;
      }
    }
    size_t read_offset = col_start;
    size_t total_size = col.meta_data.total_compressed_size;

    scoped_ptr<InputStream> input(new MmapMemoryInputStream(file_path, col_start, total_size));

    //total_compressed_size might be incorrect, need to scan through the page headers
    //to find out the correct size
    int total_values = 0;
    total_size = 0;
    int num_read = 0;
    const uint8_t* buf = input->Peek(1, &num_read);
    while (total_values < col.meta_data.num_values) {
      int num_read = 0;
      parquet::PageHeader page_header;
      uint32_t header_size = col.meta_data.total_compressed_size - 
                             (buf - input->Peek(1, &num_read));
      DeserializeThriftMsg(buf, &header_size, &page_header);
      total_size += header_size + page_header.compressed_page_size;
      buf += header_size + page_header.compressed_page_size;

      if (page_header.type == PageType::DATA_PAGE) {
        total_values += page_header.data_page_header.num_values;
      } else if (page_header.type == PageType::DATA_PAGE_V2) {
        total_values += page_header.data_page_header_v2.num_values;
      }
    }

    if (total_size > col.meta_data.total_compressed_size) {
      input_.reset(new MmapMemoryInputStream(file_path, read_offset, total_size));
    } else {
      input_.swap(input);
    }

    column_metadata_ = col.meta_data;
    ColumnDescriptor& desc = helper_.columns[col_idx_];
    //desc.element = metadata_.schema[col_idx_];
    //desc.max_def_level = max_repetition_level_;
    //desc.max_rep_level = max_definition_level_;
    col_chunk_ = col; 
    reader.reset(new ColumnReader(&(col_chunk_.meta_data), 
      //&metadata_.schema[col_idx_],
      input_.get(), 
      desc));//, max_repetition_level_, max_definition_level_));
    ++ row_group_idx_;
    return true;
  }
  ++ row_group_idx_;
  return false;
}

void DumpSchema(ostream& oss, const vector<ColumnDescriptor>& columns) {
  list<int> child_stack;
  child_stack.push_front(0);
  oss << "message " << columns[0].element.name << " {\n";
  for (int col_idx = 1; col_idx < columns.size(); ++col_idx) {
    const SchemaElement& element = columns[col_idx].element;

    //indent
    for (int j=1; j<=child_stack.size(); ++j) cout << "  ";

    oss << element.repetition_type << " ";
    if (element.num_children == 0) {
        child_stack.front() --; 
        oss << element.type << " " 
            << element.name << "; /*"
            << col_idx << "*/\n";
    
        if (child_stack.front() == 0) {
          do {
            child_stack.pop_front();
            for (int j=1; j<=child_stack.size(); ++j) cout << "  ";
            cout << "}\n";
            if (!child_stack.empty())
              child_stack.front()--;
          } while (!child_stack.empty() && child_stack.front() == 0);
        }
      } else {
        oss << "group " << element.name << " {\n";
        child_stack.push_front(element.num_children);
      }
    }
    oss << "}\n";
  }
}

ostream& operator<<(ostream& oss, const parquet_cpp::ByteArray& a) {
  oss.write((const char*)a.ptr, a.len);
  return oss;
}

ostream& operator<<(ostream& oss, const parquet_cpp::Int96& v){ 
  oss << hex << v.i[0];
  oss << hex << v.i[1];
  oss << hex << v.i[2];
  return oss;
}

ostream& operator<<(ostream& oss, FieldRepetitionType::type t) {
  switch(t){
  case FieldRepetitionType::REQUIRED: oss << "required"; break;
  case FieldRepetitionType::OPTIONAL: oss << "optional"; break;
  case FieldRepetitionType::REPEATED: oss << "repeated"; break;
  }
  return oss;
}

ostream& operator<<(ostream& oss, parquet::Type::type t) {
  switch(t){
  case parquet::Type::BOOLEAN: oss << "boolean"; break;
  case parquet::Type::INT32: oss << "int32"; break;
  case parquet::Type::INT64: oss << "int64"; break;
  case parquet::Type::INT96: oss << "int96"; break;
  case parquet::Type::FLOAT: oss << "float"; break;
  case parquet::Type::DOUBLE: oss << "double"; break;
  case parquet::Type::BYTE_ARRAY: oss << "byte_array"; break;
  case parquet::Type::FIXED_LEN_BYTE_ARRAY: oss << "fixed_len_byte_array"; break;
  default: oss << "unknown-type(" << (int) t << ")";
  }
  return oss;
}

ostream& operator<<(ostream& oss, parquet::Encoding::type t) {
  switch(t) {
  case parquet::Encoding::PLAIN: oss << "plain"; break;
  case parquet::Encoding::PLAIN_DICTIONARY: oss << "plain-dictionary"; break;
  case parquet::Encoding::RLE: oss << "rle"; break;
  case parquet::Encoding::BIT_PACKED: oss << "bit-packed"; break;
  case parquet::Encoding::DELTA_BINARY_PACKED: oss << "delta-bin-packed"; break;
  case parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY: oss << "delta-len-byte-array"; break;
  case parquet::Encoding::DELTA_BYTE_ARRAY: oss << "delta-byte-array"; break;
  case parquet::Encoding::RLE_DICTIONARY: oss << "rle-dictionary"; break;
  default: oss << "unknown-encoding(" << (int) t << ")";
  }
  return oss;
}

