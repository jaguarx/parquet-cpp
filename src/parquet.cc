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
    InputStream* stream, const ColumnDescriptor& desc)
  : metadata_(metadata),
    column_(desc),
    stream_(stream),
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
  case Type::INT32:   return current_decoder_->GetInt32((int32_t*)val_, count);
  case Type::INT64:   return current_decoder_->GetInt64((int64_t*)val_, count);
  case Type::DOUBLE:  return current_decoder_->GetDouble((double*)val_, count);
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

int ColumnReader::skipValue(int values) {
  int buf_bump = std::min(values, 
          num_decoded_values_ - buffered_values_offset_);
  buffered_values_offset_ += buf_bump;

  if (values > buf_bump) {
    return current_decoder_->skip(values - buf_bump);
  }
  return values;
}

int ColumnReader::skipRecords(int num_records) {
  int num_values = num_records;
  int values = num_buffered_values_;

  if (max_repetition_level_ > 0) {
    num_values = 0;
    if (saved_rep_level_ >= 0) {
      saved_rep_level_ = -1;
      num_values ++;
    } 
    do {
      if (!repetition_level_decoder_->Get(&saved_rep_level_)) {
        saved_rep_level_ = -1;
        break;
      }
      if (saved_rep_level_ == 0) num_records --;
      if (num_records >=0) {
        num_values ++;
        values --;
      }
    } while (num_records >= 0 && values > 0);
    num_buffered_values_ -= num_values;
  }

  if (max_definition_level_ > 0) {
    int state = 0, start_p = 0;
    int values = 0;
    int def_level = 0;
    for (int i=0; i<num_values; ++i) {
      if (!definition_level_decoder_->Get(&def_level))
        break;
      bool is_null = def_level < max_definition_level_;
      if (!is_null) values++;
    }
    skipValue(values);
  } else {
    skipValue(num_values);
  }
  return num_values;
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
  int values = num_buffered_values_;
  if (max_repetition_level_ > 0) {
    vector<int32_t> buf;
    buf.reserve(num_records);

    if (saved_rep_level_ >= 0) {
      buf.push_back(saved_rep_level_);
      saved_rep_level_ = -1;
      values --;
    }
    record_offsets.reserve(num_records);
    while (record_offsets.size() <= num_records && values > 0) {
      if (!repetition_level_decoder_->Get(&saved_rep_level_)) break;
      if (saved_rep_level_ == 0) record_offsets.push_back((int32_t)buf.size());
      buf.push_back(saved_rep_level_);
      values --;
    }
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
  batch.rep_levels_.push_back(0);
  return num_values;
}

void ValueBatch::BindDescriptor(ColumnDescriptor& desc) {
  type_ = desc.element.type;
  if (desc.element.num_children == 0)
    value_byte_size_ = value_byte_size(desc.element.type);
  else 
    value_byte_size_ = 0;
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
  _id_paths.resize(columns.size());
  _rebuild_tree(ROOT_NODE, 0, 0, "");
  for(int fid = 0; fid < columns.size(); ++fid) {
    int parent = _child_to_parent[fid];
    vector<int>& path = _id_paths[fid];
    path.insert(path.begin(), parent);
    while (parent != ROOT_NODE) {
      parent = _child_to_parent[parent];
      path.insert(path.begin(), parent);
    }
  }
  _id_paths[0].clear();
}

int SchemaHelper::GetMaxDepth() const {
  int ret = 0;
  for (int i=0; i<_id_paths.size(); ++i)
    ret = _id_paths[i].size() < ret ? ret : _id_paths[i].size();
  return ret;
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

static int _lca(const vector<int>& p1, const vector<int>& p2) {
  int i = 0;
  while (i< p1.size() && i < p2.size() && p1[i] == p2[i])
    i++;
  return i;
}

int SchemaHelper::_parent_of_level(int fid, int lvl) {
  while (fid != 0) {
    if (columns[fid].element.repetition_type == FieldRepetitionType::REPEATED &&
        columns[fid].max_rep_level == lvl) {
      return fid;
    } else if (columns[fid].max_def_level >= lvl) {
      fid = _child_to_parent[fid];
    } else {
      return -1;
    }
  }
  return -1;
}

int SchemaHelper::_last_child_of_parent(int pid, const vector<int>& fids) {
  for(int i=fids.size()-1; i>=0; --i) {
    int id = fids[i];
    const vector<int>& p = _id_paths[id];
    int found = 0;
    for (int j = 0; j < p.size(); ++j)
      found += (p[j] == pid) ? 1 : 0;
    if (found > 0)
      return id;
  }
  return 0;
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

  vector<vector<int> > level_to_close(columns.size());
  vector<vector<int> > all_edges(columns.size());

  for (int i = 0; i < fids.size(); ++i) {
    int id = fids[i];
    int rep_lvl = GetMaxRepetitionLevel(id);
    vector<int>& edge = all_edges[id];
    edge.resize(1 + rep_lvl);
    vector<int>& p1 = _id_paths[id];
    level_to_close[id].resize(rep_lvl+1);

    for( int r = 0; r <= rep_lvl; ++r) {
      int tsid = _compress_state(id, r, fids);
      edge[r] = tsid;

      if (i == (fids.size()-1) && r == 0) {
        level_to_close[id][r] = 0;
      } else {
        int pid = _parent_of_level(id, r);
        int lcid = -1;
        if (pid != -1) lcid = _last_child_of_parent(pid, fids);

        if (lcid == id) { // last child of rep_lvl
          level_to_close[id][r] = _id_paths[pid].size() - 1;
        } else {
          vector<int>& p2 = _id_paths[tsid];
          int lca_lvl = _lca(p1, p2);
          level_to_close[id][r] = lca_lvl - 1;
        }
      }
      //printf("%2d, %2d, %2d => %2d\n", id, r, tsid, level_to_close[id][r]);
    }
  }

  vector<vector<int> > deflvl_to_depth;
  deflvl_to_depth.resize(columns.size());
  for(int i=0; i<fids.size(); ++i) {
    int id = fids[i];
    const vector<int>& p1 = _id_paths[id];
    int depth = 0;
    vector<int>& depth_list = deflvl_to_depth[id];
    depth_list.resize(GetMaxDefinitionLevel(id)+1);
    for (int d = 0; d < depth_list.size(); ++d) {
      while (depth < (p1.size() - 1) && (d >= GetMaxDefinitionLevel(p1[depth+1])))
        depth ++;
      depth_list[d] = depth - 1;
      //printf("%2d %2d => %2d\n", id, d, depth_list[d]);
    }
  }
  all_edges[ROOT_NODE].push_back(fids[0]);
  fsm.init(all_edges, deflvl_to_depth, level_to_close);
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

ParquetFileReader::ParquetFileReader(const string& path)
 : file_path_(path) 
{
  if (!GetFileMetadata(file_path_, &metadata_))
    throw ParquetException("unable to open file");

  size_t size = metadata_.schema.size();
  rep_levels_.resize(size);
  readers_.resize(size);
  chunk_generators_.resize(size);
  values_.resize(size);
  helper_.reset(new SchemaHelper(metadata_.schema));
  for (int i=1; i<size; ++i)
    values_[i].BindDescriptor(helper_->columns[i]);
}

const ColumnDescriptor& 
ParquetFileReader::GetColumnDescriptor(int column_id) {
  return helper_->columns[column_id];
}

ValueBatch& ParquetFileReader::GetColumnValues(int column_id) {
  return values_[column_id];
}

int ParquetFileReader::LoadColumnData(int fid, int num_records) {
  const ColumnDescriptor& desc = helper_->columns[fid];
  if (desc.element.num_children > 0)
    return 0;
  if (rep_levels_[fid] == NULL) {
    rep_levels_[fid] = &(values_[fid].RepetitionLevels()[0]); }
  if (readers_[fid].get() == NULL) {
    chunk_generators_[fid].reset(new ColumnChunkGenerator(file_path_, metadata_, 
      *helper_));
    chunk_generators_[fid]->selectColumn(fid);
    if (!chunk_generators_[fid]->NextReader(readers_[fid])) 
      return 0;
    if (!readers_[fid]->HasNext())
      return 0;
  }
  int records_remains = num_records;
  int values = 0;
  vector<int> offsets;
  ColumnReader& reader = *readers_[fid];
  do {
    values += reader.GetRecordValueBatch(values_[fid], offsets, records_remains);
    if (offsets.size() == num_records) break;
    if (!chunk_generators_[fid]->NextReader(readers_[fid])) break;
    records_remains = num_records - offsets.size();
  } while (records_remains > 0);
  rep_levels_[fid] = &(values_[fid].RepetitionLevels()[0]);
  return values;
}

template<>
int ParquetFileReader::LoadColumnData(int fid, int num_records,
  const vector<bool>& bitmask)
{
  const ColumnDescriptor& desc = helper_->columns[fid];
  if (desc.element.num_children > 0)
    return 0;
  if (rep_levels_[fid] == NULL) {
    rep_levels_[fid] = &(values_[fid].RepetitionLevels()[0]); }
  if (readers_[fid].get() == NULL) {
    chunk_generators_[fid].reset(new ColumnChunkGenerator(file_path_, metadata_,
      *helper_));
    chunk_generators_[fid]->selectColumn(fid);
    if (!chunk_generators_[fid]->NextReader(readers_[fid]))
      return 0;
    if (!readers_[fid]->HasNext())
      return 0;
  }
  int records_remains = num_records;
  int values = 0;
  vector<int> offsets;
  offsets.reserve(num_records);
  ColumnReader& reader = *readers_[fid];
  int idx = 0;
  do {
    int idx2 = idx;
    while (bitmask[idx2] && idx2 < bitmask.size() && idx2 < num_records) ++idx2;
    records_remains -= (idx2 - idx);
    if (idx < idx2) reader.skipRecords(idx2 - idx);
    idx = idx2;
    while (!bitmask[idx2] && idx2 < bitmask.size() && idx2 < num_records) ++idx2;
    int offsize = offsets.size();
    values += reader.GetRecordValueBatch(values_[fid], offsets, idx2 - idx);
    idx += (offsets.size() - offsize);
    if (idx == bitmask.size()) break;
    if (offsets.size() == num_records) break;
    if (!chunk_generators_[fid]->NextReader(readers_[fid])) break;
    records_remains -= (offsets.size() - offsize);
  } while (records_remains > 0);
  rep_levels_[fid] = &(values_[fid].RepetitionLevels()[0]);
  return values;
}

ColumnChunkGenerator::ColumnChunkGenerator(const string& file_path, 
  parquet::FileMetaData& fmd, SchemaHelper& helper):
  file_path_(file_path), metadata_(fmd), helper_(helper), row_group_idx_(0)
{
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

bool ColumnChunkGenerator::NextReader(shared_ptr<ColumnReader>& reader)
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
    col_chunk_ = col;
    reader.reset(new ColumnReader(&(col_chunk_.meta_data), 
      input_.get(), desc));
    ++ row_group_idx_;
    return true;
  }
  ++ row_group_idx_;
  return false;
}

int RecordAssembler::assemble() {
  int entry_state = fsm_.GetEntryState();
  int fid = entry_state;
  convertor_.startRecord();
  int current_level = 0;

  while ( fid != ROOT_NODE ) { 
    int& fid_idx = values_idx_[fid];
    const vector<int>& field_path = helper_.GetElementPathIds(fid);
    //printf("%2d %2d\n", fid, fid_idx);

    bool is_null = values_[fid]->isNull(fid_idx);
    int rep_lvl = values_[fid]->repetitionLevel(fid_idx+1);
    int def_lvl = values_[fid]->definitionLevel(fid_idx);

    int depth = fsm_.depthOfDefLevel(fid, def_lvl);
    for (; current_level <= depth; ++current_level) {
      int g = field_path[current_level+1];
      convertor_.startGroup(g);
    }
    if (!is_null)
      convertor_.convertField(fid, fid_idx);
    int next_level = fsm_.nextLevel(fid, rep_lvl);
    for (; current_level > next_level; current_level --)
      convertor_.endGroup();

    fid_idx ++;
    int nfid = fsm_.GetNextState(fid, rep_lvl);
    fid = nfid;
  }
  convertor_.endRecord();
  return 1;
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

string dump_value(const ValueBatch& batch, int idx) {
  stringstream oss;
  oss << batch.repetitionLevel(idx) << ':'
      << batch.repetitionLevel(idx) << ':';

  if (batch.isNull(idx)) {
    switch (batch.type()) {
    case Type::INT32: oss << batch.Get<int32_t>(idx); break;
    case Type::INT64: oss << batch.Get<int64_t>(idx); break;
    case Type::FLOAT: oss << batch.Get<float>(idx); break;
    case Type::DOUBLE: oss << batch.Get<double>(idx); break;
    case Type::BYTE_ARRAY: oss << batch.Get<ByteArray>(idx); break;
    default:
      cerr << __FILE__ << ':' << __LINE__ << " doesn't support type "
           << batch.type() << " yet\n";
    }
  } else {
    oss << "<NULL>";
  }
  return oss.str();
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

ostream& operator<<(ostream& oss, Type::type t) {
  switch(t){
  case Type::BOOLEAN: oss << "boolean"; break;
  case Type::INT32: oss << "int32"; break;
  case Type::INT64: oss << "int64"; break;
  case Type::INT96: oss << "int96"; break;
  case Type::FLOAT: oss << "float"; break;
  case Type::DOUBLE: oss << "double"; break;
  case Type::BYTE_ARRAY: oss << "byte_array"; break;
  case Type::FIXED_LEN_BYTE_ARRAY: oss << "fixed_len_byte_array"; break;
  default: oss << "unknown-type(" << (int) t << ")";
  }
  return oss;
}

ostream& operator<<(ostream& oss, Encoding::type t) {
  switch(t) {
  case Encoding::PLAIN: oss << "plain"; break;
  case Encoding::PLAIN_DICTIONARY: oss << "plain-dictionary"; break;
  case Encoding::RLE: oss << "rle"; break;
  case Encoding::BIT_PACKED: oss << "bit-packed"; break;
  case Encoding::DELTA_BINARY_PACKED: oss << "delta-bin-packed"; break;
  case Encoding::DELTA_LENGTH_BYTE_ARRAY: oss << "delta-len-byte-array"; break;
  case Encoding::DELTA_BYTE_ARRAY: oss << "delta-byte-array"; break;
  case Encoding::RLE_DICTIONARY: oss << "rle-dictionary"; break;
  default: oss << "unknown-encoding(" << (int) t << ")";
  }
  return oss;
}

