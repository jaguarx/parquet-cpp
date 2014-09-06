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

#include <string>
#include <string.h>

#include <thrift/protocol/TDebugProtocol.h>

const int DATA_PAGE_SIZE = 64 * 1024;

using namespace boost;
using namespace parquet;
using namespace std;
using impala::BitUtil;

namespace parquet_cpp {

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
    const SchemaElement* schema, InputStream* stream,
    int max_repetition_level,
    int max_definition_level)
  : metadata_(metadata),
    schema_(schema),
    stream_(stream),
    max_repetition_level_(max_repetition_level),
    max_definition_level_(max_definition_level),
    current_decoder_(NULL),
    num_buffered_values_(0),
    num_decoded_values_(0),
    buffered_values_offset_(0) {
  int value_byte_size;
  switch (metadata->type) {
    case parquet::Type::BOOLEAN:
      value_byte_size = 1;
      break;
    case parquet::Type::INT32:
      value_byte_size = sizeof(int32_t);
      break;
    case parquet::Type::INT64:
      value_byte_size = sizeof(int64_t);
      break;
    case parquet::Type::FLOAT:
      value_byte_size = sizeof(float);
      break;
    case parquet::Type::DOUBLE:
      value_byte_size = sizeof(double);
      break;
    case parquet::Type::BYTE_ARRAY:
      value_byte_size = sizeof(ByteArray);
      break;
    default:
      ParquetException::NYI("Unsupported type");
  }

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
  values_buffer_.resize(config_.batch_size * value_byte_size);
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

    if (current_page_header_.type == PageType::DICTIONARY_PAGE) {
      unordered_map<Encoding::type, shared_ptr<Decoder> >::iterator it =
          decoders_.find(Encoding::RLE_DICTIONARY);
      if (it != decoders_.end()) {
        throw ParquetException("Column cannot have more than one dictionary.");
      }

      PlainDecoder dictionary(schema_->type);
      dictionary.SetData(current_page_header_.dictionary_page_header.num_values,
          buffer, uncompressed_len);
      shared_ptr<Decoder> decoder(new DictionaryDecoder(schema_->type, &dictionary));
      decoders_[Encoding::RLE_DICTIONARY] = decoder;
      current_decoder_ = decoders_[Encoding::RLE_DICTIONARY].get();
      continue;
    } else if (current_page_header_.type == PageType::DATA_PAGE) {
      // Read a data page.
      num_buffered_values_ = current_page_header_.data_page_header.num_values;

      // repetition levels
      if (max_repetition_level_ > 0) {
        int bitwidth = BitUtil::NumRequiredBits(max_repetition_level_);
        int num_repetition_bytes = *reinterpret_cast<const uint32_t*>(buffer);
        buffer += sizeof(uint32_t);
        repetition_level_decoder_.reset(
            new impala::RleDecoder(buffer, num_repetition_bytes, bitwidth));
        buffer += num_repetition_bytes;
        uncompressed_len -= sizeof(uint32_t) + num_repetition_bytes;
      }

      // definition levels.
      if (max_definition_level_ > 0) {
        int bitwidth = BitUtil::NumRequiredBits(max_definition_level_);
        int num_definition_bytes = *reinterpret_cast<const uint32_t*>(buffer);
        buffer += sizeof(uint32_t);
        definition_level_decoder_.reset(
            new impala::RleDecoder(buffer, num_definition_bytes, bitwidth));
        buffer += num_definition_bytes;
        uncompressed_len -= sizeof(uint32_t) + num_definition_bytes;
      }

      // Get a decoder object for this page or create a new decoder if this is the
      // first page with this encoding.
      Encoding::type encoding = current_page_header_.data_page_header.encoding;
      if (IsDictionaryIndexEncoding(encoding)) encoding = Encoding::RLE_DICTIONARY;

      unordered_map<Encoding::type, shared_ptr<Decoder> >::iterator it =
          decoders_.find(encoding);
      if (it != decoders_.end()) {
        current_decoder_ = it->second.get();
      } else {
        shared_ptr<Decoder> decoder;
        switch (encoding) {
          case Encoding::PLAIN: {
            if (schema_->type == Type::BOOLEAN) {
              decoder.reset(new BoolDecoder());
            } else {
              decoder.reset(new PlainDecoder(schema_->type));
            }
            decoders_[encoding] = decoder;
            current_decoder_ = decoder.get();
            break;
          }
          case Encoding::RLE_DICTIONARY:
            throw ParquetException("Dictionary page must be before data page.");

          case Encoding::DELTA_BINARY_PACKED: {
            decoder.reset(new DeltaBitPackDecoder(schema_->type));
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

int SchemaHelper::_rebuild_tree(int fid, int rep_level, int def_level, const string& path) {
  const SchemaElement&  parent = schema[fid];
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
    //ppath = path[:]
    //ppath.append(parent.name)
    //self._element_path[fid] = ppath
  }
  _max_repetition_levels[fid] = rep_level;
  _max_definition_levels[fid] = def_level;
  if (!parent.__isset.num_children)
    return 1;

  int num_children = parent.num_children;
  int chd = fid + 1;
  while (num_children > 0) {
    num_children -= 1;
    _child_to_parent[chd] = fid;
    _parent_to_child[fid].push_back(chd);
    chd += _rebuild_tree(chd, rep_level, def_level, fullpath
             //   self._element_path[fid]
           );
  }
  return chd - fid;
}


void SchemaHelper::BuildFSM(std::vector<std::string>& fields, SchemaFSM& fsm) {

}

int SchemaHelper::_build_child_fsm(int fid, vector<vector<SchemaFSM::edge_t> >& edges){
  const vector<int>& child_ids = _parent_to_child[fid];
  for (int i=0; i<child_ids.size(); ++i) {
    int cid = child_ids[i];
    SchemaElement& element = schema[cid];
    int rep_level = _max_repetition_levels[cid];
    vector<SchemaFSM::edge_t>& subedges = edges[cid];
    subedges.resize(rep_level+1);
    for (int r = 0; r < rep_level; ++r) {
      if ( i + 1 == child_ids.size() ) {
        subedges[r].next = fid;
        subedges[r].type = ( fid == ROOT_NODE)? STAY:FOLLOW;
      } else {
        subedges[r].next = child_ids[i+1];
        subedges[r].type = STAY;
      }
    }
    if (element.repetition_type == FieldRepetitionType::REPEATED)
      subedges[rep_level] = SchemaFSM::edge_t(cid, STAY);
    if (element.__isset.num_children && element.num_children > 0) {
      _build_child_fsm(cid, edges);
    }
  }
  return 0;
}

void SchemaHelper::BuildFullFSM(SchemaFSM& fsm) {
  vector<vector<SchemaFSM::edge_t> > edges;
  edges.resize(schema.size());
  _build_child_fsm(ROOT_NODE, edges);
  fsm.init(edges);
}


}

