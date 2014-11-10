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
    buffered_values_offset_(0),
    saved_rep_level_(-1) {
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
      const DataPageHeader& data_page_header = current_page_header_.data_page_header;
      num_buffered_values_ = data_page_header.num_values;

      // repetition levels
      if (max_repetition_level_ > 0) {
        int bitwidth = BitUtil::NumRequiredBits(max_repetition_level_);

        if (data_page_header.repetition_level_encoding == Encoding::RLE)
          repetition_level_decoder_.reset(new parquet_cpp::RleDecoder(parquet::Type::INT32, bitwidth));
        else
          throw ParquetException("unsupported repetition encoding.");

        repetition_level_decoder_->SetData(data_page_header.num_values,
          buffer, uncompressed_len);
        buffer += repetition_level_decoder_->GetSize();
        uncompressed_len -= repetition_level_decoder_->GetSize();
      }

      // definition levels.
      if (max_definition_level_ > 0) {
        int bitwidth = BitUtil::NumRequiredBits(max_definition_level_);

        if (data_page_header.definition_level_encoding == Encoding::RLE)
          definition_level_decoder_.reset(new parquet_cpp::RleDecoder(parquet::Type::INT32, bitwidth));
        else
          throw ParquetException("unsupported definition encoding.");

        definition_level_decoder_->SetData(data_page_header.num_values,
          buffer, uncompressed_len);
        buffer += definition_level_decoder_->GetSize();
        uncompressed_len -= definition_level_decoder_->GetSize();
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
    if (1 != definition_level_decoder_->GetInt32(&val, 1)) {
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
    if (1 != repetition_level_decoder_->GetInt32(&rep_lvl, 1)) {
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

SchemaHelper::SchemaHelper(const string& file_path) {
  parquet::FileMetaData metadata;
  if (!GetFileMetadata(file_path, &metadata))
    throw ParquetException("unable to open file");
  schema = metadata.schema;
  init_();
}

int SchemaHelper::_rebuild_tree(int fid, int rep_level, int def_level, 
  const string& path)
{
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
    _path_to_id[fullpath] = fid;
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
    chd += _rebuild_tree(chd, rep_level, def_level, fullpath);
  }
  return chd - fid;
}

int SchemaHelper::_build_child_fsm(int fid){
  const vector<int>& child_ids = _parent_to_child[fid];
  for (int i=0; i<child_ids.size(); ++i) {
    int cid = child_ids[i];
    SchemaElement& element = schema[cid];
    int rep_level = _max_repetition_levels[cid];

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
  if (_edges.size() != schema.size()) {
    _edges.resize(schema.size());
    _build_child_fsm(ROOT_NODE);
  }
}

void SchemaHelper::BuildFSM(const vector<string>& fields, SchemaFSM& fsm) {
  BuildFullFSM();
  vector<int> fids;
  if (fields.size() == 0) {
    for (int i=0; i < schema.size(); ++i) {
      SchemaElement& element = schema[i];
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
  vector<vector<int> > all_edges(schema.size());
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
    SchemaElement& element = schema[ts_id];
    while (schema[ts_id].__isset.num_children) {
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

void ColumnValueChunk::scanRecordBoundary(){
  rep_lvl_pos_ = 0;
  def_lvl_pos_ = 0;
  val_buf_pos_ = 0;
  num_values_ = 0;

  int rep_lvl = reader_.nextRepetitionLevel();
  rep_lvls_.push_back(rep_lvl);
  int def_lvl = reader_.nextDefinitionLevel();
  def_lvls_.push_back(def_lvl);
  if (def_lvl == reader_.MaxDefinitionLevel())
    num_values_ ++;
  do {
    rep_lvl = reader_.peekRepetitionLevel();
    if (rep_lvl > 0) {
      rep_lvl = reader_.nextRepetitionLevel();
  	  rep_lvls_.push_back(rep_lvl);
      def_lvl = reader_.nextDefinitionLevel();
      def_lvls_.push_back(def_lvl);
      if (def_lvl == reader_.MaxDefinitionLevel()) {
        num_values_ ++;
      }
    }
  } while (rep_lvl > 0);
  rep_lvls_.push_back(0); //add a terminating 0
  reader_.copyValues(val_buff_, num_values_);
  value_loaded_ = true;
}

int RecordAssembler::assemble() {
  int entry_state = fsm_.GetEntryState();
  ColumnValueChunk& ch = fac_.GetChunk(entry_state);
  if (!ch.HasNext())
    return 0;

  // apply filers
  bool skip_record = fac_.applyFilter();

  int fid = entry_state;
  while (fid != ROOT_NODE) {
    ColumnValueChunk& ch = fac_.GetChunk(fid);
    if (!ch.valueLoaded())
      ch.scanRecordBoundary();
    if (skip_record) {
      ch.clearBuffer();
    } else {
   	  ch.resetBufferPos();
      ch.nextRepetitionLevel();
    }
    fid = fsm_.GetNextState(fid, 0);
  }
  if (skip_record)
    return 1;

  fid = entry_state;
  while ( fid != ROOT_NODE ) {
    ColumnValueChunk& ch = fac_.GetChunk(fid);
    fac_.consumeValueChunk(fid, ch);
    int rep_lvl = ch.nextRepetitionLevel();
    int nfid = fsm_.GetNextState(fid, rep_lvl);
    fid = nfid;
  }
  fid = entry_state;
  while (fid != ROOT_NODE) {
    ColumnValueChunk& ch = fac_.GetChunk(fid);
    ch.clearBuffer();
    fid = fsm_.GetNextState(fid, 0);
  }
  return 1;
}

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

ColumnChunkGenerator::ColumnChunkGenerator(const string& file_path, int col_idx):
  file_path_(file_path), row_group_idx_(0)
{
  if (!GetFileMetadata(file_path_, &metadata_))
    throw ParquetException("unable to open file");
  if (col_idx >= metadata_.schema.size())
    throw ParquetException("invalid column");

  helper_.reset(new SchemaHelper(metadata_.schema));
  col_idx_ = col_idx;
  max_definition_level_ = helper_->GetMaxDefinitionLevel(col_idx_);
  max_repetition_level_ = helper_->GetMaxRepetitionLevel(col_idx_);
  col_path_ = helper_->GetElementPath(col_idx);
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
   
    col_chunk_ = col; 
    reader.reset(new ColumnReader(&(col_chunk_.meta_data), &metadata_.schema[col_idx_],
      input_.get(), max_repetition_level_, max_definition_level_));
    ++ row_group_idx_;
    return true;
  }
  ++ row_group_idx_;
  return false;
}

}

