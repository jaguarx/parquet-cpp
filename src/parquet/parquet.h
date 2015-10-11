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

#ifndef PARQUET_PARQUET_H
#define PARQUET_PARQUET_H

#include <exception>
#include <sstream>
#include <map>

//#include <boost/cstdint.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include "gen-cpp/parquet_constants.h"
#include "gen-cpp/parquet_types.h"

// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/TApplicationException.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include "impala/rle-encoding.h"

namespace parquet_cpp {

using std::map;
using std::vector;
using std::string;

bool GetFileMetadata(const string& path, parquet::FileMetaData* metadata);
class Codec;
class Decoder;

struct ByteArray {
  uint32_t len;
  const uint8_t* ptr;
};

struct Int96 {
  uint32_t i[3];
};

class ParquetException : public std::exception {
 public:
  static void EofException() { throw ParquetException("Unexpected end of stream."); }
  static void NYI(const std::string& msg) {
    std::stringstream ss;
    ss << "Not yet implemented: " << msg << ".";
    throw ParquetException(ss.str());
  }

  explicit ParquetException(const char* msg) : msg_(msg) {}
  explicit ParquetException(const std::string& msg) : msg_(msg) {}
  explicit ParquetException(const char* msg, exception& e) : msg_(msg) {}

  virtual ~ParquetException() throw() {}
  virtual const char* what() const throw() { return msg_.c_str(); }

 private:
  std::string msg_;
};

// Interface for the column reader to get the bytes. The interface is a stream
// interface, meaning the bytes in order and once a byte is read, it does not
// need to be read again.
class InputStream {
 public:
  // Returns the next 'num_to_peek' without advancing the current position.
  // *num_bytes will contain the number of bytes returned which can only be
  // less than num_to_peek at end of stream cases.
  // Since the position is not advanced, calls to this function are idempotent.
  // The buffer returned to the caller is still owned by the input stream and must
  // stay valid until the next call to Peek() or Read().
  virtual const uint8_t* Peek(int num_to_peek, int* num_bytes) = 0;

  // Identical to Peek(), except the current position in the stream is advanced by
  // *num_bytes.
  virtual const uint8_t* Read(int num_to_read, int* num_bytes) = 0;

  virtual ~InputStream() {}

 protected:
  InputStream() {}
};

// Implementation of an InputStream when all the bytes are in memory.
class InMemoryInputStream : public InputStream {
 public:
  InMemoryInputStream(const uint8_t* buffer, int64_t len);
  virtual const uint8_t* Peek(int num_to_peek, int* num_bytes);
  virtual const uint8_t* Read(int num_to_read, int* num_bytes);

 private:
  const uint8_t* buffer_;
  int64_t len_;
  int64_t offset_;
};

enum SchemaConstants {
  ROOT_NODE = 0,
  STAY = 1,
  FOLLOW = 2,
};

struct edge_t{
  edge_t(int n=ROOT_NODE, int t=STAY):next(n), type(t){}
  int next;
  int type;
};

struct ColumnDescriptor {
  parquet::SchemaElement element;
  int max_def_level;
  int max_rep_level;
  ColumnDescriptor() : max_def_level(0), max_rep_level(0) {}
};

void DumpSchema(std::ostream&, const vector<ColumnDescriptor>&);

class SchemaFSM {
public:
  typedef int state_t;
  SchemaFSM() {}

  SchemaFSM& init(vector<vector<int> >& states,
    vector<vector<int> >& deflvl_to_depth,
    vector<vector<int> >& next_levels) {
    states_.swap(states);
    deflvl_to_depth_.swap(deflvl_to_depth);
    next_levels_.swap(next_levels);
    return *this;
  }

  int GetEntryState() const {
    return states_[ROOT_NODE][0];
  }

  int GetNextState(int state, int rep_level) const {
    return states_[state][rep_level];
  }

  int depthOfDefLevel(int fid, int def_lvl) const {
    return deflvl_to_depth_[fid][def_lvl]; }

  int nextLevel(int fid, int rep_lvl) const {
    return next_levels_[fid][rep_lvl]; }

  void dump(std::ostream& oss) const;

private:
  vector<vector<int> > states_;
  vector<vector<int> > next_levels_;
  vector<vector<int> > deflvl_to_depth_;

};

class SchemaHelper {
public:
  SchemaHelper (vector<parquet::SchemaElement>& _schema);
  SchemaHelper(const std::string& file_path);

  int GetMaxDefinitionLevel(int col_idx) const {
    return columns[col_idx].max_def_level; }

  int GetMaxRepetitionLevel(int col_idx) const{
    return columns[col_idx].max_rep_level; }

  int GetMaxDepth() const;

  const string& GetElementPath(int col_idx) const {
    return _element_paths[col_idx];
  }
  const vector<int>& GetElementPathIds(int col_idx) const {
    return _id_paths[col_idx]; }

  int GetElementId(const std::string& path) const {
    std::map<string, int>::const_iterator i = _path_to_id.find(path);
    if (i == _path_to_id.end())
      return -1;
    return i->second;
  }

  void BuildFullFSM();
  void BuildFSM(const vector<string>& fields, SchemaFSM& fsm);

  vector<ColumnDescriptor> columns;

private:
  void init_();
  int _build_child_fsm(int fid);
  int _rebuild_tree(int fid, int rep_level, int def_level, const string& path);
  int _follow_fsm(int fid, int rep_lvl);
  int _compress_state(int fid, int rep_lvl, const vector<int>& fields);

  int _parent_of_level(int fid, int lvl);
  int _last_child_of_parent(int pid, const vector<int>& fids);

  vector<int> _child_to_parent;
  vector<vector<int> > _parent_to_child;
  vector<string> _element_paths;
  std::map<string, int> _path_to_id;
  vector<vector<edge_t> > _edges;
  vector<vector<int> > _id_paths; // path to field, starting from root node
};

class ColumnReader;

class ValueBatch {
 public:
  ValueBatch() {}
  void BindDescriptor(const ColumnDescriptor& desc);
  bool isNull(int index) const {
    if (index >= def_levels_.size()) return true;
    int def_level = def_levels_[index];
    return def_level < max_def_level_;
  }
  template<typename T>
  const T& Get(int index) const {
    const T* base = reinterpret_cast<const T*>(&values_[0]);
    return base[index]; }

  uint8_t* valueAddress(int offset) {
    uint8_t* base = (uint8_t*)&values_[0];
    return base + offset * value_byte_size_; }

  void reset(int size_hint) {
    record_offsets_.clear(); record_offsets_.reserve(size_hint); 
    def_levels_.clear(); def_levels_.reserve(size_hint);
    rep_levels_.clear(); rep_levels_.reserve(size_hint);
    values_.clear(); values_.reserve(size_hint);
    buffer_.clear(); buffer_.reserve(8*size_hint);
  }

  void resize(int values) {
    def_levels_.resize(values);
    values_.resize(1+(values*value_byte_size_)/sizeof(uint32_t)); }

  vector<int>& recordOffsets() {
    return record_offsets_;
  }

  int recordOffset(int idx) {
    if (idx<record_offsets_.size()) return record_offsets_[idx];
    return 0;
  }

  vector<int>& DefinitionLevels() {
    return def_levels_; }

  vector<int>& RepetitionLevels() {
    return rep_levels_; }

  int repetitionLevel(int idx) const {
    if (idx < rep_levels_.size()) return rep_levels_[idx];
    return 0;
  }

  int definitionLevel(int idx) const {
    if (idx < def_levels_.size()) return def_levels_[idx];
    return 0;
  }

  int recordOffset(int idx) const {
    if (idx < record_offsets_.size()) return record_offsets_[idx];
    return 0; }

  parquet::Type::type type() const {
    return type_;
  }

  ValueBatch& appendNilRecords(int cnt);
  ValueBatch& append(ValueBatch&);
 
 private:
  friend class ColumnReader;
  int max_def_level_;
  int value_byte_size_;
  parquet::Type::type type_;
  vector<uint32_t> values_;
  vector<int> rep_levels_;
  vector<int> def_levels_;
  vector<int> record_offsets_;
  vector<uint8_t> buffer_;
};

// API to read values from a single column. This is the main client facing API.
class ColumnReader {
 public:
  struct Config {
    int batch_size;

    static Config DefaultConfig() {
      Config config;
      config.batch_size = 128;
      return config;
    }
  };

  ColumnReader(const parquet::ColumnMetaData*,
      InputStream* stream, 
      const ColumnDescriptor& desc);

  ~ColumnReader();

  // Returns true if there are still values in this column.
  bool HasNext();

  // Batch interface
  int GetValueBatch(ValueBatch& batch, int max_values);
  int GetRecordValueBatch(ValueBatch& batch, int num_records);
    //vector<int>& record_offsets, int num_records);
  template<typename T>
  int GetRecordValueBatch(ValueBatch& batch,
    vector<int>& record_offsets, int num_records, const T& bitmask);

  // skip values
  int skipValue(int count);
  int skipRecords(int count);

  int MaxDefinitionLevel() const {
    return this->max_definition_level_; }

 private:
  bool ReadNewPage();
  // Reads the next definition and repetition level. Returns true if the value is NULL.
  bool ReadDefinitionRepetitionLevels(int* def_level, int* rep_level);
  int decodeRepetitionLevels(vector<int32_t>& repetition_levels,
                             int value_count);

  int DecodeValues(uint8_t* values, vector<uint8_t>& buf, int count);

  Config config_;
public:
  const parquet::ColumnMetaData* metadata_;
private:
  ColumnDescriptor column_;
  InputStream* stream_;

  // Compression codec to use.
  boost::scoped_ptr<Codec> decompressor_;
  vector<uint8_t> decompression_buffer_;

  // Map of compression type to decompressor object.
  boost::unordered_map<parquet::Encoding::type, boost::shared_ptr<Decoder> > decoders_;

  parquet::PageHeader current_page_header_;

  // Not set if field is required.
  boost::scoped_ptr<impala::RleDecoder> definition_level_decoder_;
  // Not set for flat schemas.
  boost::scoped_ptr<impala::RleDecoder> repetition_level_decoder_;

  int max_repetition_level_;
  int max_definition_level_;

  Decoder* current_decoder_;
  int num_buffered_values_;

  vector<uint8_t> values_buffer_;
  int num_decoded_values_;
  int buffered_values_offset_;

  int saved_rep_level_;
};

class ColumnFilter {
};

class RecordFilter {
};

class RecordConvertor;

//utitlity class to go through parquet file scan specific column
class ColumnChunkGenerator {
public:
  ColumnChunkGenerator(const string& path, parquet::FileMetaData& fmd, 
    SchemaHelper& helper);

  void selectColumn(int col_id);
  const string& GetColumnPath() const {
    return col_path_;
  }

  const parquet::SchemaElement& schemaElement() const {
    return metadata_.schema[col_idx_]; }

  const parquet::ColumnMetaData& columnMetaData() const {
    return column_metadata_; }

  bool NextReader(boost::shared_ptr<ColumnReader>&);

private:
  const string& file_path_;
  parquet::FileMetaData metadata_;
  parquet::ColumnMetaData column_metadata_;
  SchemaHelper& helper_;

  int row_group_idx_;
  string col_path_;
  int col_idx_;
  parquet::ColumnChunk col_chunk_;
  boost::scoped_ptr<InputStream> input_;
};

class ParquetFileReader {
public:
  ParquetFileReader(const string& path);

  const ColumnDescriptor& GetColumnDescriptor(int column_id);
  ValueBatch& GetColumnValues(int column_id);
  ValueBatch& GetColumnValues(const string& column_path);

  void syncColumnsBoundray();
  int  LoadColumnData(int fid, int num_records);
  template<typename T>
  int  LoadColumnData(int fid, int num_records, const T& bitmap);

  SchemaHelper& GetHelper() { return *helper_; }
  vector<int*>& RepetitionLevels() {
    return rep_levels_; }
private:
  string file_path_;
  parquet::FileMetaData metadata_;

  vector<int*> rep_levels_;
  vector<int> offsets_;
  vector<ValueBatch> values_;
  vector<boost::shared_ptr<ColumnReader> > readers_;
  vector<boost::shared_ptr<ColumnChunkGenerator> > chunk_generators_;
  boost::scoped_ptr<SchemaHelper> helper_;
};

class RecordConvertor {
public:
  virtual void startRecord() = 0;
  virtual void startGroup(int fid) = 0;
  virtual void convertField(int fid, int fid_idx) = 0;
  virtual void endGroup() = 0;
  virtual void endRecord() = 0;
  virtual ~RecordConvertor(){};
};

class RecordAssembler {
public:
  RecordAssembler(SchemaHelper& helper, vector<ValueBatch*>& values,
    vector<int>& values_idx, RecordConvertor& convertor)
  : helper_(helper), values_(values), values_idx_(values_idx), convertor_(convertor) {
  }

  void selectOutputColumns(const vector<string>& columns) {
    helper_.BuildFSM(columns, fsm_);
  }

  int assemble();
  
private:
  vector<int>& values_idx_;
  vector<ValueBatch*> values_;
  SchemaHelper& helper_;
  RecordConvertor& convertor_;
  SchemaFSM fsm_;
  std::stack<int> field_stack_;

};

inline bool ColumnReader::HasNext() {
  if (num_buffered_values_ == 0) {
    ReadNewPage();
    if (num_buffered_values_ == 0) return false;
  }
  return true;
}

//inline bool ColumnReader::GetBool(int* def_level, int* rep_level) {
//  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return bool();
//  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
//  return reinterpret_cast<bool*>(&values_buffer_[0])[buffered_values_offset_++];
//}
//
//inline int32_t ColumnReader::GetInt32(int* def_level, int* rep_level) {
//  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return int32_t();
//  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
//  return reinterpret_cast<int32_t*>(&values_buffer_[0])[buffered_values_offset_++];
//}
//
//inline int64_t ColumnReader::GetInt64(int* def_level, int* rep_level) {
//  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return int64_t();
//  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
//  return reinterpret_cast<int64_t*>(&values_buffer_[0])[buffered_values_offset_++];
//}
//
//inline float ColumnReader::GetFloat(int* def_level, int* rep_level) {
//  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return float();
//  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
//  return reinterpret_cast<float*>(&values_buffer_[0])[buffered_values_offset_++];
//}
//
//inline double ColumnReader::GetDouble(int* def_level, int* rep_level) {
//  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return double();
//  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
//  return reinterpret_cast<double*>(&values_buffer_[0])[buffered_values_offset_++];
//}
//
//inline ByteArray ColumnReader::GetByteArray(int* def_level, int* rep_level) {
//  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return ByteArray();
//  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
//  return reinterpret_cast<ByteArray*>(&values_buffer_[0])[buffered_values_offset_++];
//}

// Deserialize a thrift message from buf/len.  buf/len must at least contain
// all the bytes needed to store the thrift message.  On return, len will be
// set to the actual length of the header.
template <class T>
inline void DeserializeThriftMsg(const uint8_t* buf, uint32_t* len, T* deserialized_msg) {
  // Deserialize msg bytes into c++ thrift msg using memory transport.
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(const_cast<uint8_t*>(buf), *len));
  apache::thrift::protocol::TCompactProtocolFactoryT<
      apache::thrift::transport::TMemoryBuffer> tproto_factory;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      tproto_factory.getProtocol(tmem_transport);
  try {
    deserialized_msg->read(tproto.get());
  } catch (apache::thrift::protocol::TProtocolException& e) {
    throw ParquetException("Couldn't deserialize thrift.", e);
  }
  uint32_t bytes_left = tmem_transport->available_read();
  *len = *len - bytes_left;
}

string dump_value(const ValueBatch& batch, int idx);
}

std::ostream& operator<<(std::ostream& oss, parquet::FieldRepetitionType::type t);
std::ostream& operator<<(std::ostream& oss, parquet::Type::type t);
std::ostream& operator<<(std::ostream& oss, parquet::Encoding::type t);
std::ostream& operator<<(std::ostream& oss, const parquet_cpp::Int96& v);
std::ostream& operator<<(std::ostream& oss, const parquet_cpp::ByteArray& a);

#endif

