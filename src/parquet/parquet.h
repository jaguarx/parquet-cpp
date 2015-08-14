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

bool GetFileMetadata(const std::string& path, parquet::FileMetaData* metadata);

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

class SchemaFSM {
public:
  SchemaFSM() {}

  void init(std::vector<std::vector<int> >& states) {
    states_.swap(states);
  }

  int GetEntryState() {
    return states_[ROOT_NODE][0];
  }
  int GetNextState(int state, int rep_level) {
    return states_[state][rep_level];
  }

  void dump(std::ostream& oss) const;
private:
  std::vector<std::vector<int> > states_;
};

class SchemaHelper {
public:
  SchemaHelper (std::vector<parquet::SchemaElement>& _schema):schema(_schema){
    init_();
  }
  SchemaHelper(const std::string& file_path);

  int GetMaxDefinitionLevel(int col_idx) const {
    return _max_definition_levels[col_idx];
  }

  int GetMaxRepetitionLevel(int col_idx) const{
    return _max_repetition_levels[col_idx];
  }

  const std::string& GetElementPath(int col_idx) const {
    return _element_paths[col_idx];
  }

  int GetElementId(const std::string& path) const {
    std::map<std::string, int>::const_iterator i = _path_to_id.find(path);
    if (i == _path_to_id.end())
      return -1;
    return i->second;
  }

  void BuildFullFSM();
  void BuildFSM(const std::vector<std::string>& fields, SchemaFSM& fsm);

  std::vector<parquet::SchemaElement> schema;

private:
  void init_() {
    _max_definition_levels.resize(schema.size());
    _max_repetition_levels.resize(schema.size());
    _child_to_parent.resize(schema.size());
    _parent_to_child.resize(schema.size());
    _element_paths.resize(schema.size());
    _rebuild_tree(ROOT_NODE, 0, 0, "");
  }
  int _build_child_fsm(int fid);
  int _rebuild_tree(int fid, int rep_level, int def_level, const std::string& path);
  int _follow_fsm(int fid, int rep_lvl);
  int _compress_state(int fid, int rep_lvl, const std::vector<int>& fields);

  std::vector<int> _max_definition_levels;
  std::vector<int> _max_repetition_levels;
  std::vector<int> _child_to_parent;
  std::vector<std::vector<int> > _parent_to_child;
  std::vector<std::string> _element_paths;
  std::map<std::string, int> _path_to_id;
  std::vector<std::vector<edge_t> > _edges;
};

class ColumnReader;

template<typename T>
class ValueBatch {
 public:
  ValueBatch() : max_def_level_(0) {}
  bool isNull(int index) {
    int def_level = def_levels_[index];
    return def_level < max_def_level_;
  }
  T& operator[](int idx) {
    T* base = reinterpret_cast<T*>(&values_[0]);
    return base[idx];
  }
  T get(int index) {
    T* base = reinterpret_cast<T*>(&values_[0]);
    return base[index];
  }
  void resize(int count) {
    values_.resize( (count+1) * sizeof(T)/sizeof(uint32_t) );
  }
 private:
  friend class ColumnReader;
  int max_def_level_;
  std::vector<uint32_t> values_;
  std::vector<int32_t> rep_levels_;
  std::vector<int32_t> def_levels_;
  std::vector<uint8_t> buffer_;
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
      const parquet::SchemaElement*, InputStream* stream, 
      int max_repetition_level,
      int max_definition_level);

  ~ColumnReader();

  // Returns true if there are still values in this column.
  bool HasNext();

  // Returns the next value of this type.
  // TODO: batchify this interface.
  bool GetBool(int* definition_level, int* repetition_level);
  int32_t GetInt32(int* definition_level, int* repetition_level);
  int64_t GetInt64(int* definition_level, int* repetition_level);
  float GetFloat(int* definition_level, int* repetition_level);
  double GetDouble(int* definition_level, int* repetition_level);
  ByteArray GetByteArray(int* definition_level, int* repetition_level);

  // Batch interface
  template<typename T> int GetValueBatch(ValueBatch<T>& batch, int max_values);
  template<typename T> int DecodeValues(T* values, std::vector<uint8_t>& buf, int max_values);

  // skip values
  int skipValue(int count);
  int skipCurrentRecord();

  int nextDefinitionLevel();
  int peekRepetitionLevel();
  int nextRepetitionLevel();
  int nextValue() {
    if (buffered_values_offset_ == num_decoded_values_)
      BatchDecode();
    else
      buffered_values_offset_++;
    num_buffered_values_ --;
    return buffered_values_offset_;
  }
  int copyValues(std::vector<uint8_t>& buf, int value_count);

  int decodeRepetitionLevels(std::vector<int32_t>& repetition_levels,
                             int value_count);
  // definition_levels
  int decodeValues(std::vector<uint8_t>& buf,
                   std::vector<int32_t>& definition_levels,
                   int value_count);
  int MaxDefinitionLevel() const {
    return this->max_definition_level_; }

 private:
  bool ReadNewPage();
  // Reads the next definition and repetition level. Returns true if the value is NULL.
  bool ReadDefinitionRepetitionLevels(int* def_level, int* rep_level);

  // retur true if value exists
  bool peekDefinitionRepetitionLevels(int* def_level, int* rep_level);

  void BatchDecode();

  Config config_;
public:
  const parquet::ColumnMetaData* metadata_;
private:
  const parquet::SchemaElement* schema_;
  InputStream* stream_;

  // Compression codec to use.
  boost::scoped_ptr<Codec> decompressor_;
  std::vector<uint8_t> decompression_buffer_;

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

  std::vector<uint8_t> values_buffer_;
  int num_decoded_values_;
  int buffered_values_offset_;

  int saved_rep_level_;
};

//utitlity class to go through parquet file scan specific column
class ColumnChunkGenerator {
public:
  ColumnChunkGenerator(const std::string& file_path, const std::string& col_path);
  ColumnChunkGenerator(const std::string& file_path, int col_idx);
  int GetMaxDefinitionLevel() const {
    return max_definition_level_;
  }

  int GetMaxRepetitionLevel() const {
    return max_repetition_level_;
  }

  const std::string& GetColumnPath() const {
    return col_path_;
  }

  const parquet::SchemaElement& schemaElement() const {
    return metadata_.schema[col_idx_]; }

  const parquet::ColumnMetaData& columnMetaData() const {
    return column_metadata_; }

  bool next(boost::shared_ptr<ColumnReader>&);

private:
  std::string file_path_;
  parquet::FileMetaData metadata_;
  parquet::ColumnMetaData column_metadata_;
  int row_group_idx_;

  std::string col_path_;
  int col_idx_;
  parquet::ColumnChunk col_chunk_;
  int max_definition_level_;
  int max_repetition_level_;
  boost::scoped_ptr<SchemaHelper> helper_;
  boost::scoped_ptr<InputStream> input_;
};

// vector of column rep/def levels, and values
// for a same record
class ColumnValueChunk {
public:
  ColumnValueChunk(ColumnChunkGenerator& generator)
  : generator_(generator)
  {
    generator_.next(reader_);
    value_loaded_ = false;
    rep_lvl_pos_ = 0;
    def_lvl_pos_ = 0;
    val_buf_pos_ = 0;
    num_values_ = 0;
  }

  void resetBufferPos() {
    rep_lvl_pos_ = 0;
    def_lvl_pos_ = 0;
    val_buf_pos_ = 0;
  }

  void clearBuffer() {
    rep_lvls_.resize(0);
    def_lvls_.resize(0);
    val_buff_.resize(0);
    resetBufferPos();
    value_loaded_ = false;
  }

  void scanRecordBoundary();

  template<typename F>
  int applyFilter(F f){
    int r = f(num_values_, (void*)&val_buff_[0]);
    return r;
  }

  int nextDefinitionLevel() {
    if (def_lvl_pos_ < def_lvls_.size())
      return def_lvls_[def_lvl_pos_++];
    return 0;
  }

  int nextRepetitionLevel() {
  	if (rep_lvl_pos_ < rep_lvls_.size())
      return rep_lvls_[rep_lvl_pos_++];
	  return 0;
  }

  bool boolValue() {
    return reinterpret_cast<bool*>(&val_buff_[0])[val_buf_pos_];
  }

  int32_t int32Value() {
    return reinterpret_cast<int32_t*>(&val_buff_[0])[val_buf_pos_];
  }

  int64_t int64Value() {
    return reinterpret_cast<int64_t*>(&val_buff_[0])[val_buf_pos_];
  }

  Int96 int96Value() {
    return reinterpret_cast<Int96*>(&val_buff_[0])[val_buf_pos_];
  }

  float floatValue() {
    return reinterpret_cast<float*>(&val_buff_[0])[val_buf_pos_];
  }

  double doubleValue() {
    return reinterpret_cast<double*>(&val_buff_[0])[val_buf_pos_];
  }

  ByteArray byteArrayValue() {
    return reinterpret_cast<ByteArray*>(&val_buff_[0])[val_buf_pos_];
  }

  // number of values, including NULL
  int valueLoaded() const {
  	return value_loaded_; }

  bool HasNext() {
    if (reader_->HasNext())
      return true;
    generator_.next(reader_);
    return reader_->HasNext();
  }

protected:
  ColumnChunkGenerator& generator_;
  boost::shared_ptr<ColumnReader> reader_;
  bool value_loaded_;
  int num_values_;

  int rep_lvl_pos_;
  int def_lvl_pos_;
  int val_buf_pos_;
  std::vector<int> rep_lvls_;
  std::vector<int> def_lvls_;
  std::vector<uint8_t> val_buff_;
};

class ColumnConverterFactory {
public:
  virtual bool applyFilter() = 0;
  virtual ColumnValueChunk& GetChunk(int fid) = 0;
  virtual void consumeValueChunk(int fid, ColumnValueChunk& ch) = 0;
  virtual ~ColumnConverterFactory(){};
};

class RecordAssembler {
public:
  RecordAssembler(SchemaHelper& helper, ColumnConverterFactory& fac):
    helper_(helper), fac_(fac) {
  }

  void selectOutputColumns(const std::vector<std::string>& columns) {
    helper_.BuildFSM(columns, fsm_);
  }

  int assemble();
  
private:
  SchemaHelper& helper_;
  ColumnConverterFactory& fac_;
  SchemaFSM fsm_;
};

inline bool ColumnReader::HasNext() {
  if (num_buffered_values_ == 0) {
    ReadNewPage();
    if (num_buffered_values_ == 0) return false;
  }
  return true;
}

inline bool ColumnReader::GetBool(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return bool();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<bool*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline int32_t ColumnReader::GetInt32(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return int32_t();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<int32_t*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline int64_t ColumnReader::GetInt64(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return int64_t();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<int64_t*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline float ColumnReader::GetFloat(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return float();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<float*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline double ColumnReader::GetDouble(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return double();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<double*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline ByteArray ColumnReader::GetByteArray(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return ByteArray();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<ByteArray*>(&values_buffer_[0])[buffered_values_offset_++];
}

template<typename T>
int ColumnReader::GetValueBatch(ValueBatch<T>& batch, int max_values) {
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
          DecodeValues(&batch[start_p], batch.buffer_, i - start_p);
          state = 2;
        } break;
      case 2:
        if (!is_null) { state = 1; start_p = i; }
      }
    }
    if ( state == 1 )
      DecodeValues(&batch[start_p], batch.buffer_, def_values - start_p);
  } else {
    def_values = max_values;
    memset(&batch.def_levels_[0], 0, sizeof(int32_t) * def_values);
    def_values = DecodeValues(&batch[0], batch.buffer_, max_values);
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

}

#endif

