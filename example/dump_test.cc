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

#include <parquet/parquet.h>
#include <iostream>
#include <stdio.h>

#include "example_util.h"

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

struct AnyType {
  union {
    bool bool_val;
    int32_t int32_val;
    int64_t int64_val;
    float float_val;
    double double_val;
    ByteArray byte_array_val;
  };
};

static string ByteArrayToString(const ByteArray& a) {
  return string(reinterpret_cast<const char*>(a.ptr), a.len);
}

class DumpColumnConverter : public ColumnConverter {
public:
  DumpColumnConverter(const string& path, const string& col_path)
  : gen_(path, col_path), col_path_(col_path) {
    gen_.next(&col_meta_data, reader_);
    value_read_ = 0;
  }

  ~DumpColumnConverter() {
  }

  bool next() {
    if (reader_->HasNext()) {
      _next_value();
      return true;
    } else {
      rep_lvl_ = def_lvl_ = 0;
      value_read_ --;
      if (value_read_ >= 0) {
        return true;
      }
      return false;
    }
  }

  bool HasNext() {
    return value_read_ > 0 || reader_->HasNext();
  }

  void consume() {
    cout<< col_path_ << " : ";
    if (def_lvl_ < gen_.GetMaxDefinitionLevel())
      cout << "NULL";
    else {
    switch (col_meta_data.type) {
    case Type::BOOLEAN: cout << value_.bool_val; break;
    case Type::INT32: cout << value_.int32_val; break;
    case Type::INT64: cout << value_.int64_val; break;
    case Type::FLOAT: cout << value_.float_val; break;
    case Type::DOUBLE: cout << value_.double_val; break;
    case Type::BYTE_ARRAY: cout << ByteArrayToString(value_.byte_array_val); break;
    default: break;
    }
    }
    cout << "\n";
    value_read_ --;
  }

private:
  void _next_value() {
    switch (col_meta_data.type) {
    case Type::BOOLEAN: value_.bool_val = reader_->GetBool(&def_lvl_, &rep_lvl_); break;
    case Type::INT32: value_.int32_val = reader_->GetInt32(&def_lvl_, &rep_lvl_); break;
    case Type::INT64: value_.int64_val = reader_->GetInt64(&def_lvl_, &rep_lvl_); break;
    case Type::FLOAT: value_.float_val = reader_->GetFloat(&def_lvl_, &rep_lvl_); break;
    case Type::DOUBLE: value_.double_val = reader_->GetDouble(&def_lvl_, &rep_lvl_); break;
    case Type::BYTE_ARRAY: value_.byte_array_val = reader_->GetByteArray(&def_lvl_, &rep_lvl_); break;
    }
    value_read_ ++;
  }
protected:
  ColumnChunkGenerator gen_;
  parquet::ColumnMetaData col_meta_data;
  string col_path_;
  AnyType value_;
  int value_read_;
};

class DumpColumnConverterFactory : public ColumnConverterFactory {
public:
  DumpColumnConverterFactory(SchemaHelper& helper, const string& file_path)
  : helper_(helper), file_path_(file_path) {
    converters_.resize(helper.schema.size());
  }

  ~DumpColumnConverterFactory() {
    for ( int i = 0 ; i < converters_.size(); ++i ) {
      delete converters_[i];
    }
  }

  virtual ColumnConverter* GetConverter(int fid) {
    if (converters_[fid] == NULL) {
      string col = helper_.GetElementPath(fid);
      converters_[fid] = new DumpColumnConverter(file_path_, col);
      converters_[fid]->next();
    }
    return converters_[fid];
  }
  
private:
  SchemaHelper& helper_;
  string file_path_;
  vector<ColumnConverter*> converters_;
};

// Simple example which reads all the values in the file and outputs the number of
// values, number of nulls and min/max for each column.
int main(int argc, char** argv) {
  if (argc < 2) {
    cerr << "dump_test: <file_name>\n";
    return 0;
  }
  
  SchemaHelper helper(argv[1]);
  vector<string> columns;
  columns.reserve( argc - 1 );
  for (int i = 2; i < argc ; ++i) {
    columns.push_back(argv[i]);
  }

  DumpColumnConverterFactory fac(helper, argv[1]);
  RecordAssembler ra = RecordAssembler(helper, fac);
  ra.selectOutputColumns(columns);

  while (ra.assemble()) {

  }
  return 0; 
}
