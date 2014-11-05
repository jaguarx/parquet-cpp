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
    def_lvl_ = rep_lvl_ = 0;
    reader_->HasNext();
  }

  ~DumpColumnConverter() {
  }

  bool next() {
    if (reader_->HasNext()) {
      _next_value();
      return true;
    } else {
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

  void consume(int def_lvl) {
    cout<< " " << col_path_ << " : ";
    if (def_lvl < gen_.GetMaxDefinitionLevel())
      cout << "NULL";
    else {
    reader_->nextValue();
    switch (col_meta_data.type) {
    case Type::BOOLEAN: cout << reader_->boolValue(); break;
    case Type::INT32: cout << reader_->int32Value(); break;
    case Type::INT64: cout << reader_->int64Value(); break;
    case Type::FLOAT: cout << reader_->floatValue(); break;
    case Type::DOUBLE: cout << reader_->doubleValue(); break;
    case Type::BYTE_ARRAY: cout << ByteArrayToString(reader_->byteArrayValue()); break;
    default: break;
    }
    }
    cout << "\n";
    value_read_ --;
  }

private:
  void _next_value() {
/*
    switch (col_meta_data.type) {
    case Type::BOOLEAN: reader_->GetBool(&def_lvl_, &rep_lvl_); break;
    case Type::INT32: reader_->GetInt32(&def_lvl_, &rep_lvl_); break;
    case Type::INT64: reader_->GetInt64(&def_lvl_, &rep_lvl_); break;
    case Type::FLOAT: reader_->GetFloat(&def_lvl_, &rep_lvl_); break;
    case Type::DOUBLE: reader_->GetDouble(&def_lvl_, &rep_lvl_); break;
    case Type::BYTE_ARRAY: reader_->GetByteArray(&def_lvl_, &rep_lvl_); break;
    }*/
    //reader_->nextValue();
    //value_read_ ++;
  }
protected:
  ColumnChunkGenerator gen_;
  parquet::ColumnMetaData col_meta_data;
  string col_path_;
  int def_lvl_;
  int rep_lvl_;
  //AnyType value_;
  int value_read_;
};

class DumpColumnConverterFactory : public ColumnConverterFactory {
public:
  DumpColumnConverterFactory(SchemaHelper& helper, const string& file_path)
  : helper_(helper), file_path_(file_path) {
    converters_.resize(helper.schema.size());
    record_count_ = 0;
  }

  ~DumpColumnConverterFactory() {
    for ( int i = 0 ; i < converters_.size(); ++i ) {
      delete converters_[i];
    }
  }

  int applyFilter() {
    cerr << "##### record " << record_count_ << " ####\n";
    if (0 == (record_count_ & 1)) {
      int v = 0;
      for(int i=0; i<helper_.schema.size(); ++i) {
        const SchemaElement& e = helper_.schema[i];
        if (e.__isset.num_children)
          continue;
        ColumnConverter* cnv = GetConverter(i);
        if (cnv) {
          int si = cnv->skipRecord();
          if (cnv->reader_->HasNext())
            v |= 1;
        }
      }
      record_count_ ++;
      return (v>0? 1:0);
    }
    record_count_ ++;
    return 0;
  }

  virtual ColumnConverter* GetConverter(int fid) {
    if (converters_[fid] == NULL) {
      const string& col = helper_.GetElementPath(fid);
      if (col.size() <= 0)
        return NULL;
      //cerr << col << "\n";
      converters_[fid] = new DumpColumnConverter(file_path_, col);
      //converters_[fid]->next();
    }
    return converters_[fid];
  }
  
private:
  SchemaHelper& helper_;
  string file_path_;
  vector<ColumnConverter*> converters_;
  int record_count_;
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
