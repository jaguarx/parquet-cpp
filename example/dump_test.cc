
#include <parquet/parquet.h>
#include <parquet/record_filter.h>
#include <iostream>
#include <stdio.h>
#include <unistd.h>

#include "example_util.h"

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

ostream& operator<<(ostream& oss, const ByteArray& a) {
  oss.write((const char*)a.ptr, a.len);
  return oss;
}

ostream& operator<<(ostream& oss, const Int96& v){
  oss << hex << v.i[0];
  oss << hex << v.i[1];
  oss << hex << v.i[2];
  return oss;
}

class DumpColumnValueChunk : public ColumnValueChunk {
public:
  DumpColumnValueChunk(ColumnReader& reader, const string& col_path)
  : ColumnValueChunk(reader), col_path_(col_path) {

  }
  void dumpNextValue() {
    int def_lvl = def_lvls_[def_lvl_pos_];
    def_lvl_pos_ ++;
    if (def_lvl == reader_.MaxDefinitionLevel()) {
      cout << " " << col_path_ << " : ";
      switch (reader_.metadata_->type) {
      case parquet::Type::BOOLEAN: cout << boolValue(); break;
      case parquet::Type::INT32: cout << int32Value(); break;
      case parquet::Type::INT64: cout << int64Value(); break;
      case parquet::Type::FLOAT: cout << floatValue(); break;
      case parquet::Type::DOUBLE: cout << doubleValue(); break;
      case parquet::Type::INT96: cout << int96Value(); break;
      case parquet::Type::BYTE_ARRAY: cout << byteArrayValue(); break;
      }
      cout << "\n";
      val_buf_pos_++;
    }
  }
private:
  string col_path_;
};

struct equal_int64 {
  int64_t v_;
  equal_int64 (int64_t v): v_(v){}

  int operator()(size_t num_values, void* buf) {
    if (num_values == 0)
      return 1;
    int64_t id = *(int64_t*)buf;
    if (id == v_)
      return 1;
    return 0;
  }
};

class DumpColumnConverterFactory : public ColumnConverterFactory {
public:
  struct matchset_t {
    DumpColumnConverterFactory& fac_;

    bool get(int id) {
      ColumnValueChunk& vc = fac_.GetChunk(1);
      vc.scanRecordBoundary();
      return 1 == vc.applyFilter(equal_int64(fac_.filter_value_));
    }

    matchset_t(DumpColumnConverterFactory& fac)
    : fac_(fac){}
  };

  DumpColumnConverterFactory(int64_t filter_value,
                             bool_expr_tree_t& tree,
    SchemaHelper& helper, const string& file_path)
  : filter_value_(filter_value), tree_(tree),
    helper_(helper), file_path_(file_path) {

    gens_.resize(helper.schema.size());
    col_metadatas_.resize(helper.schema.size());
    readers_.resize(helper.schema.size());
    value_chunks_.resize(helper.schema.size());

    record_count_ = 0;
  }

  ~DumpColumnConverterFactory() {
  }

  bool applyFilter() {
    record_count_ ++;
    matchset_t s(*this);
    bool r = tree_.eval(0, s);
    if (!r) {
      cerr << "##### record " << record_count_ << " ####\n";
    }
    return r;
  }

  void consumeValueChunk(int fid, ColumnValueChunk& ch) {
    value_chunks_[fid]->dumpNextValue();
  }
  
  virtual ColumnValueChunk& GetChunk(int fid) {
    if (gens_[fid] == NULL) {
      const string& col = helper_.GetElementPath(fid);
      gens_[fid] = new ColumnChunkGenerator(file_path_, col);
    }
    if (value_chunks_[fid] == NULL) {
      const string& col = helper_.GetElementPath(fid);
      parquet::ColumnMetaData& col_meta_data = col_metadatas_[fid];
      gens_[fid]->next(&col_meta_data, readers_[fid]);
      readers_[fid]->HasNext();
      value_chunks_[fid] = new DumpColumnValueChunk(*readers_[fid], col);
    }
    return *value_chunks_[fid];
  }
private:
  int64_t filter_value_;
  bool_expr_tree_t& tree_;
  SchemaHelper& helper_;
  string file_path_;
  vector<ColumnChunkGenerator*> gens_;
  vector<parquet::ColumnMetaData> col_metadatas_;
  vector<boost::shared_ptr<ColumnReader> > readers_;
  vector<DumpColumnValueChunk*> value_chunks_;
  int record_count_;
};

int main(int argc, char** argv) {
  int filter_value = 0;
  int col_id = 1;
  string filename;
  vector<string> columns;

  int opt;
  while ((opt = getopt(argc, argv, "c:v:f:")) != -1) {
    switch(opt) {
    case 'c': col_id = atoi(optarg); break;
    case 'v': filter_value = atoi(optarg); break;
    case 'f': filename = optarg; break;
    }
  }

  vector<expr_node_t> nodes;
  nodes.push_back(expr_node_t(col_id));
  bool_expr_tree_t t(nodes);

  SchemaHelper helper(filename);
  columns.reserve( argc - 1 );
  for (int i = optind; i < argc ; ++i) {
    columns.push_back(argv[i]);
  }

  DumpColumnConverterFactory fac(filter_value, t, helper, filename);
  RecordAssembler ra = RecordAssembler(helper, fac);
  ra.selectOutputColumns(columns);

  while (ra.assemble()) {

  }
  return 0; 
}
