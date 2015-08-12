
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

static inline void dump_bytes(ostream& oss, uint8_t* val, int len) {
  for(int i=0; i<len; ++i)
    oss << hex << val[i];
}

void dump_values(ostream& oss,
  const parquet::SchemaElement& element, uint8_t* buf,
  int32_t* def_lvls, int32_t max_def_lvl, int values) {
  for( int i=0; i < values; ++i) {
    if ((def_lvls != NULL) && (def_lvls[i] < max_def_lvl)) {
      oss << "NULL\n";
      continue;
    }
    switch(element.type) {
    case parquet::Type::BOOLEAN: oss << *(bool*)buf << "\n"; buf ++; break;
    case parquet::Type::INT32: oss << *(int32_t*)buf << "\n"; buf += sizeof(int32_t); break;
    case parquet::Type::INT64: oss << *(int64_t*)buf << "\n"; buf += sizeof(int64_t); break;
    case parquet::Type::FLOAT: oss << *(float*)buf << "\n"; buf += sizeof(float); break;
    case parquet::Type::DOUBLE: oss << *(double*)buf << "\n"; buf += sizeof(double); break;
    case parquet::Type::INT96: oss << *(Int96*)buf << "\n"; buf += sizeof(Int96); break;
    case parquet::Type::BYTE_ARRAY: oss << *(ByteArray*)buf << "\n"; buf += sizeof(ByteArray); break;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      dump_bytes(oss, buf, element.type_length); buf += element.type_length; break;
    }
  }
}

class DumpColumnValueChunk : public ColumnValueChunk {
public:
  DumpColumnValueChunk(ColumnChunkGenerator& generator, const string& col_path)
  : ColumnValueChunk(generator), element_(generator.schemaElement()), col_path_(col_path) {
  }

  void dumpNextValue() {
    int def_lvl = def_lvls_[def_lvl_pos_];
    def_lvl_pos_ ++;
    if (def_lvl == reader_->MaxDefinitionLevel()) {
      cout << " " << col_path_ << " : ";
      dump_values(cout, generator_.schemaElement(), &val_buff_[0], NULL, 0, 1);
      cout << "\n";
      val_buf_pos_++;
    }
  }

private:
  parquet::SchemaElement element_;
  string col_path_;
};

struct equal_int64 {
  int64_t v_;
  equal_int64 (int64_t v): v_(v){}

  int operator()(size_t num_values, const vector<int>& rep_lvls, const vector<int>& def_lvls, void* buf) {
    if (num_values == 0)
      return 0;
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
      ColumnValueChunk& vc = fac_.GetChunk(id);
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
    value_chunks_.resize(helper.schema.size());

    record_count_ = 0;
  }

  ~DumpColumnConverterFactory() {
  }

  bool applyFilter() {
    record_count_ ++;
    matchset_t s(*this);
    bool r = tree_.eval(0, s);
    cerr << "##### record " << record_count_ << " ####\n";
    return !r;
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
      value_chunks_[fid] = new DumpColumnValueChunk(*gens_[fid], col);
    }
    return *value_chunks_[fid];
  }

private:
  int64_t filter_value_;
  bool_expr_tree_t& tree_;
  SchemaHelper& helper_;
  string file_path_;
  vector<ColumnChunkGenerator*> gens_;
  vector<DumpColumnValueChunk*> value_chunks_;
  int record_count_;
};

struct command_t {
  typedef int (func_t)(int, char**);
  const char* name;
  const char* help;
  func_t* func;
  command_t(const char* n, const char* h, func_t* f)
  : name(n), help(h), func(f) {}

  command_t() : name(NULL), help(NULL), func(NULL){}
};

int _show_schema(int argc, char** argv) {
  for (int i=1; i<argc; ++i) {
    if (argc > 2)
      cout << argv[i] << "\n";
    SchemaHelper h(argv[i]);
    list<int> child_stack;
    for (int col_idx = 0; col_idx < h.schema.size(); ++col_idx) {
      const SchemaElement& element = h.schema[col_idx];
      cout << " " << col_idx << " : " 
           << _FieldRepetitionType_VALUES_TO_NAMES.find(element.repetition_type)->second << " ";
      for (int j=0; j<child_stack.size(); ++j) cout << "  ";
      cout << element.name << "\t";
      if (element.num_children == 0) {
        child_stack.front() --;
        if (child_stack.front() == 0)
          child_stack.pop_front();
        cout << _Type_VALUES_TO_NAMES.find(element.type)->second;
      } else {
        child_stack.push_front(element.num_children);
      }
      cout << "\n";
    }
  }
  return 0;
}

int _dump_columns(int argc, char** argv) {
  int opt;
  int col_idx = 1;
  while ((opt = getopt(argc, argv, "c:")) != -1) {
    switch (opt) {
    case 'c': col_idx = atoi(optarg); break;
    }
  }
  if (optind >= argc)
    return 1;

  vector<uint8_t> buf;
  ColumnChunkGenerator gen(argv[optind], col_idx);
  boost::shared_ptr<ColumnReader> reader;
  while (gen.next(reader)) {
    int64_t num_values = gen.columnMetaData().num_values;
    reader->HasNext();
    int batch_size = std::min(4096, (int)num_values);
    while (batch_size > 0) {
      vector<int32_t> def_lvls;
      batch_size = reader->decodeValues(buf, def_lvls, batch_size);
      if (batch_size > 0) {
        num_values -= batch_size;
        dump_values(cout, gen.schemaElement(), &buf[0],
                    &def_lvls[0],
                    reader->MaxDefinitionLevel(), batch_size);
        batch_size = std::min(4096, (int)num_values);
      }
    }
  }
  return 0;
}

int _dump_column_chunk(int argc, char** argv) {
  int opt;
  int col_idx = 1;
  while ((opt = getopt(argc, argv, "c:")) != -1) {
    switch (opt) {
    case 'c': col_idx = atoi(optarg); break;
    }
  }
  if (optind >= argc)
    return 1;

  vector<uint8_t> buf;
  ColumnChunkGenerator gen(argv[optind], col_idx);
  boost::shared_ptr<ColumnReader> reader;
  while (gen.next(reader)) {
    const ColumnMetaData& cmd = gen.columnMetaData();
    cout<< "num_values:" << cmd.num_values
        << ", size:" << cmd.total_compressed_size << "/" << cmd.total_uncompressed_size
        << ", encodings: ";
    for(int i=0; i< cmd.encodings.size(); ++i) {
        cout << ((i>0)?",":"") << cmd.encodings[i];
    }
    cout << "\n";
  }
  return 0;
}

int _search(int argc, char** argv) {
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

command_t commands[] = {
  command_t("showschema", "<files>\n"
                          "\t\tshow schema of files",
     _show_schema),
  command_t("columnchunk", "-c <col_id> <file>\n"
                          "\t\tdump column chunk info",
     _dump_column_chunk),
  command_t("dumpcolumn", "-c <col_id> <file>\n"
                          "\t\tdump column data",
     _dump_columns),
  command_t("search",     "-c <filter_col_id> -v <value> -f <files> <output-columns>\n"
                          "\t\tsearch and assemble matching record",
     _search),
  command_t()
};

void usage() {
  for (int i=0; commands[i].name != NULL; ++i) {
    cerr << "  " << commands[i].name << "\t" << commands[i].help << "\n";
  }
  exit(1);
}

int main(int argc, char** argv) {
  if (argc < 2)
    usage();

  for (int i=0; commands[i].name; ++i) {
    if (strcmp(argv[1], commands[i].name) == 0)
      return commands[i].func(argc-1, argv+1);
  }
  usage();
  return 0;
}
