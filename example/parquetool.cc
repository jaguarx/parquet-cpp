
#include <parquet/parquet.h>
#include <parquet/record_filter.h>
#include <iostream>
#include <iomanip>
#include <stdio.h>
#include <unistd.h>

#include "example_util.h"

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

static inline void dump_bytes(ostream& oss, uint8_t* val, int len) {
  for(int i=0; i<len; ++i)
    oss << hex << val[i];
}

void dump_values(ostream& oss,
  const SchemaElement& element, uint8_t* buf,
  int32_t* def_lvls, int32_t max_def_lvl, int values) {
  for( int i=0; i < values; ++i) {
    if ((def_lvls != NULL) && (def_lvls[i] < max_def_lvl)) {
      oss << "NULL\n";
      continue;
    }
    switch(element.type) {
    case Type::BOOLEAN: oss << *(bool*)buf << "\n"; buf ++; break;
    case Type::INT32: oss << *(int32_t*)buf << "\n"; buf += sizeof(int32_t); break;
    case Type::INT64: oss << *(int64_t*)buf << "\n"; buf += sizeof(int64_t); break;
    case Type::FLOAT: oss << *(float*)buf << "\n"; buf += sizeof(float); break;
    case Type::DOUBLE: oss << *(double*)buf << "\n"; buf += sizeof(double); break;
    case Type::INT96: oss << *(Int96*)buf << "\n"; buf += sizeof(Int96); break;
    case Type::BYTE_ARRAY: oss << *(ByteArray*)buf << "\n"; buf += sizeof(ByteArray); break;
    case Type::FIXED_LEN_BYTE_ARRAY:
      dump_bytes(oss, buf, element.type_length); buf += element.type_length; break;
    }
  }
}

class DumpConvertor : public RecordConvertor {
public:
  DumpConvertor(SchemaHelper& helper, vector<ValueBatch*>& values)
  : helper_(helper), values_(values) {
    values_idx_.resize(values_.size());
  }
  void startRecord() {
    cout << "---\n";
    indent_ = 0;
  }

  void startGroup(int fid) {
    indent_ ++;
    for (int i=0; i<indent_; ++i) cout << "  ";
    cout << helper_.columns[fid].element.name << "\n";
  }
  void convertField(int fid, int idx) {
    for (int i=0; i<=indent_; ++i) cout << "  ";
    cout << helper_.columns[fid].element.name << " ";
    const ColumnDescriptor& desc = helper_.columns[fid];
    ValueBatch& batch = *(values_[fid]);
    switch (desc.element.type) {
    case Type::INT32: cout << batch.Get<int32_t>(idx); break;
    case Type::INT64: cout << batch.Get<int64_t>(idx); break;
    case Type::FLOAT: cout << batch.Get<float>(idx); break;
    case Type::DOUBLE: cout << batch.Get<double>(idx); break;
    case Type::BYTE_ARRAY: cout << batch.Get<ByteArray>(idx); break;
    default:
      cerr << "dump doesn't suppor type " << desc.element.type << "\n";
    }
    cout << "\n";
  }

  void endGroup() { indent_ --; }
  void endRecord() {
  }

private:
  int indent_;
  vector<int> values_idx_;
  vector<ValueBatch*> values_;
  SchemaHelper& helper_;
};

struct equal_int64 {
  int64_t v_;
  equal_int64 (int64_t v): v_(v){}

  int operator()(size_t num_values, void* buf) {
    if (num_values == 0)
      return 0;
    int64_t id = *(int64_t*)buf;
    if (id == v_)
      return 1;
    return 0;
  }
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
    parquet_cpp::DumpSchema(cout, h.columns);
  }
  return 0;
}

template<typename T>
void _batch_dump(ValueBatch& batch, int count) {
  for (int i=0; i<count; ++i) {
    cout << batch.RepetitionLevels()[i] << ":"
         << batch.DefinitionLevels()[i] << ":";
    if (batch.isNull(i)) cout << "NULL\n";
    else cout << batch.Get<T>(i) << "\n";
  }
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

  int batch_size = 128;
  ParquetFileReader reader(argv[optind]);
  const ColumnDescriptor& desc = reader.GetColumnDescriptor(col_idx);
  
  int count = 0;
  do {
    count = reader.LoadColumnData(col_idx, batch_size);
    ValueBatch& batch = reader.GetColumnValues(col_idx);
    switch (desc.element.type) {
    //case Type::BOOLEAN: _batch_dump<bool>(*reader, count); break;
    case Type::INT32: _batch_dump<int32_t>(batch, count); break;
    case Type::INT64: _batch_dump<int64_t>(batch, count); break;
    //case Type::INT96:
    case Type::FLOAT: _batch_dump<float>(batch, count); break;
    case Type::DOUBLE: _batch_dump<double>(batch, count); break;
    case Type::BYTE_ARRAY: _batch_dump<ByteArray>(batch, count); break;
    //case Type::FIXED_LENGTH_BYTE_ARRAY:
    default:
      cerr << "batch dump doesn't support type " << desc.element.type << " yet\n"; return 0;
    }
  } while (count == batch_size);
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
/*
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
*/
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

  //DumpColumnConverterFactory fac(filter_value, t, helper, filename);
  //RecordAssembler ra = RecordAssembler(helper, fac);
  //ra.selectOutputColumns(columns);

  //while (ra.assemble()) {

  //}
  return 0; 
}

int _cat(int argc, char** argv) {
  string filename;
  int count = 2;

  int opt;
  while ((opt = getopt(argc, argv, "c:v:f:")) != -1) {
    switch(opt) {
    case 'c': count = atoi(optarg); break;
    case 'f': filename = optarg; break;
    }   
  }

  ParquetFileReader reader(filename);
  SchemaHelper& helper = reader.GetHelper();

  vector<ValueBatch*> values(helper.columns.size());
  for(int i=1; i<helper.columns.size(); ++i)
    reader.LoadColumnData(i, count);

  vector<string> columns;
  columns.reserve( argc - 1 );

  if (optind < argc) {
    for (int i = optind; i < argc ; ++i) {
      int fid = reader.GetHelper().GetElementId(argv[i]);
      if (fid < 0) {
        cerr << "unknown field : " << argv[i] << "\n";
        return 0;
      }
      columns.push_back(argv[i]);
      values[fid] = &(reader.GetColumnValues(fid));
    }
  } else {
    for (int i=1; i<helper.columns.size(); ++i)
      values[i] = &(reader.GetColumnValues(i));
  }

  DumpConvertor dump(helper, values);
  RecordAssembler ra = RecordAssembler(helper, values, dump);
  ra.selectOutputColumns(columns);

  while (count > 0) {
    count --;
    ra.assemble();
  }
  return 0;
}

command_t commands[] = {
  command_t("schema", "<files>\n"
                          "\t\tshow schema of files",
     _show_schema),
  command_t("chunk", "-c <col_id> <file>\n"
                          "\t\tdump column chunk info",
     _dump_column_chunk),
  command_t("dump", "-c <col_id> <file>\n"
                          "\t\tdump column data",
     _dump_columns),
  command_t("search",     "-c <filter_col_id> -v <value> -f <files> <output-columns>\n"
                          "\t\tsearch and assemble matching record",
     _search),
  command_t("cat",        "-c <columns> <file>\n"
                          "\t\tprint records",
     _cat),
  command_t()
};

void usage() {
  for (int i=0; commands[i].name != NULL; ++i)
    cerr << "  " << commands[i].name << "\t" << commands[i].help << "\n";
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
