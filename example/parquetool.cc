
#include <parquet/parquet.h>
#include <parquet/record_filter.h>
#include <iostream>
#include <iomanip>
#include <stdio.h>
#include <unistd.h>

#include "example_util.h"
#include "util/json_convertor.h"

using namespace parquet;
using namespace parquet_cpp;
using namespace std;
using namespace boost;

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
 
  vector<bool> bitmask(batch_size);
  bitmask[0] = true; 
  int count = 0;
  do {
    count = reader.LoadColumnData(col_idx, batch_size);//, bitmask);
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

class WrapppedDump : public json::JsonConvertor {
public:
  WrapppedDump(bool pretty, SchemaHelper& helper, vector<ValueBatch*>& values)
  : JsonConvertor(cout, helper, values, pretty) {}

  void endRecord() {
    JsonConvertor::endRecord();
    cout << "\n";
  }
};

int _cat(int argc, char** argv) {
  string filename;
  int count = 2;
  int start = 0;
  int json_pretty = 1;

  int opt;
  while ((opt = getopt(argc, argv, "s:c:f:j:")) != -1) {
    switch(opt) {
    case 'c': count = atoi(optarg); break;
    case 's': start = atoi(optarg); break;
    case 'f': filename = optarg; break;
    case 'j': json_pretty = atoi(optarg); break;
    }   
  }

  ParquetFileReader reader(filename);
  SchemaHelper& helper = reader.GetHelper();

  int batch_size = 128;

  vector<ValueBatch*> values(helper.columns.size());
  vector<bool> bitmask(batch_size);
  for (int i=0; i<batch_size; ++i) bitmask[i] = true;

  if (start > 0) {
    while (start >= batch_size) {
      for(int i=1; i<helper.columns.size(); ++i)
        reader.LoadColumnData(i, batch_size, bitmask);
      start -= batch_size;
    }
    if (start > 0)
      for(int i=1; i<helper.columns.size(); ++i)
        reader.LoadColumnData(i, start, bitmask);
  }
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
    for (int i=1; i<helper.columns.size(); ++i) {
      values[i] = &(reader.GetColumnValues(i));
    }
  }

  WrapppedDump dump((json_pretty > 0)?true:false, helper, values);
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
  command_t("chunk",  "-c <col_id> <file>\n"
                      "\t\tdump column chunk info",
     _dump_column_chunk),
  command_t("dump",   "-c <col_id> <file>\n"
                      "\t\tdump column data",
     _dump_columns),
  command_t("cat",    "-j <pretty> -s <start_idx> -c count -f <file> [columns]\n"
                      "\t\tprint records\n"
                      "\t\t -j <pretty>     print as JSON with pretty print\n"
                      "\t\t -s <start_idx>  print from # record\n"
                      "\t\t -c <count>      number of records to print\n"
                      "\t\t -f <file>       parquet file to print\n",
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
