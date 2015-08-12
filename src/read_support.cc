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

#include "read_support.h"
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

using namespace parquet;
using namespace std;

namespace parquet_cpp {
// 4 byte constant + 4 byte metadata len
const uint32_t FOOTER_SIZE = 8;
const uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

struct ScopedFile {
 public:
  ScopedFile(FILE* f) : file_(f) { }
  ~ScopedFile() { fclose(file_); }

 private:
  FILE* file_;
};

bool GetFileMetadata(const string& path, FileMetaData* metadata) {
  FILE* file = fopen(path.c_str(), "r");
  if (!file) {
    cerr << "Could not open file: " << path << endl;
    return false;
  }
  ScopedFile cleanup(file);
  fseek(file, 0L, SEEK_END);
  size_t file_len = ftell(file);
  if (file_len < FOOTER_SIZE) {
    cerr << "Invalid parquet file. Corrupt footer." << endl;
    return false;
  }

  uint8_t footer_buffer[FOOTER_SIZE];
  fseek(file, file_len - FOOTER_SIZE, SEEK_SET);
  size_t bytes_read = fread(footer_buffer, 1, FOOTER_SIZE, file);
  if (bytes_read != FOOTER_SIZE) {
    cerr << "Invalid parquet file. Corrupt footer." << endl;
    return false;
  }
  if (memcmp(footer_buffer + 4, PARQUET_MAGIC, 4) != 0) {
    cerr << "Invalid parquet file. Corrupt footer." << endl;
    return false;
  }

  uint32_t metadata_len = *reinterpret_cast<uint32_t*>(footer_buffer);
  size_t metadata_start = file_len - FOOTER_SIZE - metadata_len;
  if (metadata_start < 0) {
    cerr << "Invalid parquet file. File is less than file metadata size." << endl;
    return false;
  }

  fseek(file, metadata_start, SEEK_SET);
  uint8_t metadata_buffer[metadata_len];
  bytes_read = fread(metadata_buffer, 1, metadata_len, file);
  if (bytes_read != metadata_len) {
    cerr << "Invalid parquet file. Could not read metadata bytes." << endl;
    return false;
  }

  DeserializeThriftMsg(metadata_buffer, &metadata_len, metadata);
  return true;
}

const uint8_t * _mmap_file(const string& path, uint64_t& off, uint64_t len, int& fd) {
  char errbuf[128] = {0};
  fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    strerror_r(errno, errbuf, sizeof(errbuf)-1);
    throw ParquetException(errbuf);
  }
  off_t pa_off = off & ~(sysconf(_SC_PAGE_SIZE) - 1);
  uint8_t* addr = (uint8_t*)mmap(NULL, len + off - pa_off, PROT_READ, 
    MAP_PRIVATE, fd, pa_off);
  if (addr == MAP_FAILED) {
    strerror_r(errno, errbuf, sizeof(errbuf)-1);
    throw ParquetException(errbuf);
  }
  off -= pa_off;
  return (const uint8_t*)(addr + off);
}

MmapMemoryInputStream::MmapMemoryInputStream(const std::string& path, uint64_t offset, uint64_t size)
 : InMemoryInputStream(buffer_ = _mmap_file(path, offset, size, fd_), size) {
  buffer_ -= offset;
  size_ = size + offset;
}

MmapMemoryInputStream::~MmapMemoryInputStream() {
  if (fd_ > 0) {
    munmap((void*)buffer_, size_);
    close(fd_);
  }
}

}
