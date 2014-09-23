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

#ifndef PARQUET_READ_SUPPORT_H
#define PARQUET_READ_SUPPORT_H

#include <string>
#include <parquet/parquet.h>

namespace parquet_cpp {
bool GetFileMetadata(const std::string& path, parquet::FileMetaData* metadata);
void* mmapfile(const std::string& path, size_t offset, size_t size);

class MmapMemoryInputStream : public InMemoryInputStream {
 public:
  MmapMemoryInputStream(const std::string& path, uint64_t offset, uint64_t size);
  ~MmapMemoryInputStream();
 private:
  int fd_;
  uint64_t size_;
  const uint8_t* buffer_;
};
};
#endif
