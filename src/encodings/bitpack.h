
template <class T>
int unpack1_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]);
  out[0] = 0x1 & t;
  out[1] = 0x1 & (t >> 1);
  out[2] = 0x1 & (t >> 2);
  out[3] = 0x1 & (t >> 3);
  out[4] = 0x1 & (t >> 4);
  out[5] = 0x1 & (t >> 5);
  out[6] = 0x1 & (t >> 6);
  out[7] = 0x1 & (t >> 7);
  return 1;
}


template <class T>
int unpack2_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]);
  out[0] = 0x3 & t;
  out[1] = 0x3 & (t >> 2);
  out[2] = 0x3 & (t >> 4);
  out[3] = 0x3 & (t >> 6);
  t = ((uint32_t)in[1]);
  out[4] = 0x3 & t;
  out[5] = 0x3 & (t >> 2);
  out[6] = 0x3 & (t >> 4);
  out[7] = 0x3 & (t >> 6);
  return 2;
}


template <class T>
int unpack3_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]);
  out[0] = 0x7 & t;
  out[1] = 0x7 & (t >> 3);
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[2] = 0x7 & (t >> 6);
  t = ((uint32_t)in[1]);
  out[3] = 0x7 & (t >> 1);
  out[4] = 0x7 & (t >> 4);
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8);
  out[5] = 0x7 & (t >> 7);
  t = ((uint32_t)in[2]);
  out[6] = 0x7 & (t >> 2);
  out[7] = 0x7 & (t >> 5);
  return 3;
}


template <class T>
int unpack4_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]);
  out[0] = 0xf & t;
  out[1] = 0xf & (t >> 4);
  t = ((uint32_t)in[1]);
  out[2] = 0xf & t;
  out[3] = 0xf & (t >> 4);
  t = ((uint32_t)in[2]);
  out[4] = 0xf & t;
  out[5] = 0xf & (t >> 4);
  t = ((uint32_t)in[3]);
  out[6] = 0xf & t;
  out[7] = 0xf & (t >> 4);
  return 4;
}


template <class T>
int unpack5_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]);
  out[0] = 0x1f & t;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[1] = 0x1f & (t >> 5);
  t = ((uint32_t)in[1]);
  out[2] = 0x1f & (t >> 2);
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8);
  out[3] = 0x1f & (t >> 7);
  t = ((uint32_t)in[2]) | (((uint32_t)in[3])<<8);
  out[4] = 0x1f & (t >> 4);
  t = ((uint32_t)in[3]);
  out[5] = 0x1f & (t >> 1);
  t = ((uint32_t)in[3]) | (((uint32_t)in[4])<<8);
  out[6] = 0x1f & (t >> 6);
  t = ((uint32_t)in[4]);
  out[7] = 0x1f & (t >> 3);
  return 5;
}


template <class T>
int unpack6_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]);
  out[0] = 0x3f & t;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[1] = 0x3f & (t >> 6);
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8);
  out[2] = 0x3f & (t >> 4);
  t = ((uint32_t)in[2]);
  out[3] = 0x3f & (t >> 2);
  t = ((uint32_t)in[3]);
  out[4] = 0x3f & t;
  t = ((uint32_t)in[3]) | (((uint32_t)in[4])<<8);
  out[5] = 0x3f & (t >> 6);
  t = ((uint32_t)in[4]) | (((uint32_t)in[5])<<8);
  out[6] = 0x3f & (t >> 4);
  t = ((uint32_t)in[5]);
  out[7] = 0x3f & (t >> 2);
  return 6;
}


template <class T>
int unpack7_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]);
  out[0] = 0x7f & t;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[1] = 0x7f & (t >> 7);
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8);
  out[2] = 0x7f & (t >> 6);
  t = ((uint32_t)in[2]) | (((uint32_t)in[3])<<8);
  out[3] = 0x7f & (t >> 5);
  t = ((uint32_t)in[3]) | (((uint32_t)in[4])<<8);
  out[4] = 0x7f & (t >> 4);
  t = ((uint32_t)in[4]) | (((uint32_t)in[5])<<8);
  out[5] = 0x7f & (t >> 3);
  t = ((uint32_t)in[5]) | (((uint32_t)in[6])<<8);
  out[6] = 0x7f & (t >> 2);
  t = ((uint32_t)in[6]);
  out[7] = 0x7f & (t >> 1);
  return 7;
}


template <class T>
int unpack8_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]);
  out[0] = 0xff & t;
  t = ((uint32_t)in[1]);
  out[1] = 0xff & t;
  t = ((uint32_t)in[2]);
  out[2] = 0xff & t;
  t = ((uint32_t)in[3]);
  out[3] = 0xff & t;
  t = ((uint32_t)in[4]);
  out[4] = 0xff & t;
  t = ((uint32_t)in[5]);
  out[5] = 0xff & t;
  t = ((uint32_t)in[6]);
  out[6] = 0xff & t;
  t = ((uint32_t)in[7]);
  out[7] = 0xff & t;
  return 8;
}


template <class T>
int unpack9_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[0] = 0x1ff & t;
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8);
  out[1] = 0x1ff & (t >> 1);
  t = ((uint32_t)in[2]) | (((uint32_t)in[3])<<8);
  out[2] = 0x1ff & (t >> 2);
  t = ((uint32_t)in[3]) | (((uint32_t)in[4])<<8);
  out[3] = 0x1ff & (t >> 3);
  t = ((uint32_t)in[4]) | (((uint32_t)in[5])<<8);
  out[4] = 0x1ff & (t >> 4);
  t = ((uint32_t)in[5]) | (((uint32_t)in[6])<<8);
  out[5] = 0x1ff & (t >> 5);
  t = ((uint32_t)in[6]) | (((uint32_t)in[7])<<8);
  out[6] = 0x1ff & (t >> 6);
  t = ((uint32_t)in[7]) | (((uint32_t)in[8])<<8);
  out[7] = 0x1ff & (t >> 7);
  return 9;
}


template <class T>
int unpack10_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[0] = 0x3ff & t;
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8);
  out[1] = 0x3ff & (t >> 2);
  t = ((uint32_t)in[2]) | (((uint32_t)in[3])<<8);
  out[2] = 0x3ff & (t >> 4);
  t = ((uint32_t)in[3]) | (((uint32_t)in[4])<<8);
  out[3] = 0x3ff & (t >> 6);
  t = ((uint32_t)in[5]) | (((uint32_t)in[6])<<8);
  out[4] = 0x3ff & t;
  t = ((uint32_t)in[6]) | (((uint32_t)in[7])<<8);
  out[5] = 0x3ff & (t >> 2);
  t = ((uint32_t)in[7]) | (((uint32_t)in[8])<<8);
  out[6] = 0x3ff & (t >> 4);
  t = ((uint32_t)in[8]) | (((uint32_t)in[9])<<8);
  out[7] = 0x3ff & (t >> 6);
  return 10;
}


template <class T>
int unpack11_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[0] = 0x7ff & t;
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8);
  out[1] = 0x7ff & (t >> 3);
  t = ((uint32_t)in[2]) | (((uint32_t)in[3])<<8) | (((uint32_t)in[4])<<16);
  out[2] = 0x7ff & (t >> 6);
  t = ((uint32_t)in[4]) | (((uint32_t)in[5])<<8);
  out[3] = 0x7ff & (t >> 1);
  t = ((uint32_t)in[5]) | (((uint32_t)in[6])<<8);
  out[4] = 0x7ff & (t >> 4);
  t = ((uint32_t)in[6]) | (((uint32_t)in[7])<<8) | (((uint32_t)in[8])<<16);
  out[5] = 0x7ff & (t >> 7);
  t = ((uint32_t)in[8]) | (((uint32_t)in[9])<<8);
  out[6] = 0x7ff & (t >> 2);
  t = ((uint32_t)in[9]) | (((uint32_t)in[10])<<8);
  out[7] = 0x7ff & (t >> 5);
  return 11;
}


template <class T>
int unpack12_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[0] = 0xfff & t;
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8);
  out[1] = 0xfff & (t >> 4);
  t = ((uint32_t)in[3]) | (((uint32_t)in[4])<<8);
  out[2] = 0xfff & t;
  t = ((uint32_t)in[4]) | (((uint32_t)in[5])<<8);
  out[3] = 0xfff & (t >> 4);
  t = ((uint32_t)in[6]) | (((uint32_t)in[7])<<8);
  out[4] = 0xfff & t;
  t = ((uint32_t)in[7]) | (((uint32_t)in[8])<<8);
  out[5] = 0xfff & (t >> 4);
  t = ((uint32_t)in[9]) | (((uint32_t)in[10])<<8);
  out[6] = 0xfff & t;
  t = ((uint32_t)in[10]) | (((uint32_t)in[11])<<8);
  out[7] = 0xfff & (t >> 4);
  return 12;
}


template <class T>
int unpack13_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[0] = 0x1fff & t;
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8) | (((uint32_t)in[3])<<16);
  out[1] = 0x1fff & (t >> 5);
  t = ((uint32_t)in[3]) | (((uint32_t)in[4])<<8);
  out[2] = 0x1fff & (t >> 2);
  t = ((uint32_t)in[4]) | (((uint32_t)in[5])<<8) | (((uint32_t)in[6])<<16);
  out[3] = 0x1fff & (t >> 7);
  t = ((uint32_t)in[6]) | (((uint32_t)in[7])<<8) | (((uint32_t)in[8])<<16);
  out[4] = 0x1fff & (t >> 4);
  t = ((uint32_t)in[8]) | (((uint32_t)in[9])<<8);
  out[5] = 0x1fff & (t >> 1);
  t = ((uint32_t)in[9]) | (((uint32_t)in[10])<<8) | (((uint32_t)in[11])<<16);
  out[6] = 0x1fff & (t >> 6);
  t = ((uint32_t)in[11]) | (((uint32_t)in[12])<<8);
  out[7] = 0x1fff & (t >> 3);
  return 13;
}


template <class T>
int unpack14_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[0] = 0x3fff & t;
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8) | (((uint32_t)in[3])<<16);
  out[1] = 0x3fff & (t >> 6);
  t = ((uint32_t)in[3]) | (((uint32_t)in[4])<<8) | (((uint32_t)in[5])<<16);
  out[2] = 0x3fff & (t >> 4);
  t = ((uint32_t)in[5]) | (((uint32_t)in[6])<<8);
  out[3] = 0x3fff & (t >> 2);
  t = ((uint32_t)in[7]) | (((uint32_t)in[8])<<8);
  out[4] = 0x3fff & t;
  t = ((uint32_t)in[8]) | (((uint32_t)in[9])<<8) | (((uint32_t)in[10])<<16);
  out[5] = 0x3fff & (t >> 6);
  t = ((uint32_t)in[10]) | (((uint32_t)in[11])<<8) | (((uint32_t)in[12])<<16);
  out[6] = 0x3fff & (t >> 4);
  t = ((uint32_t)in[12]) | (((uint32_t)in[13])<<8);
  out[7] = 0x3fff & (t >> 2);
  return 14;
}


template <class T>
int unpack15_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[0] = 0x7fff & t;
  t = ((uint32_t)in[1]) | (((uint32_t)in[2])<<8) | (((uint32_t)in[3])<<16);
  out[1] = 0x7fff & (t >> 7);
  t = ((uint32_t)in[3]) | (((uint32_t)in[4])<<8) | (((uint32_t)in[5])<<16);
  out[2] = 0x7fff & (t >> 6);
  t = ((uint32_t)in[5]) | (((uint32_t)in[6])<<8) | (((uint32_t)in[7])<<16);
  out[3] = 0x7fff & (t >> 5);
  t = ((uint32_t)in[7]) | (((uint32_t)in[8])<<8) | (((uint32_t)in[9])<<16);
  out[4] = 0x7fff & (t >> 4);
  t = ((uint32_t)in[9]) | (((uint32_t)in[10])<<8) | (((uint32_t)in[11])<<16);
  out[5] = 0x7fff & (t >> 3);
  t = ((uint32_t)in[11]) | (((uint32_t)in[12])<<8) | (((uint32_t)in[13])<<16);
  out[6] = 0x7fff & (t >> 2);
  t = ((uint32_t)in[13]) | (((uint32_t)in[14])<<8);
  out[7] = 0x7fff & (t >> 1);
  return 15;
}


template <class T>
int unpack16_8(uint8_t* in, T* out) {
  uint32_t t = 0;
  t = ((uint32_t)in[0]) | (((uint32_t)in[1])<<8);
  out[0] = 0xffff & t;
  t = ((uint32_t)in[2]) | (((uint32_t)in[3])<<8);
  out[1] = 0xffff & t;
  t = ((uint32_t)in[4]) | (((uint32_t)in[5])<<8);
  out[2] = 0xffff & t;
  t = ((uint32_t)in[6]) | (((uint32_t)in[7])<<8);
  out[3] = 0xffff & t;
  t = ((uint32_t)in[8]) | (((uint32_t)in[9])<<8);
  out[4] = 0xffff & t;
  t = ((uint32_t)in[10]) | (((uint32_t)in[11])<<8);
  out[5] = 0xffff & t;
  t = ((uint32_t)in[12]) | (((uint32_t)in[13])<<8);
  out[6] = 0xffff & t;
  t = ((uint32_t)in[14]) | (((uint32_t)in[15])<<8);
  out[7] = 0xffff & t;
  return 16;
}


template <class T>
int unpack17_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16);
  out[0] = 0x1ffff & t;
  t = ((uint64_t)in[2]) | (((uint64_t)in[3])<<8) | (((uint64_t)in[4])<<16);
  out[1] = 0x1ffff & (t >> 1);
  t = ((uint64_t)in[4]) | (((uint64_t)in[5])<<8) | (((uint64_t)in[6])<<16);
  out[2] = 0x1ffff & (t >> 2);
  t = ((uint64_t)in[6]) | (((uint64_t)in[7])<<8) | (((uint64_t)in[8])<<16);
  out[3] = 0x1ffff & (t >> 3);
  t = ((uint64_t)in[8]) | (((uint64_t)in[9])<<8) | (((uint64_t)in[10])<<16);
  out[4] = 0x1ffff & (t >> 4);
  t = ((uint64_t)in[10]) | (((uint64_t)in[11])<<8) | (((uint64_t)in[12])<<16);
  out[5] = 0x1ffff & (t >> 5);
  t = ((uint64_t)in[12]) | (((uint64_t)in[13])<<8) | (((uint64_t)in[14])<<16);
  out[6] = 0x1ffff & (t >> 6);
  t = ((uint64_t)in[14]) | (((uint64_t)in[15])<<8) | (((uint64_t)in[16])<<16);
  out[7] = 0x1ffff & (t >> 7);
  return 17;
}


template <class T>
int unpack18_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16);
  out[0] = 0x3ffff & t;
  t = ((uint64_t)in[2]) | (((uint64_t)in[3])<<8) | (((uint64_t)in[4])<<16);
  out[1] = 0x3ffff & (t >> 2);
  t = ((uint64_t)in[4]) | (((uint64_t)in[5])<<8) | (((uint64_t)in[6])<<16);
  out[2] = 0x3ffff & (t >> 4);
  t = ((uint64_t)in[6]) | (((uint64_t)in[7])<<8) | (((uint64_t)in[8])<<16);
  out[3] = 0x3ffff & (t >> 6);
  t = ((uint64_t)in[9]) | (((uint64_t)in[10])<<8) | (((uint64_t)in[11])<<16);
  out[4] = 0x3ffff & t;
  t = ((uint64_t)in[11]) | (((uint64_t)in[12])<<8) | (((uint64_t)in[13])<<16);
  out[5] = 0x3ffff & (t >> 2);
  t = ((uint64_t)in[13]) | (((uint64_t)in[14])<<8) | (((uint64_t)in[15])<<16);
  out[6] = 0x3ffff & (t >> 4);
  t = ((uint64_t)in[15]) | (((uint64_t)in[16])<<8) | (((uint64_t)in[17])<<16);
  out[7] = 0x3ffff & (t >> 6);
  return 18;
}


template <class T>
int unpack19_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16);
  out[0] = 0x7ffff & t;
  t = ((uint64_t)in[2]) | (((uint64_t)in[3])<<8) | (((uint64_t)in[4])<<16);
  out[1] = 0x7ffff & (t >> 3);
  t = ((uint64_t)in[4]) | (((uint64_t)in[5])<<8) | (((uint64_t)in[6])<<16) | (((uint64_t)in[7])<<24);
  out[2] = 0x7ffff & (t >> 6);
  t = ((uint64_t)in[7]) | (((uint64_t)in[8])<<8) | (((uint64_t)in[9])<<16);
  out[3] = 0x7ffff & (t >> 1);
  t = ((uint64_t)in[9]) | (((uint64_t)in[10])<<8) | (((uint64_t)in[11])<<16);
  out[4] = 0x7ffff & (t >> 4);
  t = ((uint64_t)in[11]) | (((uint64_t)in[12])<<8) | (((uint64_t)in[13])<<16) | (((uint64_t)in[14])<<24);
  out[5] = 0x7ffff & (t >> 7);
  t = ((uint64_t)in[14]) | (((uint64_t)in[15])<<8) | (((uint64_t)in[16])<<16);
  out[6] = 0x7ffff & (t >> 2);
  t = ((uint64_t)in[16]) | (((uint64_t)in[17])<<8) | (((uint64_t)in[18])<<16);
  out[7] = 0x7ffff & (t >> 5);
  return 19;
}


template <class T>
int unpack20_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16);
  out[0] = 0xfffff & t;
  t = ((uint64_t)in[2]) | (((uint64_t)in[3])<<8) | (((uint64_t)in[4])<<16);
  out[1] = 0xfffff & (t >> 4);
  t = ((uint64_t)in[5]) | (((uint64_t)in[6])<<8) | (((uint64_t)in[7])<<16);
  out[2] = 0xfffff & t;
  t = ((uint64_t)in[7]) | (((uint64_t)in[8])<<8) | (((uint64_t)in[9])<<16);
  out[3] = 0xfffff & (t >> 4);
  t = ((uint64_t)in[10]) | (((uint64_t)in[11])<<8) | (((uint64_t)in[12])<<16);
  out[4] = 0xfffff & t;
  t = ((uint64_t)in[12]) | (((uint64_t)in[13])<<8) | (((uint64_t)in[14])<<16);
  out[5] = 0xfffff & (t >> 4);
  t = ((uint64_t)in[15]) | (((uint64_t)in[16])<<8) | (((uint64_t)in[17])<<16);
  out[6] = 0xfffff & t;
  t = ((uint64_t)in[17]) | (((uint64_t)in[18])<<8) | (((uint64_t)in[19])<<16);
  out[7] = 0xfffff & (t >> 4);
  return 20;
}


template <class T>
int unpack21_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16);
  out[0] = 0x1fffff & t;
  t = ((uint64_t)in[2]) | (((uint64_t)in[3])<<8) | (((uint64_t)in[4])<<16) | (((uint64_t)in[5])<<24);
  out[1] = 0x1fffff & (t >> 5);
  t = ((uint64_t)in[5]) | (((uint64_t)in[6])<<8) | (((uint64_t)in[7])<<16);
  out[2] = 0x1fffff & (t >> 2);
  t = ((uint64_t)in[7]) | (((uint64_t)in[8])<<8) | (((uint64_t)in[9])<<16) | (((uint64_t)in[10])<<24);
  out[3] = 0x1fffff & (t >> 7);
  t = ((uint64_t)in[10]) | (((uint64_t)in[11])<<8) | (((uint64_t)in[12])<<16) | (((uint64_t)in[13])<<24);
  out[4] = 0x1fffff & (t >> 4);
  t = ((uint64_t)in[13]) | (((uint64_t)in[14])<<8) | (((uint64_t)in[15])<<16);
  out[5] = 0x1fffff & (t >> 1);
  t = ((uint64_t)in[15]) | (((uint64_t)in[16])<<8) | (((uint64_t)in[17])<<16) | (((uint64_t)in[18])<<24);
  out[6] = 0x1fffff & (t >> 6);
  t = ((uint64_t)in[18]) | (((uint64_t)in[19])<<8) | (((uint64_t)in[20])<<16);
  out[7] = 0x1fffff & (t >> 3);
  return 21;
}


template <class T>
int unpack22_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16);
  out[0] = 0x3fffff & t;
  t = ((uint64_t)in[2]) | (((uint64_t)in[3])<<8) | (((uint64_t)in[4])<<16) | (((uint64_t)in[5])<<24);
  out[1] = 0x3fffff & (t >> 6);
  t = ((uint64_t)in[5]) | (((uint64_t)in[6])<<8) | (((uint64_t)in[7])<<16) | (((uint64_t)in[8])<<24);
  out[2] = 0x3fffff & (t >> 4);
  t = ((uint64_t)in[8]) | (((uint64_t)in[9])<<8) | (((uint64_t)in[10])<<16);
  out[3] = 0x3fffff & (t >> 2);
  t = ((uint64_t)in[11]) | (((uint64_t)in[12])<<8) | (((uint64_t)in[13])<<16);
  out[4] = 0x3fffff & t;
  t = ((uint64_t)in[13]) | (((uint64_t)in[14])<<8) | (((uint64_t)in[15])<<16) | (((uint64_t)in[16])<<24);
  out[5] = 0x3fffff & (t >> 6);
  t = ((uint64_t)in[16]) | (((uint64_t)in[17])<<8) | (((uint64_t)in[18])<<16) | (((uint64_t)in[19])<<24);
  out[6] = 0x3fffff & (t >> 4);
  t = ((uint64_t)in[19]) | (((uint64_t)in[20])<<8) | (((uint64_t)in[21])<<16);
  out[7] = 0x3fffff & (t >> 2);
  return 22;
}


template <class T>
int unpack23_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16);
  out[0] = 0x7fffff & t;
  t = ((uint64_t)in[2]) | (((uint64_t)in[3])<<8) | (((uint64_t)in[4])<<16) | (((uint64_t)in[5])<<24);
  out[1] = 0x7fffff & (t >> 7);
  t = ((uint64_t)in[5]) | (((uint64_t)in[6])<<8) | (((uint64_t)in[7])<<16) | (((uint64_t)in[8])<<24);
  out[2] = 0x7fffff & (t >> 6);
  t = ((uint64_t)in[8]) | (((uint64_t)in[9])<<8) | (((uint64_t)in[10])<<16) | (((uint64_t)in[11])<<24);
  out[3] = 0x7fffff & (t >> 5);
  t = ((uint64_t)in[11]) | (((uint64_t)in[12])<<8) | (((uint64_t)in[13])<<16) | (((uint64_t)in[14])<<24);
  out[4] = 0x7fffff & (t >> 4);
  t = ((uint64_t)in[14]) | (((uint64_t)in[15])<<8) | (((uint64_t)in[16])<<16) | (((uint64_t)in[17])<<24);
  out[5] = 0x7fffff & (t >> 3);
  t = ((uint64_t)in[17]) | (((uint64_t)in[18])<<8) | (((uint64_t)in[19])<<16) | (((uint64_t)in[20])<<24);
  out[6] = 0x7fffff & (t >> 2);
  t = ((uint64_t)in[20]) | (((uint64_t)in[21])<<8) | (((uint64_t)in[22])<<16);
  out[7] = 0x7fffff & (t >> 1);
  return 23;
}


template <class T>
int unpack24_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16);
  out[0] = 0xffffff & t;
  t = ((uint64_t)in[3]) | (((uint64_t)in[4])<<8) | (((uint64_t)in[5])<<16);
  out[1] = 0xffffff & t;
  t = ((uint64_t)in[6]) | (((uint64_t)in[7])<<8) | (((uint64_t)in[8])<<16);
  out[2] = 0xffffff & t;
  t = ((uint64_t)in[9]) | (((uint64_t)in[10])<<8) | (((uint64_t)in[11])<<16);
  out[3] = 0xffffff & t;
  t = ((uint64_t)in[12]) | (((uint64_t)in[13])<<8) | (((uint64_t)in[14])<<16);
  out[4] = 0xffffff & t;
  t = ((uint64_t)in[15]) | (((uint64_t)in[16])<<8) | (((uint64_t)in[17])<<16);
  out[5] = 0xffffff & t;
  t = ((uint64_t)in[18]) | (((uint64_t)in[19])<<8) | (((uint64_t)in[20])<<16);
  out[6] = 0xffffff & t;
  t = ((uint64_t)in[21]) | (((uint64_t)in[22])<<8) | (((uint64_t)in[23])<<16);
  out[7] = 0xffffff & t;
  return 24;
}


template <class T>
int unpack25_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16) | (((uint64_t)in[3])<<24);
  out[0] = 0x1ffffff & t;
  t = ((uint64_t)in[3]) | (((uint64_t)in[4])<<8) | (((uint64_t)in[5])<<16) | (((uint64_t)in[6])<<24);
  out[1] = 0x1ffffff & (t >> 1);
  t = ((uint64_t)in[6]) | (((uint64_t)in[7])<<8) | (((uint64_t)in[8])<<16) | (((uint64_t)in[9])<<24);
  out[2] = 0x1ffffff & (t >> 2);
  t = ((uint64_t)in[9]) | (((uint64_t)in[10])<<8) | (((uint64_t)in[11])<<16) | (((uint64_t)in[12])<<24);
  out[3] = 0x1ffffff & (t >> 3);
  t = ((uint64_t)in[12]) | (((uint64_t)in[13])<<8) | (((uint64_t)in[14])<<16) | (((uint64_t)in[15])<<24);
  out[4] = 0x1ffffff & (t >> 4);
  t = ((uint64_t)in[15]) | (((uint64_t)in[16])<<8) | (((uint64_t)in[17])<<16) | (((uint64_t)in[18])<<24);
  out[5] = 0x1ffffff & (t >> 5);
  t = ((uint64_t)in[18]) | (((uint64_t)in[19])<<8) | (((uint64_t)in[20])<<16) | (((uint64_t)in[21])<<24);
  out[6] = 0x1ffffff & (t >> 6);
  t = ((uint64_t)in[21]) | (((uint64_t)in[22])<<8) | (((uint64_t)in[23])<<16) | (((uint64_t)in[24])<<24);
  out[7] = 0x1ffffff & (t >> 7);
  return 25;
}


template <class T>
int unpack26_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16) | (((uint64_t)in[3])<<24);
  out[0] = 0x3ffffff & t;
  t = ((uint64_t)in[3]) | (((uint64_t)in[4])<<8) | (((uint64_t)in[5])<<16) | (((uint64_t)in[6])<<24);
  out[1] = 0x3ffffff & (t >> 2);
  t = ((uint64_t)in[6]) | (((uint64_t)in[7])<<8) | (((uint64_t)in[8])<<16) | (((uint64_t)in[9])<<24);
  out[2] = 0x3ffffff & (t >> 4);
  t = ((uint64_t)in[9]) | (((uint64_t)in[10])<<8) | (((uint64_t)in[11])<<16) | (((uint64_t)in[12])<<24);
  out[3] = 0x3ffffff & (t >> 6);
  t = ((uint64_t)in[13]) | (((uint64_t)in[14])<<8) | (((uint64_t)in[15])<<16) | (((uint64_t)in[16])<<24);
  out[4] = 0x3ffffff & t;
  t = ((uint64_t)in[16]) | (((uint64_t)in[17])<<8) | (((uint64_t)in[18])<<16) | (((uint64_t)in[19])<<24);
  out[5] = 0x3ffffff & (t >> 2);
  t = ((uint64_t)in[19]) | (((uint64_t)in[20])<<8) | (((uint64_t)in[21])<<16) | (((uint64_t)in[22])<<24);
  out[6] = 0x3ffffff & (t >> 4);
  t = ((uint64_t)in[22]) | (((uint64_t)in[23])<<8) | (((uint64_t)in[24])<<16) | (((uint64_t)in[25])<<24);
  out[7] = 0x3ffffff & (t >> 6);
  return 26;
}


template <class T>
int unpack27_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16) | (((uint64_t)in[3])<<24);
  out[0] = 0x7ffffff & t;
  t = ((uint64_t)in[3]) | (((uint64_t)in[4])<<8) | (((uint64_t)in[5])<<16) | (((uint64_t)in[6])<<24);
  out[1] = 0x7ffffff & (t >> 3);
  t = ((uint64_t)in[6]) | (((uint64_t)in[7])<<8) | (((uint64_t)in[8])<<16) | (((uint64_t)in[9])<<24) | (((uint64_t)in[10])<<32);
  out[2] = 0x7ffffff & (t >> 6);
  t = ((uint64_t)in[10]) | (((uint64_t)in[11])<<8) | (((uint64_t)in[12])<<16) | (((uint64_t)in[13])<<24);
  out[3] = 0x7ffffff & (t >> 1);
  t = ((uint64_t)in[13]) | (((uint64_t)in[14])<<8) | (((uint64_t)in[15])<<16) | (((uint64_t)in[16])<<24);
  out[4] = 0x7ffffff & (t >> 4);
  t = ((uint64_t)in[16]) | (((uint64_t)in[17])<<8) | (((uint64_t)in[18])<<16) | (((uint64_t)in[19])<<24) | (((uint64_t)in[20])<<32);
  out[5] = 0x7ffffff & (t >> 7);
  t = ((uint64_t)in[20]) | (((uint64_t)in[21])<<8) | (((uint64_t)in[22])<<16) | (((uint64_t)in[23])<<24);
  out[6] = 0x7ffffff & (t >> 2);
  t = ((uint64_t)in[23]) | (((uint64_t)in[24])<<8) | (((uint64_t)in[25])<<16) | (((uint64_t)in[26])<<24);
  out[7] = 0x7ffffff & (t >> 5);
  return 27;
}


template <class T>
int unpack28_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16) | (((uint64_t)in[3])<<24);
  out[0] = 0xfffffff & t;
  t = ((uint64_t)in[3]) | (((uint64_t)in[4])<<8) | (((uint64_t)in[5])<<16) | (((uint64_t)in[6])<<24);
  out[1] = 0xfffffff & (t >> 4);
  t = ((uint64_t)in[7]) | (((uint64_t)in[8])<<8) | (((uint64_t)in[9])<<16) | (((uint64_t)in[10])<<24);
  out[2] = 0xfffffff & t;
  t = ((uint64_t)in[10]) | (((uint64_t)in[11])<<8) | (((uint64_t)in[12])<<16) | (((uint64_t)in[13])<<24);
  out[3] = 0xfffffff & (t >> 4);
  t = ((uint64_t)in[14]) | (((uint64_t)in[15])<<8) | (((uint64_t)in[16])<<16) | (((uint64_t)in[17])<<24);
  out[4] = 0xfffffff & t;
  t = ((uint64_t)in[17]) | (((uint64_t)in[18])<<8) | (((uint64_t)in[19])<<16) | (((uint64_t)in[20])<<24);
  out[5] = 0xfffffff & (t >> 4);
  t = ((uint64_t)in[21]) | (((uint64_t)in[22])<<8) | (((uint64_t)in[23])<<16) | (((uint64_t)in[24])<<24);
  out[6] = 0xfffffff & t;
  t = ((uint64_t)in[24]) | (((uint64_t)in[25])<<8) | (((uint64_t)in[26])<<16) | (((uint64_t)in[27])<<24);
  out[7] = 0xfffffff & (t >> 4);
  return 28;
}


template <class T>
int unpack29_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16) | (((uint64_t)in[3])<<24);
  out[0] = 0x1fffffff & t;
  t = ((uint64_t)in[3]) | (((uint64_t)in[4])<<8) | (((uint64_t)in[5])<<16) | (((uint64_t)in[6])<<24) | (((uint64_t)in[7])<<32);
  out[1] = 0x1fffffff & (t >> 5);
  t = ((uint64_t)in[7]) | (((uint64_t)in[8])<<8) | (((uint64_t)in[9])<<16) | (((uint64_t)in[10])<<24);
  out[2] = 0x1fffffff & (t >> 2);
  t = ((uint64_t)in[10]) | (((uint64_t)in[11])<<8) | (((uint64_t)in[12])<<16) | (((uint64_t)in[13])<<24) | (((uint64_t)in[14])<<32);
  out[3] = 0x1fffffff & (t >> 7);
  t = ((uint64_t)in[14]) | (((uint64_t)in[15])<<8) | (((uint64_t)in[16])<<16) | (((uint64_t)in[17])<<24) | (((uint64_t)in[18])<<32);
  out[4] = 0x1fffffff & (t >> 4);
  t = ((uint64_t)in[18]) | (((uint64_t)in[19])<<8) | (((uint64_t)in[20])<<16) | (((uint64_t)in[21])<<24);
  out[5] = 0x1fffffff & (t >> 1);
  t = ((uint64_t)in[21]) | (((uint64_t)in[22])<<8) | (((uint64_t)in[23])<<16) | (((uint64_t)in[24])<<24) | (((uint64_t)in[25])<<32);
  out[6] = 0x1fffffff & (t >> 6);
  t = ((uint64_t)in[25]) | (((uint64_t)in[26])<<8) | (((uint64_t)in[27])<<16) | (((uint64_t)in[28])<<24);
  out[7] = 0x1fffffff & (t >> 3);
  return 29;
}


template <class T>
int unpack30_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16) | (((uint64_t)in[3])<<24);
  out[0] = 0x3fffffff & t;
  t = ((uint64_t)in[3]) | (((uint64_t)in[4])<<8) | (((uint64_t)in[5])<<16) | (((uint64_t)in[6])<<24) | (((uint64_t)in[7])<<32);
  out[1] = 0x3fffffff & (t >> 6);
  t = ((uint64_t)in[7]) | (((uint64_t)in[8])<<8) | (((uint64_t)in[9])<<16) | (((uint64_t)in[10])<<24) | (((uint64_t)in[11])<<32);
  out[2] = 0x3fffffff & (t >> 4);
  t = ((uint64_t)in[11]) | (((uint64_t)in[12])<<8) | (((uint64_t)in[13])<<16) | (((uint64_t)in[14])<<24);
  out[3] = 0x3fffffff & (t >> 2);
  t = ((uint64_t)in[15]) | (((uint64_t)in[16])<<8) | (((uint64_t)in[17])<<16) | (((uint64_t)in[18])<<24);
  out[4] = 0x3fffffff & t;
  t = ((uint64_t)in[18]) | (((uint64_t)in[19])<<8) | (((uint64_t)in[20])<<16) | (((uint64_t)in[21])<<24) | (((uint64_t)in[22])<<32);
  out[5] = 0x3fffffff & (t >> 6);
  t = ((uint64_t)in[22]) | (((uint64_t)in[23])<<8) | (((uint64_t)in[24])<<16) | (((uint64_t)in[25])<<24) | (((uint64_t)in[26])<<32);
  out[6] = 0x3fffffff & (t >> 4);
  t = ((uint64_t)in[26]) | (((uint64_t)in[27])<<8) | (((uint64_t)in[28])<<16) | (((uint64_t)in[29])<<24);
  out[7] = 0x3fffffff & (t >> 2);
  return 30;
}


template <class T>
int unpack31_8(uint8_t* in, T* out) {
  uint64_t t = 0;
  t = ((uint64_t)in[0]) | (((uint64_t)in[1])<<8) | (((uint64_t)in[2])<<16) | (((uint64_t)in[3])<<24);
  out[0] = 0x7fffffff & t;
  t = ((uint64_t)in[3]) | (((uint64_t)in[4])<<8) | (((uint64_t)in[5])<<16) | (((uint64_t)in[6])<<24) | (((uint64_t)in[7])<<32);
  out[1] = 0x7fffffff & (t >> 7);
  t = ((uint64_t)in[7]) | (((uint64_t)in[8])<<8) | (((uint64_t)in[9])<<16) | (((uint64_t)in[10])<<24) | (((uint64_t)in[11])<<32);
  out[2] = 0x7fffffff & (t >> 6);
  t = ((uint64_t)in[11]) | (((uint64_t)in[12])<<8) | (((uint64_t)in[13])<<16) | (((uint64_t)in[14])<<24) | (((uint64_t)in[15])<<32);
  out[3] = 0x7fffffff & (t >> 5);
  t = ((uint64_t)in[15]) | (((uint64_t)in[16])<<8) | (((uint64_t)in[17])<<16) | (((uint64_t)in[18])<<24) | (((uint64_t)in[19])<<32);
  out[4] = 0x7fffffff & (t >> 4);
  t = ((uint64_t)in[19]) | (((uint64_t)in[20])<<8) | (((uint64_t)in[21])<<16) | (((uint64_t)in[22])<<24) | (((uint64_t)in[23])<<32);
  out[5] = 0x7fffffff & (t >> 3);
  t = ((uint64_t)in[23]) | (((uint64_t)in[24])<<8) | (((uint64_t)in[25])<<16) | (((uint64_t)in[26])<<24) | (((uint64_t)in[27])<<32);
  out[6] = 0x7fffffff & (t >> 2);
  t = ((uint64_t)in[27]) | (((uint64_t)in[28])<<8) | (((uint64_t)in[29])<<16) | (((uint64_t)in[30])<<24);
  out[7] = 0x7fffffff & (t >> 1);
  return 31;
}


template<class T>
int unpack_8(int bitwidth, uint8_t* in, T* out) {
  switch(bitwidth) {
  case 1: return unpack1_8(in, out);
  case 2: return unpack2_8(in, out);
  case 3: return unpack3_8(in, out);
  case 4: return unpack4_8(in, out);
  case 5: return unpack5_8(in, out);
  case 6: return unpack6_8(in, out);
  case 7: return unpack7_8(in, out);
  case 8: return unpack8_8(in, out);
  case 9: return unpack9_8(in, out);
  case 10: return unpack10_8(in, out);
  case 11: return unpack11_8(in, out);
  case 12: return unpack12_8(in, out);
  case 13: return unpack13_8(in, out);
  case 14: return unpack14_8(in, out);
  case 15: return unpack15_8(in, out);
  case 16: return unpack16_8(in, out);
  case 17: return unpack17_8(in, out);
  case 18: return unpack18_8(in, out);
  case 19: return unpack19_8(in, out);
  case 20: return unpack20_8(in, out);
  case 21: return unpack21_8(in, out);
  case 22: return unpack22_8(in, out);
  case 23: return unpack23_8(in, out);
  case 24: return unpack24_8(in, out);
  case 25: return unpack25_8(in, out);
  case 26: return unpack26_8(in, out);
  case 27: return unpack27_8(in, out);
  case 28: return unpack28_8(in, out);
  case 29: return unpack29_8(in, out);
  case 30: return unpack30_8(in, out);
  case 31: return unpack31_8(in, out);
  default: return -1;
  }
}

