#ifndef PROCESS_CSV
#define PROCESS_CSV

#include <stddef.h>
#include <stdint.h>

// Define feature macros for SIMD support with carryless multiplication
// We need both SIMD instructions AND carryless multiplication for the CSV parser
#if defined(__AVX2__) && defined(__PCLMUL__)
  #define HAS_SIMD_CSV 1
  #define USE_AVX2 1
  #include <immintrin.h>
  #include <wmmintrin.h>
#elif defined(__ARM_NEON) && (defined(__ARM_FEATURE_AES) || defined(__ARM_FEATURE_CRYPTO))
  // Note: __ARM_FEATURE_CRYPTO is deprecated; prefer __ARM_FEATURE_AES
  // We need polynomial multiply (vmull_p64/PMULL) for carryless multiplication
  // Runtime check: 'pmull' flag in /proc/cpuinfo on Linux
  // We support both macros for compatibility with older compilers
  #define HAS_SIMD_CSV 1
  #define USE_NEON 1
  #include <arm_neon.h>
#endif

// CSV parsing constants
#define COMMA_CHAR 0x2C
#define NEWLINE_CHAR 0x0A
#define QUOTE_CHAR 0x22
#define ALL_ONES_MASK ~0ULL
#define ALL_ZEROS_MASK 0ULL

size_t get_delimiter_indices(uint8_t *buf, size_t len, uint8_t separator, size_t* indices);

#endif
