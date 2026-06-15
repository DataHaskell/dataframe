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

// Status codes reported via the `status` out-parameter of
// `get_delimiter_indices`.
#define GDI_OK 0
#define GDI_UNCLOSED_QUOTE 1

// Sentinel returned by `get_delimiter_indices` when the build lacks
// SIMD support or the running CPU does not advertise the carryless
// multiply extensions needed by the fast path.  The caller falls back
// to the Haskell state machine.
#define GDI_SIMD_UNAVAILABLE ((size_t)-1)

size_t get_delimiter_indices(uint8_t *buf, size_t len, uint8_t separator,
                             size_t *indices, int *status);

// Chunked parallel-scan entry points (see process_csv.c).  Both return
// GDI_SIMD_UNAVAILABLE when the build/CPU lacks SIMD support.
size_t count_delimiters_chunk(const uint8_t *buf, size_t base, size_t len,
                              uint8_t separator, int start_in_quote,
                              size_t *newline_count);

size_t emit_delimiter_indices_chunk(const uint8_t *buf, size_t base,
                                    size_t len, uint8_t separator,
                                    int start_in_quote, size_t ordinal_base,
                                    size_t *indices, size_t *rowends,
                                    size_t *rowend_count);

#endif
