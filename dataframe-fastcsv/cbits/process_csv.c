#include "process_csv.h"
#include <stdlib.h>
#include <stdio.h>

/*
 * compile with clang -O3
 * in cabal use 
 *   cc-options: -O3
 *
 * Produce an array of field delimiter indices
 * Fields can be delimited by commas and newlines
 * TODO: allow the user to provide a custom delimiter
 * character to replace commas.
 * This should work with UTF-8, so long as the delimiter
 * character is a single byte.
 *
 * Delimiters can be escaped inside of quotes. Quotes
 * can also be placed inside quotes by double quoting.
 * For the purposes of this parser we can ignore double
 * quotes inside quotes, thereby treating the first quote
 * as the closing of the string and the next one the
 * immediate opening of a new one
 * 
 * We can find the quoted regions by first finding
 * the positions of the quotes (cmpeq and then movemask)
 * and then using the carryless multiplication operation
 * to know the regions that are quoted. We can then simply
 * and the inverse of the quotemask to exclude commas and
 * newlines inside quotes
 *
 */

// if the character is found at a particular
// position in the array of bytes, the
// corresponding bit in the returned uint64_t should
// be turned on.
// Example: searching for commas in
// input:  one, two, three
// result: 000100001000000
#ifdef HAS_SIMD_CSV
static uint64_t find_character_in_chunk(uint8_t *in, uint8_t c) {
#ifdef USE_AVX2
  // AVX2 implementation: load two 32-byte chunks
  __m256i v0 = _mm256_loadu_si256((const __m256i *)(in));
  __m256i v1 = _mm256_loadu_si256((const __m256i *)(in + 32));
  __m256i b = _mm256_set1_epi8((char)c);
  __m256i m0 = _mm256_cmpeq_epi8(v0, b);
  __m256i m1 = _mm256_cmpeq_epi8(v1, b);
  uint32_t lo = (uint32_t)_mm256_movemask_epi8(m0);
  uint32_t hi = (uint32_t)_mm256_movemask_epi8(m1);
  return ((uint64_t)hi << 32) | (uint64_t)lo;
#else // USE_NEON
  // ARM NEON implementation: load 64 bytes deinterleaved
  uint8x16x4_t src = vld4q_u8(in);
  uint8x16_t mask = vmovq_n_u8(c);
  uint8x16_t cmp0 = vceqq_u8(src.val[0], mask);
  uint8x16_t cmp1 = vceqq_u8(src.val[1], mask);
  uint8x16_t cmp2 = vceqq_u8(src.val[2], mask);
  uint8x16_t cmp3 = vceqq_u8(src.val[3], mask);

  // For an explanation of how to do movemask in
  // NEON, see: https://branchfree.org/2019/04/01/fitting-my-head-through-the-arm-holes-or-two-sequences-to-substitute-for-the-missing-pmovmskb-instruction-on-arm-neon/
  // The specific implementation below is owed to the 
  // user 'aqrit' in a comment on the blog above
  // There's also https://community.arm.com/arm-community-blogs/b/servers-and-cloud-computing-blog/posts/porting-x86-vector-bitmask-optimizations-to-arm-neon
  // 
  // The input to the move mask must be de-interleaved.
  // That is, for a string after vceqq_u8  we have,
  // cmp0 = aaaaaaaa / eeeeeeee / ...
  // cmp1 = bbbbbbbb / ffffffff / ...
  // cmp2 = cccccccc / gggggggg / ...
  // cmp3 = dddddddd / hhhhhhhh / ...
  // Luckily vld4q_u8 does this for us. Now we want
  // to interleave this into a 64bit integer with bits
  // abcdefgh...
  // cmp0 holds bits for positions
  // 0,4,8,..., cmp1 holds bits for 1,5,9,..., and so on.
  // So to bring together the bits for different positions
  // we right shift and combine with vsriq_n_u8
  //
  // vsriq_n_u8 shifts each byte of the first operand 
  // right by n bits, and combines it with the bits of
  // the second operand.
  // example:
  // uint8_t mask = 0xFF >> n; // if n = 1, 0111 1111
  // uint8_t shifted = operand1 >> n // 0bbb, bbbb
  // operand1 = (operand2 & (!mask)) | shifted
  // (aaaa aaaa & 1000 0000) | 0bbb bbbb = abbb bbbb
  // 
  // So we first bring together the bits the first two
  // rows and the next two rows (so to speak)
  // t0 = abbbbbbb/efffffff/...
  uint8x16_t t0 = vsriq_n_u8(cmp1, cmp0, 1);
  // t1 = cddddddd/ghhhhhhh/...
  uint8x16_t t1 = vsriq_n_u8(cmp3, cmp2, 1);
  // Now we must combine each of our combined rows
  // so that we get back our column interleaved
  // t2 = abcddddd/efghhhhh/...
  uint8x16_t t2 = vsriq_n_u8(t1, t0, 2);
  // Then to get rid of the repeated bits in the upper half
  // t3 = abcdabcd/efghefgh/...
  uint8x16_t t3 = vsriq_n_u8(t2, t2, 4);
  // and now it's the relatively simple matter of getting
  // rid of half the bits. We combine our 8bit words into 16
  // bit words for this step, and then we shift right by 4
  // and turn the result into an 8 bit word
  // afterreinterpert: abcdabcdefghefgh/...
  // afterrightshift: 0000abcdabcdefgh/...
  // take the lower bits: abcdefgh/...
  uint8x8_t t4 = vshrn_n_u16(vreinterpretq_u16_u8(t3), 4);
  // Finally we recombine them into a 64 bit integer
  // (vreinterpret_u64_u8 here does uint8x8 -> uint64x1
  // and vget_lane_u64 does uint64x1 -> uint64) 
  return vget_lane_u64(vreinterpret_u64_u8(t4), 0);
#endif
}
#endif

// I owe a debt to https://github.com/geofflangdale/simdcsv
// Let's go ahead and assume `in` will only ever get 64 bytes
// initial_quoted will be either all_ones ~0ULL or all_zeros 0ULL
#ifdef HAS_SIMD_CSV
static uint64_t parse_chunk(uint8_t *in, uint8_t separator, uint64_t *initial_quoted) {
  uint64_t quotebits = find_character_in_chunk(in, QUOTE_CHAR);
  // See https://wunkolo.github.io/post/2020/05/pclmulqdq-tricks/
  // Also, section 3.1.1 of Parsing Gigabytes of JSON per Second,
  // Geoff Langdale, Daniel Lemire, https://arxiv.org/pdf/1902.08318
#ifdef USE_AVX2
  // Use PCLMUL for carryless multiplication on x86
  __m128i a = _mm_set_epi64x(0, (int64_t)ALL_ONES_MASK);
  __m128i b = _mm_set_epi64x(0, (int64_t)quotebits);
  __m128i result = _mm_clmulepi64_si128(a, b, 0);
  uint64_t quotemask = (uint64_t)_mm_cvtsi128_si64(result);
#else // USE_NEON
  // Use vmull_p64 (PMULL) for carryless multiplication on ARM
  // Requires ARM crypto extensions (compile: __ARM_FEATURE_AES, runtime: pmull flag)
  uint64_t quotemask = vmull_p64(ALL_ONES_MASK, quotebits);
#endif
  quotemask ^= (*initial_quoted);
  // Find out if the chunk ends in a quoted region by looking
  // at the last bit
  (*initial_quoted) = (uint64_t)((int64_t)quotemask >> 63);

  uint64_t commabits = find_character_in_chunk(in, separator);
  uint64_t newlinebits = find_character_in_chunk(in, NEWLINE_CHAR);

  uint64_t delimiter_bits = (commabits | newlinebits) & ~quotemask;
  return delimiter_bits;
}
#endif

#ifdef HAS_SIMD_CSV
static size_t find_one_indices(size_t start_index, uint64_t bits, size_t *indices, size_t *base) {
  size_t position = 0;
  uint64_t bitset = bits;
  while (bitset != 0) {
    // temp only has the least significant bit of
    // bitset turned on.
    // In twos complement: 0 - x = ~ x + 1
    uint64_t temp = bitset & -bitset;
    // count trailing zeros
    size_t r = __builtin_ctzll(bitset);
    indices[(*base) + position] = start_index + r;
    position++;

    bitset ^= temp;
  }
  *base += position;
  return position;
}
#endif

size_t get_delimiter_indices(uint8_t *buf, size_t len, uint8_t separator, size_t *indices) {
  // Recall we padded our file with 64 empty bytes.
  // So if, for example, we had a file of 187 bytes
  // We pad it with zeros and so we have 251 bytes
  // The chunks we have are ptr + 0, ptr + 64, and pt
  // (we don't do ptr + 192 since we do len - 64
  // below. This way, in ptr + 128 we have 59 bytes of
  // actual data and 5 bytes of zeros. If we didn't do this
  // we'd be reading past the end of file on the last row
#ifdef HAS_SIMD_CSV
  size_t unpaddedLen = len < 64 ? 0 : len - 64;
  uint64_t initial_quoted = 0ULL;
  size_t base = 0;
  for (size_t i = 0; i < unpaddedLen; i += 64) {
    uint8_t *in = buf + i;
    uint64_t delimiter_bits = parse_chunk(in, separator, &initial_quoted);
    find_one_indices(i, delimiter_bits, indices, &base);
  }
  return base;
#else
  // SIMD not available or carryless multiplication not supported.
  // Signal fallback to Haskell implementation.
  (void)buf; (void)len; (void)indices;
  return (size_t)-1;
#endif
}
