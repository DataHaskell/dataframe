/* Arrow C Data Interface structs (verbatim from specification).
   See https://arrow.apache.org/docs/format/CDataInterface.html */

#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowSchema {
    /* Array type description */
    const char *format;
    const char *name;
    const char *metadata;
    int64_t     flags;
    int64_t     n_children;
    struct ArrowSchema **children;
    struct ArrowSchema  *dictionary;

    void (*release)(struct ArrowSchema *);
    void *private_data;
};

struct ArrowArray {
    /* Array data description */
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray  *dictionary;

    void (*release)(struct ArrowArray *);
    /* Opaque producer-specific data */
    void *private_data;
};

#ifdef __cplusplus
}
#endif
