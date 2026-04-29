/*
 * dfusion_bridge.h
 *
 * C ABI for the Rust crate `dfusion-bridge`, which wraps Apache DataFusion
 * for use from Haskell. All payloads named *_json are UTF-8 encoded JSON
 * strings; expression payloads use the wire format produced by
 * DataFrame.IR.ExprJson.encodeExpr. Validity bitmaps and Arrow buffers cross
 * the boundary unchanged via the Arrow C Data Interface (see arrow_abi.h
 * in the parent dataframe package).
 */

#ifndef DFUSION_BRIDGE_H
#define DFUSION_BRIDGE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DfCtx  DfCtx;
typedef struct DfPlan DfPlan;

/* Context lifecycle. One DfCtx owns a tokio runtime and a SessionContext. */
DfCtx *df_ctx_new(void);
void   df_ctx_free(DfCtx *ctx);

/* Plan handles. df_plan_free is idempotent on NULL. */
void   df_plan_free(DfPlan *plan);

/* Thread-local last-error message. Pointer is valid until the next df_*
 * call on the same thread; copy if you need to keep it. */
const char *df_last_error(void);

/* ----- Sources ---------------------------------------------------------- */

/* Scan a CSV file. schema_json may be NULL for type inference; otherwise it
 * is a JSON object: { "fields": [["col_name", "int|double|text|bool"], ...] }.
 * Returns NULL on error (see df_last_error). */
DfPlan *df_scan_csv(DfCtx *ctx,
                    const char *path,
                    const char *schema_json);

/* ----- Operators -------------------------------------------------------- */

/* Filter rows where the boolean expression evaluates true. */
DfPlan *df_plan_filter(DfPlan *plan, const char *expr_json);

/* Keep at most `n` rows. */
DfPlan *df_plan_take(DfPlan *plan, uint64_t n);

/* Project to the named columns. names_json is a JSON array of strings. */
DfPlan *df_plan_select(DfPlan *plan, const char *names_json);

/* Add a derived column. */
DfPlan *df_plan_derive(DfPlan *plan,
                       const char *col_name,
                       const char *expr_json);

/* Sort by a list of (column, ascending) pairs.
 * orders_json shape: [{"col": "name", "asc": true}, ...] */
DfPlan *df_plan_sort_by(DfPlan *plan, const char *orders_json);

/* GroupBy + aggregate.
 *   keys_json:  JSON array of column-name strings to group by.
 *   aggs_json:  JSON array of {"name": "...", "expr": <agg-expr-json>} objects,
 *               where each agg-expr-json is a top-level "agg" node.            */
DfPlan *df_plan_groupby_aggregate(DfPlan *plan,
                                   const char *keys_json,
                                   const char *aggs_json);

/* Join two plans on key pairs.
 *   how:      "inner" | "left" | "right" | "outer"
 *   on_json:  JSON array of [left_key, right_key] string pairs.                */
DfPlan *df_plan_join(DfPlan *left,
                     DfPlan *right,
                     const char *how,
                     const char *on_json);

/* ----- Materialization -------------------------------------------------- */

/* Execute the plan, concatenate batches, and export the result via the
 * Arrow C Data Interface. *schema_out and *array_out receive the addresses
 * of newly allocated FFI_ArrowSchema and FFI_ArrowArray structs (cast to
 * uint64_t). The caller is responsible for invoking the producer's release
 * callbacks once the data has been copied (matches existing arrowToDataframe
 * in the dataframe package). Returns 0 on success, -1 on error. */
int32_t df_plan_collect(DfPlan *plan,
                        uint64_t *schema_out,
                        uint64_t *array_out);

#ifdef __cplusplus
}
#endif

#endif /* DFUSION_BRIDGE_H */
