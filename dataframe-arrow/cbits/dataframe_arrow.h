#pragma once
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Execute a JSON-encoded query plan and return Arrow C Data Interface pointers.
 *
 * plan_json  – null-terminated UTF-8 JSON (see DataFrame.IR for schema)
 * schema_out – receives ArrowSchema* cast to uint64_t
 * array_out  – receives ArrowArray*  cast to uint64_t
 *
 * Returns 0 on success, -1 on error (message written to stderr).
 *
 * Plan ops: ReadCsv, ReadTsv, FromArrow, Select, GroupBy, Sort, Limit,
 *           Filter, Derive
 *
 * Example (GroupBy):
 *   {"op":"GroupBy","keys":["Sex"],
 *    "aggregations":[{"name":"n","agg":"count","col":"Sex"}],
 *    "input":{"op":"ReadCsv","path":"data/titanic.csv"}}
 */
int dfExecutePlan(const char* plan_json,
                  uint64_t*   schema_out,
                  uint64_t*   array_out);

/**
 * Fit a TAO decision tree on the materialized result of plan_json, predicting
 * target_col from the remaining columns. The trained tree is serialized to
 * JSON and copied into a freshly malloc'd buffer; the buffer pointer is
 * written to *model_json_out and its length to *model_len_out.
 *
 * The caller owns the returned buffer and must release it with dfFreeModel.
 *
 * target_type   – wire-format type tag ("int", "double", "text", "bool", …)
 *                 or the literal "auto" to infer the target's type from the
 *                 materialized DataFrame's schema.
 * config_json   – JSON object with serializable TreeConfig fields:
 *                   max_depth, min_samples_split, min_leaf_size,
 *                   percentiles, expression_pairs, tao_iterations,
 *                   tao_convergence_tol, max_expr_depth, bool_expansion,
 *                   complexity_penalty, enable_string_ops, enable_cross_cols,
 *                   enable_arith_ops
 *                 Missing fields fall back to defaultTreeConfig.
 *
 * Returns 0 on success, -1 on error.
 */
int dfFitDecisionTree(const char* plan_json,
                      const char* target_col,
                      const char* target_type,
                      const char* config_json,
                      char**      model_json_out,
                      uint64_t*   model_len_out);

/**
 * Release a tree-model buffer allocated by dfFitDecisionTree.
 */
void dfFreeModel(char* model_json_ptr);

#ifdef __cplusplus
}
#endif
