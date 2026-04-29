//! C ABI shim that exposes Apache DataFusion to Haskell.
//!
//! Each plan-extending function takes an opaque `*mut DfPlan` (an
//! `Arc<DataFusion DataFrame>`), builds a new plan node, and returns a
//! fresh `*mut DfPlan`. `df_plan_collect` materializes the plan and
//! exports the result through the Arrow C Data Interface; the Haskell side
//! consumes those pointers via `arrowToDataframe`.

use std::cell::RefCell;
use std::ffi::{c_char, CStr, CString};
use std::ptr;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StructArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::dataframe::DataFrame as DfDataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::expr_fn::{avg, count, max as agg_max, median, min as agg_min, sum as agg_sum};
use datafusion::logical_expr::{col, lit, BinaryExpr, Expr, JoinType, Operator};
use datafusion::prelude::CsvReadOptions;
use datafusion::scalar::ScalarValue;
use serde_json::Value;
use tokio::runtime::Runtime;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

fn set_error(msg: impl Into<String>) {
    let s = msg.into();
    let cs = CString::new(s).unwrap_or_else(|_| CString::new("dfusion: error contains NUL").unwrap());
    LAST_ERROR.with(|cell| *cell.borrow_mut() = Some(cs));
}

fn clear_error() {
    LAST_ERROR.with(|cell| *cell.borrow_mut() = None);
}

#[no_mangle]
pub extern "C" fn df_last_error() -> *const c_char {
    LAST_ERROR.with(|cell| match &*cell.borrow() {
        Some(cs) => cs.as_ptr(),
        None => ptr::null(),
    })
}

pub struct DfCtx {
    runtime: Runtime,
    session: Arc<SessionContext>,
}

pub struct DfPlan {
    ctx: Arc<DfCtxInner>,
    df: DfDataFrame,
}

// Inner shared state so DfPlans keep the runtime/session alive even if the
// caller frees their DfCtx handle first.
struct DfCtxInner {
    runtime: Runtime,
    session: Arc<SessionContext>,
}

#[no_mangle]
pub extern "C" fn df_ctx_new() -> *mut DfCtx {
    clear_error();
    match Runtime::new() {
        Ok(rt) => {
            let session = Arc::new(SessionContext::new());
            let ctx = Box::new(DfCtx { runtime: rt, session });
            Box::into_raw(ctx)
        }
        Err(e) => {
            set_error(format!("df_ctx_new: failed to start tokio runtime: {e}"));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn df_ctx_free(ctx: *mut DfCtx) {
    if ctx.is_null() { return; }
    unsafe { drop(Box::from_raw(ctx)); }
}

#[no_mangle]
pub extern "C" fn df_plan_free(plan: *mut DfPlan) {
    if plan.is_null() { return; }
    unsafe { drop(Box::from_raw(plan)); }
}

// Wrap the result of a plan-builder operation. On error, sets last_error and
// returns null.
fn wrap_plan(ctx: Arc<DfCtxInner>, result: datafusion::error::Result<DfDataFrame>) -> *mut DfPlan {
    match result {
        Ok(df) => Box::into_raw(Box::new(DfPlan { ctx, df })),
        Err(e) => {
            set_error(format!("{e}"));
            ptr::null_mut()
        }
    }
}

unsafe fn cstr_or_err<'a>(p: *const c_char, what: &str) -> Option<&'a str> {
    if p.is_null() {
        set_error(format!("{what}: null pointer"));
        return None;
    }
    match CStr::from_ptr(p).to_str() {
        Ok(s) => Some(s),
        Err(e) => {
            set_error(format!("{what}: invalid utf-8: {e}"));
            None
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn df_scan_csv(
    ctx: *mut DfCtx,
    path: *const c_char,
    schema_json: *const c_char,
) -> *mut DfPlan {
    clear_error();
    if ctx.is_null() {
        set_error("df_scan_csv: null context");
        return ptr::null_mut();
    }
    let ctx_ref = &*ctx;
    let inner = Arc::new(DfCtxInner {
        runtime: clone_runtime(),
        session: ctx_ref.session.clone(),
    });
    let path_str = match cstr_or_err(path, "df_scan_csv.path") {
        Some(s) => s.to_owned(),
        None => return ptr::null_mut(),
    };

    // Schema override (optional).
    let opts = CsvReadOptions::new();
    let opts_owned: Option<Schema> = if schema_json.is_null() {
        None
    } else {
        match cstr_or_err(schema_json, "df_scan_csv.schema_json") {
            Some(s) => match parse_schema_json(s) {
                Ok(sch) => Some(sch),
                Err(e) => {
                    set_error(format!("df_scan_csv: bad schema_json: {e}"));
                    return ptr::null_mut();
                }
            },
            None => return ptr::null_mut(),
        }
    };

    let session = ctx_ref.session.clone();
    let result = ctx_ref.runtime.block_on(async {
        let opts = match &opts_owned {
            Some(s) => opts.schema(s),
            None => opts,
        };
        session.read_csv(path_str, opts).await
    });
    wrap_plan(inner, result)
}

// We want each DfPlan to share runtime/session with siblings without copying
// the handle out from the user-owned DfCtx. Easiest path: every plan op
// captures the SessionContext + a shared Tokio runtime by Arc reference.
// Since Tokio's Runtime is not Clone, we keep a single runtime per process
// for plan-builder calls and use the session-bound runtime for execution.
fn clone_runtime() -> Runtime {
    // Plan-builder ops are essentially synchronous; create a small dedicated
    // runtime per plan handle so that spawned blocking work doesn't poison
    // the caller's context. Cheap (<1ms) compared to query execution.
    Runtime::new().expect("tokio runtime")
}

fn parse_schema_json(s: &str) -> Result<Schema, String> {
    #[derive(serde::Deserialize)]
    struct SchemaWire { fields: Vec<(String, String)> }
    let wire: SchemaWire = serde_json::from_str(s).map_err(|e| e.to_string())?;
    let fields: Vec<Field> = wire
        .fields
        .into_iter()
        .map(|(name, ty)| {
            let dt = match ty.as_str() {
                "int" | "int64" => DataType::Int64,
                "int32" => DataType::Int32,
                "double" | "float64" => DataType::Float64,
                "float" | "float32" => DataType::Float32,
                "bool" => DataType::Boolean,
                "text" | "string" | "utf8" => DataType::Utf8,
                other => return Err(format!("unsupported type tag '{other}'")),
            };
            Ok(Field::new(&name, dt, true))
        })
        .collect::<Result<_, String>>()?;
    Ok(Schema::new(fields))
}

#[no_mangle]
pub unsafe extern "C" fn df_plan_filter(
    plan: *mut DfPlan,
    expr_json: *const c_char,
) -> *mut DfPlan {
    clear_error();
    if plan.is_null() {
        set_error("df_plan_filter: null plan");
        return ptr::null_mut();
    }
    let plan_ref = &*plan;
    let expr_str = match cstr_or_err(expr_json, "df_plan_filter.expr_json") {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let val: Value = match serde_json::from_str(expr_str) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("df_plan_filter: invalid json: {e}"));
            return ptr::null_mut();
        }
    };
    let expr = match decode_expr(&val) {
        Ok(e) => e,
        Err(e) => {
            set_error(format!("df_plan_filter: {e}"));
            return ptr::null_mut();
        }
    };
    let result = plan_ref.df.clone().filter(expr);
    wrap_plan(plan_ref.ctx.clone(), result)
}

#[no_mangle]
pub unsafe extern "C" fn df_plan_take(
    plan: *mut DfPlan,
    n: u64,
) -> *mut DfPlan {
    clear_error();
    if plan.is_null() {
        set_error("df_plan_take: null plan");
        return ptr::null_mut();
    }
    let plan_ref = &*plan;
    let result = plan_ref.df.clone().limit(0, Some(n as usize));
    wrap_plan(plan_ref.ctx.clone(), result)
}

#[no_mangle]
pub unsafe extern "C" fn df_plan_select(
    plan: *mut DfPlan,
    names_json: *const c_char,
) -> *mut DfPlan {
    clear_error();
    if plan.is_null() {
        set_error("df_plan_select: null plan");
        return ptr::null_mut();
    }
    let plan_ref = &*plan;
    let s = match cstr_or_err(names_json, "df_plan_select.names_json") {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let names: Vec<String> = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("df_plan_select: invalid json: {e}"));
            return ptr::null_mut();
        }
    };
    let exprs: Vec<Expr> = names.iter().map(|n| col(n)).collect();
    let result = plan_ref.df.clone().select(exprs);
    wrap_plan(plan_ref.ctx.clone(), result)
}

#[no_mangle]
pub unsafe extern "C" fn df_plan_derive(
    plan: *mut DfPlan,
    col_name: *const c_char,
    expr_json: *const c_char,
) -> *mut DfPlan {
    clear_error();
    if plan.is_null() {
        set_error("df_plan_derive: null plan");
        return ptr::null_mut();
    }
    let plan_ref = &*plan;
    let name_str = match cstr_or_err(col_name, "df_plan_derive.col_name") {
        Some(s) => s.to_owned(),
        None => return ptr::null_mut(),
    };
    let expr_str = match cstr_or_err(expr_json, "df_plan_derive.expr_json") {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let val: Value = match serde_json::from_str(expr_str) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("df_plan_derive: invalid json: {e}"));
            return ptr::null_mut();
        }
    };
    let expr = match decode_expr(&val) {
        Ok(e) => e,
        Err(e) => {
            set_error(format!("df_plan_derive: {e}"));
            return ptr::null_mut();
        }
    };
    let result = plan_ref.df.clone().with_column(&name_str, expr);
    wrap_plan(plan_ref.ctx.clone(), result)
}

/// Group-by + aggregate. keys_json is a list of column name strings; aggs_json
/// is a list of `{"name": "alias", "expr": <agg_expr_json>}` objects, where
/// each agg_expr_json is a top-level "agg" node produced by Haskell-side
/// encodeExpr on an `Agg ...` expression.
#[no_mangle]
pub unsafe extern "C" fn df_plan_groupby_aggregate(
    plan: *mut DfPlan,
    keys_json: *const c_char,
    aggs_json: *const c_char,
) -> *mut DfPlan {
    clear_error();
    if plan.is_null() {
        set_error("df_plan_groupby_aggregate: null plan");
        return ptr::null_mut();
    }
    let plan_ref = &*plan;
    let keys_str = match cstr_or_err(keys_json, "df_plan_groupby_aggregate.keys_json") {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let aggs_str = match cstr_or_err(aggs_json, "df_plan_groupby_aggregate.aggs_json") {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let keys: Vec<String> = match serde_json::from_str(keys_str) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("df_plan_groupby_aggregate: keys json: {e}"));
            return ptr::null_mut();
        }
    };
    #[derive(serde::Deserialize)]
    struct AggEntry {
        name: String,
        expr: Value,
    }
    let entries: Vec<AggEntry> = match serde_json::from_str(aggs_str) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("df_plan_groupby_aggregate: aggs json: {e}"));
            return ptr::null_mut();
        }
    };
    let group_exprs: Vec<Expr> = keys.iter().map(|k| col(k)).collect();
    let agg_exprs: Vec<Expr> = match entries
        .iter()
        .map(|e| Ok(decode_expr(&e.expr)?.alias(&e.name)))
        .collect::<Result<Vec<_>, String>>()
    {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("df_plan_groupby_aggregate: {e}"));
            return ptr::null_mut();
        }
    };
    let result = plan_ref.df.clone().aggregate(group_exprs, agg_exprs);
    wrap_plan(plan_ref.ctx.clone(), result)
}

/// Join two plans on a list of (left, right) key columns. `how` is one of
/// "inner", "left", "right", "outer".
#[no_mangle]
pub unsafe extern "C" fn df_plan_join(
    left: *mut DfPlan,
    right: *mut DfPlan,
    how: *const c_char,
    on_json: *const c_char,
) -> *mut DfPlan {
    clear_error();
    if left.is_null() || right.is_null() {
        set_error("df_plan_join: null plan");
        return ptr::null_mut();
    }
    let left_ref = &*left;
    let right_ref = &*right;
    let how_str = match cstr_or_err(how, "df_plan_join.how") {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let on_str = match cstr_or_err(on_json, "df_plan_join.on_json") {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let pairs: Vec<(String, String)> = match serde_json::from_str(on_str) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("df_plan_join: on json: {e}"));
            return ptr::null_mut();
        }
    };
    let join_type = match how_str {
        "inner" => JoinType::Inner,
        "left"  => JoinType::Left,
        "right" => JoinType::Right,
        "outer" | "full_outer" => JoinType::Full,
        other => {
            set_error(format!("df_plan_join: unsupported how '{other}'"));
            return ptr::null_mut();
        }
    };
    let left_keys: Vec<&str> = pairs.iter().map(|(l, _)| l.as_str()).collect();
    let right_keys: Vec<&str> = pairs.iter().map(|(_, r)| r.as_str()).collect();
    // Alias both sides so DataFusion treats them as distinct relations, even
    // when both were loaded with the anonymous "?table?" qualifier.
    let left_aliased = match left_ref.df.clone().alias("l") {
        Ok(d) => d,
        Err(e) => { set_error(format!("df_plan_join: {e}")); return ptr::null_mut(); }
    };
    let right_aliased = match right_ref.df.clone().alias("r") {
        Ok(d) => d,
        Err(e) => { set_error(format!("df_plan_join: {e}")); return ptr::null_mut(); }
    };
    let result = left_aliased.join(
        right_aliased,
        join_type,
        &left_keys,
        &right_keys,
        None,
    );
    wrap_plan(left_ref.ctx.clone(), result)
}

#[no_mangle]
pub unsafe extern "C" fn df_plan_sort_by(
    plan: *mut DfPlan,
    orders_json: *const c_char,
) -> *mut DfPlan {
    clear_error();
    if plan.is_null() {
        set_error("df_plan_sort_by: null plan");
        return ptr::null_mut();
    }
    let plan_ref = &*plan;
    let s = match cstr_or_err(orders_json, "df_plan_sort_by.orders_json") {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    #[derive(serde::Deserialize)]
    struct SortSpec { col: String, asc: bool }
    let specs: Vec<SortSpec> = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(e) => {
            set_error(format!("df_plan_sort_by: invalid json: {e}"));
            return ptr::null_mut();
        }
    };
    let sort_exprs: Vec<datafusion::logical_expr::SortExpr> = specs
        .into_iter()
        .map(|sp| {
            datafusion::logical_expr::SortExpr {
                expr: col(&sp.col),
                asc: sp.asc,
                nulls_first: !sp.asc,
            }
        })
        .collect();
    let result = plan_ref.df.clone().sort(sort_exprs);
    wrap_plan(plan_ref.ctx.clone(), result)
}

fn decode_expr(v: &Value) -> Result<Expr, String> {
    let obj = v.as_object().ok_or("expected expr object")?;
    let node = obj.get("node").and_then(Value::as_str).ok_or("missing 'node'")?;
    match node {
        "col" => {
            let name = obj.get("name").and_then(Value::as_str).ok_or("col: missing name")?;
            Ok(col(name))
        }
        "lit" => {
            let out_type = obj.get("out_type").and_then(Value::as_str).ok_or("lit: missing out_type")?;
            let value = obj.get("value").ok_or("lit: missing value")?;
            decode_literal(out_type, value)
        }
        "binary" => {
            let op = obj.get("op").and_then(Value::as_str).ok_or("binary: missing op")?;
            let lhs = decode_expr(obj.get("lhs").ok_or("binary: missing lhs")?)?;
            let rhs = decode_expr(obj.get("rhs").ok_or("binary: missing rhs")?)?;
            // Wire names match DataFrame.IR.ExprJson.recognizeBinary.
            let operator = match op {
                "eq"   => Operator::Eq,
                "neq"  => Operator::NotEq,
                "lt"   => Operator::Lt,
                "leq"  => Operator::LtEq,
                "gt"   => Operator::Gt,
                "geq"  => Operator::GtEq,
                "and"  => Operator::And,
                "or"   => Operator::Or,
                "add"  => Operator::Plus,
                "sub"  => Operator::Minus,
                "mult" => Operator::Multiply,
                "divide" => Operator::Divide,
                "div"  => Operator::Divide,
                "mod"  => Operator::Modulo,
                other => return Err(format!("unsupported binary op '{other}'")),
            };
            Ok(Expr::BinaryExpr(BinaryExpr::new(Box::new(lhs), operator, Box::new(rhs))))
        }
        "if" => {
            let cond = decode_expr(obj.get("cond").ok_or("if: missing cond")?)?;
            let then_ = decode_expr(obj.get("then").ok_or("if: missing then")?)?;
            let else_ = decode_expr(obj.get("else").ok_or("if: missing else")?)?;
            // CASE WHEN cond THEN then ELSE else END
            Ok(datafusion::logical_expr::case(cond)
                .when(lit(true), then_)
                .otherwise(else_)
                .map_err(|e| e.to_string())?)
        }
        "unary" => {
            let op = obj.get("op").and_then(Value::as_str).ok_or("unary: missing op")?;
            let arg = decode_expr(obj.get("arg").ok_or("unary: missing arg")?)?;
            match op {
                "not" => Ok(!arg),
                "negate" => Ok(-arg),
                "abs" => Ok(datafusion::functions::math::abs().call(vec![arg])),
                "toDouble" => Ok(datafusion::logical_expr::cast(arg, DataType::Float64)),
                other => Err(format!("unsupported unary op '{other}'")),
            }
        }
        "agg" => {
            let name = obj.get("agg").and_then(Value::as_str).ok_or("agg: missing 'agg' name")?;
            let arg = decode_expr(obj.get("arg").ok_or("agg: missing arg")?)?;
            match name {
                "sum"     => Ok(agg_sum(arg)),
                "count"   => Ok(count(arg)),
                "mean" | "avg" => Ok(avg(arg)),
                "min"     => Ok(agg_min(arg)),
                "max"     => Ok(agg_max(arg)),
                "median"  => Ok(median(arg)),
                other     => Err(format!("unsupported aggregation '{other}'")),
            }
        }
        other => Err(format!("unknown expr node '{other}'")),
    }
}

fn decode_literal(out_type: &str, v: &Value) -> Result<Expr, String> {
    let scalar = match out_type {
        "int" | "int64" => ScalarValue::Int64(v.as_i64()),
        "int32" => ScalarValue::Int32(v.as_i64().map(|x| x as i32)),
        "double" | "float64" => ScalarValue::Float64(v.as_f64()),
        "float" | "float32" => ScalarValue::Float32(v.as_f64().map(|x| x as f32)),
        "bool" => ScalarValue::Boolean(v.as_bool()),
        "text" | "string" | "utf8" => ScalarValue::Utf8(v.as_str().map(|s| s.to_owned())),
        other => return Err(format!("lit: unsupported type tag '{other}'")),
    };
    Ok(Expr::Literal(scalar))
}

#[no_mangle]
pub unsafe extern "C" fn df_plan_collect(
    plan: *mut DfPlan,
    schema_out: *mut u64,
    array_out: *mut u64,
) -> i32 {
    clear_error();
    if plan.is_null() || schema_out.is_null() || array_out.is_null() {
        set_error("df_plan_collect: null pointer");
        return -1;
    }
    let plan_ref = &*plan;
    let batches: Result<Vec<RecordBatch>, _> =
        plan_ref.ctx.runtime.block_on(async { plan_ref.df.clone().collect().await });
    let batches = match batches {
        Ok(b) => b,
        Err(e) => {
            set_error(format!("df_plan_collect: {e}"));
            return -1;
        }
    };

    let schema = plan_ref.df.schema().as_arrow().clone();
    let combined: RecordBatch = if batches.is_empty() {
        RecordBatch::new_empty(Arc::new(schema.clone()))
    } else {
        match concat_batches(&Arc::new(schema.clone()), &batches) {
            Ok(b) => b,
            Err(e) => {
                set_error(format!("df_plan_collect: concat: {e}"));
                return -1;
            }
        }
    };

    // Convert to a top-level StructArray so the Arrow C Data Interface export
    // produces a single (schema, array) pair where children == columns.
    let struct_array: StructArray = combined.into();
    let array_ref: ArrayRef = Arc::new(struct_array);
    let array_data = array_ref.to_data();

    let ffi_array = match FFI_ArrowArray::new(&array_data) {
        a => a,
    };
    let ffi_schema = match FFI_ArrowSchema::try_from(array_ref.data_type()) {
        Ok(s) => s,
        Err(e) => {
            set_error(format!("df_plan_collect: schema export: {e}"));
            return -1;
        }
    };

    // Move both onto the heap; Haskell owns them now and is responsible for
    // calling the producer-supplied release callbacks (matches existing
    // arrowToDataframe semantics).
    let ffi_schema_box = Box::new(ffi_schema);
    let ffi_array_box = Box::new(ffi_array);
    let schema_ptr = Box::into_raw(ffi_schema_box) as u64;
    let array_ptr = Box::into_raw(ffi_array_box) as u64;
    *schema_out = schema_ptr;
    *array_out = array_ptr;
    0
}
