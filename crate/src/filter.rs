//! Serializes DataFusion filter expressions into the JSON tree that
//! JavaScript tables receive.
//!
//! Supported shapes:
//! - `column <op> literal` with `=`, `!=`, `<`, `<=`, `>`, `>=`
//! - `column [NOT] IN (literal, ...)`
//! - `column IS [NOT] NULL`
//! - `AND`, `OR` and `NOT` over supported shapes
//!
//! Literals are tagged so the TypeScript side can rebuild exact JS values:
//! `{"t": "int", "v": "123"}`. Integers travel as strings because JSON numbers
//! lose precision above 2^53.

use datafusion::logical_expr::{BinaryExpr, Expr, Operator, expr::InList};
use datafusion::scalar::ScalarValue;
use serde_json::{Value, json};

pub fn to_json(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And | Operator::Or => {
                let mut args = Vec::new();
                flatten(expr, *op, &mut args)?;
                let name = if *op == Operator::And { "and" } else { "or" };
                Some(json!({ "op": name, "args": args }))
            }
            _ => {
                let op = comparison(op)?;
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(v, _)) => {
                        Some(json!({ "op": op, "column": c.name, "value": literal(v)? }))
                    }
                    (Expr::Literal(v, _), Expr::Column(c)) => {
                        Some(json!({ "op": flip(op), "column": c.name, "value": literal(v)? }))
                    }
                    _ => None,
                }
            }
        },
        Expr::Not(inner) => Some(json!({ "op": "not", "arg": to_json(inner)? })),
        Expr::IsNull(inner) => match inner.as_ref() {
            Expr::Column(c) => Some(json!({ "op": "isNull", "column": c.name })),
            _ => None,
        },
        Expr::IsNotNull(inner) => match inner.as_ref() {
            Expr::Column(c) => Some(json!({ "op": "isNotNull", "column": c.name })),
            _ => None,
        },
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let Expr::Column(c) = expr.as_ref() else {
                return None;
            };
            let values = list
                .iter()
                .map(|item| match item {
                    Expr::Literal(v, _) => literal(v),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            let op = if *negated { "notIn" } else { "in" };
            Some(json!({ "op": op, "column": c.name, "values": values }))
        }
        _ => None,
    }
}

fn flatten(expr: &Expr, op: Operator, out: &mut Vec<Value>) -> Option<()> {
    match expr {
        Expr::BinaryExpr(b) if b.op == op => {
            flatten(&b.left, op, out)?;
            flatten(&b.right, op, out)
        }
        other => {
            out.push(to_json(other)?);
            Some(())
        }
    }
}

fn comparison(op: &Operator) -> Option<&'static str> {
    Some(match op {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        _ => return None,
    })
}

/// Rewrites `literal <op> column` as `column <op'> literal`.
fn flip(op: &str) -> &'static str {
    match op {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        "!=" => "!=",
        _ => "=",
    }
}

fn int<T: ToString>(v: &Option<T>) -> Value {
    match v {
        Some(n) => json!({ "t": "int", "v": n.to_string() }),
        None => json!({ "t": "null" }),
    }
}

fn float(v: Option<f64>) -> Value {
    match v {
        // NaN and infinities are not valid JSON numbers.
        Some(n) if n.is_finite() => json!({ "t": "float", "v": n }),
        Some(n) => json!({ "t": "float", "v": n.to_string() }),
        None => json!({ "t": "null" }),
    }
}

fn timestamp(v: &Option<i64>, unit: &str, tz: &Option<std::sync::Arc<str>>) -> Value {
    match v {
        Some(n) => json!({ "t": "ts", "v": n.to_string(), "unit": unit, "tz": tz.as_deref() }),
        None => json!({ "t": "null" }),
    }
}

fn literal(v: &ScalarValue) -> Option<Value> {
    Some(match v {
        ScalarValue::Null => json!({ "t": "null" }),
        ScalarValue::Boolean(b) => match b {
            Some(b) => json!({ "t": "bool", "v": b }),
            None => json!({ "t": "null" }),
        },
        ScalarValue::Int8(n) => int(n),
        ScalarValue::Int16(n) => int(n),
        ScalarValue::Int32(n) => int(n),
        ScalarValue::Int64(n) => int(n),
        ScalarValue::UInt8(n) => int(n),
        ScalarValue::UInt16(n) => int(n),
        ScalarValue::UInt32(n) => int(n),
        ScalarValue::UInt64(n) => int(n),
        ScalarValue::Float32(n) => float(n.map(f64::from)),
        ScalarValue::Float64(n) => float(*n),
        ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s) => match s {
            Some(s) => json!({ "t": "str", "v": s }),
            None => json!({ "t": "null" }),
        },
        ScalarValue::Date32(n) => match n {
            Some(n) => json!({ "t": "date", "v": (*n as i64 * 86_400_000).to_string() }),
            None => json!({ "t": "null" }),
        },
        ScalarValue::Date64(n) => match n {
            Some(n) => json!({ "t": "date", "v": n.to_string() }),
            None => json!({ "t": "null" }),
        },
        ScalarValue::TimestampSecond(n, tz) => timestamp(n, "s", tz),
        ScalarValue::TimestampMillisecond(n, tz) => timestamp(n, "ms", tz),
        ScalarValue::TimestampMicrosecond(n, tz) => timestamp(n, "us", tz),
        ScalarValue::TimestampNanosecond(n, tz) => timestamp(n, "ns", tz),
        _ => return None,
    })
}
