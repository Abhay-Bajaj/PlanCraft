use anyhow::{Result, anyhow};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

pub type Row = HashMap<String, JsonValue>;

pub fn parse_cell(s: &str) -> JsonValue {
    let t = s.trim();

    if t.is_empty() {
        return JsonValue::Null;
    }
    if let Ok(i) = t.parse::<i64>() {
        return JsonValue::from(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return JsonValue::from(f);
    }
    if t.eq_ignore_ascii_case("true") {
        return JsonValue::from(true);
    }
    if t.eq_ignore_ascii_case("false") {
        return JsonValue::from(false);
    }

    JsonValue::from(t.to_string())
}

pub fn cmp_json(lhs: &JsonValue, op: &str, rhs: &JsonValue) -> Result<bool> {
    // numeric compare if both can be numbers
    let ln = lhs.as_f64();
    let rn = rhs.as_f64();

    if ln.is_some() && rn.is_some() {
        let a = ln.unwrap();
        let b = rn.unwrap();
        return Ok(match op {
            ">" => a > b,
            ">=" => a >= b,
            "<" => a < b,
            "<=" => a <= b,
            "==" => (a - b).abs() < f64::EPSILON,
            "!=" => (a - b).abs() >= f64::EPSILON,
            _ => return Err(anyhow!("Unsupported operator: {op}")),
        });
    }

    // fallback string compare
    let a = lhs
        .as_str()
        .map(|x| x.to_string())
        .unwrap_or_else(|| lhs.to_string());
    let b = rhs
        .as_str()
        .map(|x| x.to_string())
        .unwrap_or_else(|| rhs.to_string());

    Ok(match op {
        "==" => a == b,
        "!=" => a != b,
        ">" => a > b,
        ">=" => a >= b,
        "<" => a < b,
        "<=" => a <= b,
        _ => return Err(anyhow!("Unsupported operator: {op}")),
    })
}
