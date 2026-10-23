//! JSON values with the semantics the gauge state file is defined by.
//!
//! The state file outlives any one build of the evaluator: it sits in the
//! watchdog's state dir between polls, and a deploy swaps the binary under it.
//! Its format is Python's `json.dump` layout, and reading it back applies
//! Python's coercions — the format `scripts/gauge_deltas.py`, the reference
//! evaluator `scripts/gauge_parity.sh` holds this one to, reads and writes. So
//! a baseline written by either reads identically in the other.
//!
//! What that takes, and why `serde_json::Value` cannot do it:
//!   * objects keep their key ORDER — the state file is written in a fixed
//!     order, and a watched gauge's value is copied into it verbatim;
//!   * numbers keep int vs float, and a float prints in Python's shortest-repr
//!     layout (`60.0`, `1e+16`, `1e-05`), not ryu's (`60.0`, `1e16`, `1e-5`);
//!   * strings print with every character outside printable ASCII escaped as
//!     `\uXXXX`;
//!   * comparison and arithmetic follow Python: ints and floats compare
//!     exactly, `true` is 1, and an operation Python refuses (`"a" - 1`,
//!     `null < 2`, iterating a number) is a [`Fault`], which `main` turns into
//!     exit 1 — the same "check failed to run" an uncaught exception gives.
//!
//! Outside it, deliberately: JSON that serde_json rejects and Python accepts
//! (`NaN`/`Infinity` tokens, lone `\ud800` escapes, integers wider than 64
//! bits, nesting deeper than 128 levels, `-0` read as an integer), and
//! Python's parsing of numeric STRINGS in `float()`/`int()`. Neither the
//! engine's `summary()` nor either evaluator's writer produces any of them.

use std::cmp::Ordering;
use std::fmt::{self, Write as _};

use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

/// An operation a value's type does not support: what Python raises
/// `TypeError` or `ValueError` for. The text is for stderr only.
#[derive(Debug)]
pub struct Fault(pub String);

impl Fault {
    pub fn new(msg: impl Into<String>) -> Self {
        Fault(msg.into())
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(String),
    List(Vec<Value>),
    Obj(Obj),
}

/// The `0` a missing number defaults to.
pub static ZERO: Value = Value::Int(0);

/// A JSON object in key order. A repeated key keeps its FIRST position and its
/// LAST value, which is what Python's `json.loads` does with one.
#[derive(Clone, Debug, Default)]
pub struct Obj(Vec<(String, Value)>);

impl Obj {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn insert(&mut self, key: &str, value: Value) {
        match self.0.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = value,
            None => self.0.push((key.to_owned(), value)),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A number as Python does arithmetic on it. `Bool` is kept apart only so it
/// prints as `True`/`False`; it computes as 1 or 0.
#[derive(Clone, Copy, Debug)]
pub enum Num {
    Bool(bool),
    Int(i128),
    Float(f64),
}

/// A [`Num`] with the bool folded into the int it computes as.
enum Wide {
    Int(i128),
    Float(f64),
}

/// Every finite f64 at or beyond this magnitude is outside i128.
const TWO_POW_127: f64 = -(i128::MIN as f64);

impl Num {
    fn widen(self) -> Wide {
        match self {
            Num::Bool(b) => Wide::Int(i128::from(b)),
            Num::Int(i) => Wide::Int(i),
            Num::Float(x) => Wide::Float(x),
        }
    }

    /// Python's ordering: an int and a float compare EXACTLY, not after
    /// rounding the int to a float. `None` only for NaN.
    pub fn compare(self, other: Num) -> Option<Ordering> {
        match (self.widen(), other.widen()) {
            (Wide::Int(a), Wide::Int(b)) => Some(a.cmp(&b)),
            (Wide::Float(a), Wide::Float(b)) => a.partial_cmp(&b),
            (Wide::Int(a), Wide::Float(b)) => int_vs_float(a, b),
            (Wide::Float(a), Wide::Int(b)) => int_vs_float(b, a).map(Ordering::reverse),
        }
    }

    pub fn lt(self, other: Num) -> bool {
        self.compare(other) == Some(Ordering::Less)
    }

    /// `self - other`: int if both are ints (or bools), float otherwise.
    pub fn minus(self, other: Num) -> Num {
        match (self.widen(), other.widen()) {
            (Wide::Int(a), Wide::Int(b)) => Num::Int(a - b),
            _ => Num::Float(self.as_f64() - other.as_f64()),
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            Num::Bool(b) => f64::from(u8::from(b)),
            Num::Int(i) => i as f64,
            Num::Float(x) => x,
        }
    }
}

fn int_vs_float(i: i128, f: f64) -> Option<Ordering> {
    if f.is_nan() {
        return None;
    }
    if f >= TWO_POW_127 {
        return Some(Ordering::Less);
    }
    if f < -TWO_POW_127 {
        return Some(Ordering::Greater);
    }
    let t = f.trunc();
    let fraction = if f > t {
        Ordering::Less
    } else if f < t {
        Ordering::Greater
    } else {
        Ordering::Equal
    };
    Some(i.cmp(&(t as i128)).then(fraction))
}

/// Python's `str()`, which is also what an f-string prints.
impl fmt::Display for Num {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Num::Bool(true) => f.write_str("True"),
            Num::Bool(false) => f.write_str("False"),
            Num::Int(i) => write!(f, "{i}"),
            Num::Float(x) => f.write_str(&float_repr(*x)),
        }
    }
}

/// Python's `repr()` of a float: the shortest digits that round-trip, written
/// positionally between 1e-4 and 1e16 (always with a fractional part, so
/// `60.0`), and as `d.ddde±XX` outside that.
pub fn float_repr(x: f64) -> String {
    if x.is_nan() {
        return "nan".into();
    }
    if x.is_infinite() {
        return if x > 0.0 { "inf" } else { "-inf" }.into();
    }
    // `{:e}` writes the same shortest round-trip digits Python's repr uses,
    // as `[-]d[.ddd]e<exp>`; only the layout around them differs.
    let sci = format!("{x:e}");
    let (mantissa, exp) = sci.split_once('e').expect("{:e} always writes an exponent");
    let exp: i32 = exp.parse().expect("{:e} writes a decimal exponent");
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(m) => ("-", m),
        None => ("", mantissa),
    };
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    // The value is 0.DIGITS x 10^point, the position Python's dtoa reports.
    let point = exp + 1;
    let mut out = String::from(sign);
    if point <= -4 || point > 16 {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        let exp_sign = if exp < 0 { '-' } else { '+' };
        let _ = write!(out, "e{exp_sign}{:02}", exp.unsigned_abs());
    } else if point <= 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', point.unsigned_abs() as usize));
        out.push_str(&digits);
    } else {
        let point = point as usize;
        if point < digits.len() {
            out.push_str(&digits[..point]);
            out.push('.');
            out.push_str(&digits[point..]);
        } else {
            out.push_str(&digits);
            out.extend(std::iter::repeat_n('0', point - digits.len()));
            out.push_str(".0");
        }
    }
    out
}

impl Value {
    /// The value as a number, if Python would do arithmetic on it.
    pub fn num(&self) -> Option<Num> {
        match self {
            Value::Bool(b) => Some(Num::Bool(*b)),
            Value::Int(i) => Some(Num::Int(*i)),
            Value::Float(x) => Some(Num::Float(*x)),
            _ => None,
        }
    }

    /// Python truthiness: false for null, false, 0, 0.0 and anything empty.
    pub fn truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::Float(x) => *x != 0.0,
            Value::Str(s) => !s.is_empty(),
            Value::List(v) => !v.is_empty(),
            Value::Obj(o) => !o.is_empty(),
        }
    }

    /// Python's `needle in self`: an element of a list, a key of an object, a
    /// substring of a string. Anything else is not a container.
    pub fn contains(&self, needle: &str) -> Result<bool, Fault> {
        match self {
            Value::List(v) => Ok(v.iter().any(|x| matches!(x, Value::Str(s) if s == needle))),
            Value::Obj(o) => Ok(o.get(needle).is_some()),
            Value::Str(s) => Ok(s.contains(needle)),
            other => Err(Fault::new(format!("argument of type {} is not iterable", other.kind()))),
        }
    }

    /// Python's `list(self)`: a list's elements, an object's keys, a string's
    /// characters.
    pub fn members(&self) -> Result<Vec<Value>, Fault> {
        match self {
            Value::List(v) => Ok(v.clone()),
            Value::Obj(o) => Ok(o.0.iter().map(|(k, _)| Value::Str(k.clone())).collect()),
            Value::Str(s) => Ok(s.chars().map(|c| Value::Str(c.into())).collect()),
            other => Err(Fault::new(format!("{} object is not iterable", other.kind()))),
        }
    }

    /// Python's `self == other`. Never fails: values of unrelated types are
    /// simply unequal.
    pub fn py_eq(&self, other: &Value) -> bool {
        if let (Some(a), Some(b)) = (self.num(), other.num()) {
            return a.compare(b) == Some(Ordering::Equal);
        }
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Str(a), Value::Str(b)) => a == b,
            (Value::List(a), Value::List(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.py_eq(y))
            }
            (Value::Obj(a), Value::Obj(b)) => {
                a.0.len() == b.0.len()
                    && a.0.iter().all(|(k, v)| b.get(k).is_some_and(|w| v.py_eq(w)))
            }
            _ => false,
        }
    }

    /// Python's `self < other`: numbers by value, strings by code point, lists
    /// lexicographically. Every other pairing is a fault.
    pub fn lt(&self, other: &Value) -> Result<bool, Fault> {
        if let (Some(a), Some(b)) = (self.num(), other.num()) {
            return Ok(a.lt(b));
        }
        match (self, other) {
            (Value::Str(a), Value::Str(b)) => Ok(a < b),
            (Value::List(a), Value::List(b)) => {
                for (x, y) in a.iter().zip(b) {
                    if !x.py_eq(y) {
                        return x.lt(y);
                    }
                }
                Ok(a.len() < b.len())
            }
            (a, b) => Err(Fault::new(format!(
                "'<' not supported between {} and {}",
                a.kind(),
                b.kind()
            ))),
        }
    }

    /// Python's `float(self)`. A string is refused whatever it spells.
    pub fn to_float(&self) -> Result<f64, Fault> {
        match self.num() {
            Some(n) => Ok(n.as_f64()),
            None => Err(Fault::new(format!("float() of {}", self.kind()))),
        }
    }

    /// Python's `int(self)`: truncates a float toward zero. A string is
    /// refused whatever it spells, and so is a float beyond i128.
    pub fn to_int(&self) -> Result<i128, Fault> {
        match self.num().map(Num::widen) {
            Some(Wide::Int(i)) => Ok(i),
            Some(Wide::Float(x)) if x.is_finite() && x.abs() < TWO_POW_127 => Ok(x.trunc() as i128),
            _ => Err(Fault::new(format!("int() of {}", self.kind()))),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::Str(_) => "str",
            Value::List(_) => "list",
            Value::Obj(_) => "object",
        }
    }
}

/// Exactly the bytes Python's `json.dump` writes for this value: `", "` and
/// `": "` separators, keys in order, non-ASCII escaped.
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("null"),
            Value::Bool(b) => f.write_str(if *b { "true" } else { "false" }),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(x) if x.is_nan() => f.write_str("NaN"),
            Value::Float(x) if x.is_infinite() => {
                f.write_str(if *x > 0.0 { "Infinity" } else { "-Infinity" })
            }
            Value::Float(x) => f.write_str(&float_repr(*x)),
            Value::Str(s) => write_str(f, s),
            Value::List(v) => {
                f.write_str("[")?;
                for (i, x) in v.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{x}")?;
                }
                f.write_str("]")
            }
            Value::Obj(o) => {
                f.write_str("{")?;
                for (i, (k, v)) in o.0.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write_str(f, k)?;
                    write!(f, ": {v}")?;
                }
                f.write_str("}")
            }
        }
    }
}

fn write_str(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
    f.write_str("\"")?;
    for c in s.chars() {
        match c {
            '"' => f.write_str("\\\"")?,
            '\\' => f.write_str("\\\\")?,
            '\n' => f.write_str("\\n")?,
            '\r' => f.write_str("\\r")?,
            '\t' => f.write_str("\\t")?,
            '\u{8}' => f.write_str("\\b")?,
            '\u{c}' => f.write_str("\\f")?,
            ' '..='~' => f.write_char(c)?,
            _ => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    write!(f, "\\u{unit:04x}")?;
                }
            }
        }
    }
    f.write_str("\"")
}

pub fn parse(text: &str) -> Result<Value, serde_json::Error> {
    serde_json::from_str(text)
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(ValueVisitor)
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_bool<E>(self, b: bool) -> Result<Value, E> {
        Ok(Value::Bool(b))
    }

    fn visit_i64<E>(self, i: i64) -> Result<Value, E> {
        Ok(Value::Int(i.into()))
    }

    fn visit_u64<E>(self, u: u64) -> Result<Value, E> {
        Ok(Value::Int(u.into()))
    }

    fn visit_f64<E>(self, x: f64) -> Result<Value, E> {
        Ok(Value::Float(x))
    }

    fn visit_str<E>(self, s: &str) -> Result<Value, E> {
        Ok(Value::Str(s.to_owned()))
    }

    fn visit_string<E>(self, s: String) -> Result<Value, E> {
        Ok(Value::Str(s))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(Value::List(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut obj = Obj::default();
        while let Some((key, value)) = map.next_entry::<String, Value>()? {
            obj.insert(&key, value);
        }
        Ok(Value::Obj(obj))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floats_print_as_python_repr() {
        // Each expectation is Python 3's repr() of the same double.
        let cases = [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (60.0, "60.0"),
            (3840.0, "3840.0"),
            (491044.1, "491044.1"),
            (0.1, "0.1"),
            (0.5, "0.5"),
            (1e-4, "0.0001"),
            (1e-5, "1e-05"),
            (1.5e-7, "1.5e-07"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (1.2345e16, "1.2345e+16"),
            (123456789012345.6, "123456789012345.6"),
            (1.5e300, "1.5e+300"),
            (5e-324, "5e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
            (2.0f64.powi(70), "1.1805916207174113e+21"),
        ];
        for (x, want) in cases {
            assert_eq!(float_repr(x), want, "repr of {x:e}");
        }
    }

    #[test]
    fn dump_matches_python_json_dump_layout() {
        let v = parse(r#"{"b":1,"a":[1.0,true,null,"é\u0001\"\\\n😀"],"b":2,"o":{}}"#).unwrap();
        assert_eq!(
            v.to_string(),
            r#"{"b": 2, "a": [1.0, true, null, "\u00e9\u0001\"\\\n\ud83d\ude00"], "o": {}}"#
        );
    }

    #[test]
    fn ints_and_floats_compare_exactly() {
        let big = Num::Int(9_007_199_254_740_993); // 2^53 + 1: not a double
        let near = Num::Float(9_007_199_254_740_992.0);
        assert!(near.lt(big), "Python: 9007199254740992.0 < 2**53 + 1");
        assert!(!big.lt(near));
        assert!(Num::Int(2).lt(Num::Float(2.5)));
        assert!(Num::Float(-2.5).lt(Num::Int(-2)));
        assert_eq!(Num::Bool(true).compare(Num::Float(1.0)), Some(Ordering::Equal));
    }

    #[test]
    fn arithmetic_keeps_python_types() {
        assert_eq!(Num::Int(3).minus(Num::Int(1)).to_string(), "2");
        assert_eq!(Num::Float(3.0).minus(Num::Int(1)).to_string(), "2.0");
        assert_eq!(Num::Bool(true).minus(Num::Int(0)).to_string(), "1");
        assert_eq!(Num::Bool(true).to_string(), "True");
    }
}
