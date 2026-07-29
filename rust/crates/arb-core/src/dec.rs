//! String-preserving fixed-scale decimal.
//!
//! The tape's parity contract is on STRINGS: `"0.4300"` must round-trip as
//! `"0.4300"`, and arithmetic must reproduce Python `decimal` exact-result
//! semantics for the operations the recorder performs (add for Kalshi
//! delta accumulation, 1-p for NO->YES mapping). Value = mantissa / 10^scale.
//! Feeds send plain fixed-point strings only; scientific notation is parsed
//! but re-formats as plain (a visible shadow-diff, never a silent error).

use std::cmp::Ordering;
use std::fmt;

#[derive(Clone, Copy, Debug)]
pub struct Dec {
    pub mantissa: i128,
    pub scale: u8,
}

#[derive(Debug)]
pub struct ParseDecError(pub String);

impl Dec {
    pub const ZERO: Dec = Dec { mantissa: 0, scale: 0 };

    pub fn parse(s: &str) -> Result<Dec, ParseDecError> {
        let t = s.trim();
        let (body, exp) = match t.find(['e', 'E']) {
            Some(i) => (
                &t[..i],
                t[i + 1..].parse::<i32>().map_err(|_| ParseDecError(s.into()))?,
            ),
            None => (t, 0),
        };
        let (neg, body) = match body.strip_prefix('-') {
            Some(b) => (true, b),
            None => (false, body.strip_prefix('+').unwrap_or(body)),
        };
        let (int_part, frac_part) = match body.split_once('.') {
            Some((i, f)) => (i, f),
            None => (body, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return Err(ParseDecError(s.into()));
        }
        let digits: String = [int_part, frac_part].concat();
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ParseDecError(s.into()));
        }
        let mut mantissa: i128 = digits.parse().map_err(|_| ParseDecError(s.into()))?;
        if neg {
            mantissa = -mantissa;
        }
        // scale = frac digits - exponent, floored at 0 by scaling the mantissa
        let mut scale = frac_part.len() as i32 - exp;
        while scale < 0 {
            mantissa *= 10;
            scale += 1;
        }
        if scale > 38 {
            return Err(ParseDecError(s.into()));
        }
        Ok(Dec { mantissa, scale: scale as u8 })
    }

    fn scaled(&self, to: u8) -> i128 {
        self.mantissa * 10i128.pow((to - self.scale) as u32)
    }

    pub fn cmp_num(&self, other: &Dec) -> Ordering {
        let s = self.scale.max(other.scale);
        self.scaled(s).cmp(&other.scaled(s))
    }

    pub fn is_positive(&self) -> bool {
        self.mantissa > 0
    }

    /// Exact addition: result scale = max(scales) (Python decimal semantics).
    pub fn add(&self, other: &Dec) -> Dec {
        let s = self.scale.max(other.scale);
        Dec { mantissa: self.scaled(s) + other.scaled(s), scale: s }
    }

    /// 1 - self, preserving self's scale ("0.4300" -> "0.5700").
    pub fn one_minus(&self) -> Dec {
        Dec { mantissa: 10i128.pow(self.scale as u32) - self.mantissa, scale: self.scale }
    }
}

impl PartialEq for Dec {
    fn eq(&self, other: &Self) -> bool {
        self.cmp_num(other) == Ordering::Equal
    }
}
impl Eq for Dec {}
impl PartialOrd for Dec {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Dec {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_num(other)
    }
}

impl fmt::Display for Dec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let neg = self.mantissa < 0;
        let abs = self.mantissa.unsigned_abs().to_string();
        let s = self.scale as usize;
        let sign = if neg { "-" } else { "" };
        if s == 0 {
            return write!(f, "{sign}{abs}");
        }
        if abs.len() <= s {
            write!(f, "{sign}0.{}{}", "0".repeat(s - abs.len()), abs)
        } else {
            let (i, fr) = abs.split_at(abs.len() - s);
            write!(f, "{sign}{i}.{fr}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Dec {
        Dec::parse(s).unwrap()
    }

    #[test]
    fn round_trips_preserve_scale() {
        for s in ["0.4300", "697.00", "0.01", "1", "0", "0.0100", "33072.3800", "107.30"] {
            assert_eq!(d(s).to_string(), s, "{s}");
        }
    }

    #[test]
    fn one_minus_preserves_scale() {
        assert_eq!(d("0.4300").one_minus().to_string(), "0.5700");
        assert_eq!(d("0.43").one_minus().to_string(), "0.57");
        assert_eq!(d("0.05").one_minus().to_string(), "0.95");
    }

    #[test]
    fn add_uses_max_scale() {
        assert_eq!(d("697.00").add(&d("12")).to_string(), "709.00");
        assert_eq!(d("697.00").add(&d("-697.00")).to_string(), "0.00");
        assert_eq!(d("0.1").add(&d("0.05")).to_string(), "0.15");
    }

    #[test]
    fn numeric_compare_across_scales() {
        assert_eq!(d("0.43"), d("0.4300"));
        assert!(d("0.4300") < d("0.44"));
        assert!(d("2").is_positive() && !d("0.00").is_positive());
    }
}
