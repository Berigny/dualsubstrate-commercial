//! Metatron-star flow-rule engine (S0-only: 8 primes)
//! Digits 0-7 map to states as:
//!  S1: 0=null, 1=electric, 2=magnetic, 3=matter
//!  S2: 4=null, 5=electric, 6=magnetic, 7=matter
//! Centroid C is virtual; even→C→odd enforced.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Node {
    S0,
    S1,
    S2,
    S3,
    S4,
    S5,
    S6,
    S7,
}

impl Node {
    fn index(&self) -> u8 {
        match self {
            Node::S0 => 0,
            Node::S1 => 1,
            Node::S2 => 2,
            Node::S3 => 3,
            Node::S4 => 4,
            Node::S5 => 5,
            Node::S6 => 6,
            Node::S7 => 7,
        }
    }

    fn is_even(&self) -> bool {
        self.index() % 2 == 0
    }
}

/// Whitelisted direct edges (maxims 4,5,6)
fn allowed_direct(src: Node, dst: Node) -> bool {
    use Node::*;
    matches!(
        (src, dst),
        (S1, S2) | (S5, S6) | // work
        (S3, S0) | (S7, S4) | // heat dump
        (S1, S0) // electric dissipation
    )
}

/// Global forbidden set (maxim 7)
fn forbidden_bypass(src: Node, dst: Node) -> bool {
    src.is_even() && !dst.is_even() && !allowed_direct(src, dst)
}

/// Public API: check single transition
pub fn transition_allowed(src: Node, dst: Node) -> bool {
    if src == dst {
        return true; // persistence
    }
    if forbidden_bypass(src, dst) {
        return false;
    }
    allowed_direct(src, dst) || src.is_even() == dst.is_even()
}

/// Batch check (used by ledger hot-path)
pub fn batch_allowed(edges: &[(Node, Node)]) -> Vec<bool> {
    edges
        .iter()
        .map(|(s, d)| transition_allowed(*s, *d))
        .collect()
}

//--------------------------------------------------
// Optional Python bindings
//--------------------------------------------------
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pyfunction]
fn py_transition_allowed(src: u8, dst: u8) -> PyResult<bool> {
    let src_n = match src {
        0..=7 => unsafe { std::mem::transmute(src) },
        _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("bad src")),
    };
    let dst_n = match dst {
        0..=7 => unsafe { std::mem::transmute(dst) },
        _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("bad dst")),
    };
    Ok(transition_allowed(src_n, dst_n))
}

#[cfg(feature = "python")]
#[pyfunction]
fn py_batch_allowed(edges: Vec<(u8, u8)>) -> PyResult<Vec<bool>> {
    let mut converted = Vec::with_capacity(edges.len());
    for (src, dst) in edges.into_iter() {
        let src_n = match src {
            0..=7 => unsafe { std::mem::transmute(src) },
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("bad src")),
        };
        let dst_n = match dst {
            0..=7 => unsafe { std::mem::transmute(dst) },
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("bad dst")),
        };
        converted.push((src_n, dst_n));
    }
    Ok(batch_allowed(&converted))
}

#[cfg(feature = "python")]
#[pymodule]
fn flow_rule(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_transition_allowed, m)?)?;
    m.add_function(wrap_pyfunction!(py_batch_allowed, m)?)?;
    Ok(())
}

//--------------------------------------------------
// Quick unit tests
//--------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistence_always_ok() {
        for n in [
            Node::S0,
            Node::S1,
            Node::S2,
            Node::S3,
            Node::S4,
            Node::S5,
            Node::S6,
            Node::S7,
        ] {
            assert!(transition_allowed(n, n));
        }
    }

    #[test]
    fn even_to_odd_must_be_whitelisted() {
        assert!(!transition_allowed(Node::S2, Node::S1)); // 2→1 forbidden
        assert!(transition_allowed(Node::S1, Node::S2)); // 1→2 allowed (work)
    }

    #[test]
    fn heat_dumps_ok() {
        assert!(transition_allowed(Node::S3, Node::S0));
        assert!(transition_allowed(Node::S7, Node::S4));
    }
}
