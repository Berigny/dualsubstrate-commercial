#![allow(non_local_definitions)]

mod centroid;
mod msd;
mod registry;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use centroid::CentroidDigit;
use chrono::Utc;
use flow_rule::Node;
use msd::Msd;
use pyo3::prelude::*;
use rocksdb::{ColumnFamilyDescriptor, Options, WriteBatch};
use serde::{Deserialize, Serialize};

fn node_from_u8(n: u8) -> Option<Node> {
    match n {
        0 => Some(Node::S0),
        1 => Some(Node::S1),
        2 => Some(Node::S2),
        3 => Some(Node::S3),
        4 => Some(Node::S4),
        5 => Some(Node::S5),
        6 => Some(Node::S6),
        7 => Some(Node::S7),
        _ => None,
    }
}

#[pyclass]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LedgerEvent {
    #[pyo3(get)]
    pub entity_id: u64,
    #[pyo3(get)]
    pub prime: u32,
    #[pyo3(get)]
    pub msd_digits: Vec<i8>,
    #[pyo3(get)]
    pub via_c: bool,
    #[pyo3(get)]
    pub centroid_digit: CentroidDigit,
    #[pyo3(get)]
    pub timestamp: u64,
}

#[pyclass]
pub struct Ledger {
    db: rocksdb::DB,
    log_path: PathBuf,
}

#[pymethods]
impl Ledger {
    #[new]
    fn py_new(path: String) -> PyResult<Self> {
        Ledger::new(path).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
    }

    #[pyo3(name = "anchor_batch")]
    fn anchor_batch_py(&self, entity: u64, commands: Vec<(u32, u8)>) -> PyResult<Vec<LedgerEvent>> {
        Ledger::anchor_batch(self, entity, &commands)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
    }
}

impl Ledger {
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self, String> {
        let base_path = base_path.as_ref();
        std::fs::create_dir_all(base_path).map_err(|e| e.to_string())?;

        let db_path = base_path.join("db");
        std::fs::create_dir_all(&db_path).map_err(|e| e.to_string())?;

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors = ["default", "factors", "postings"]
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect::<Vec<_>>();

        let db = rocksdb::DB::open_cf_descriptors(&opts, &db_path, cf_descriptors)
            .map_err(|e| e.to_string())?;

        let log_path = base_path.join("event.log");
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| e.to_string())?;

        Ok(Ledger { db, log_path })
    }

    /// high-throughput entry: 10 k ops / call
    pub fn anchor_batch(
        &self,
        entity: u64,
        commands: &[(u32, u8)],
    ) -> Result<Vec<LedgerEvent>, String> {
        let ts = Utc::now().timestamp_millis() as u64;
        let mut base_centroid = centroid::centroid_now(ts);
        let mut events = Vec::with_capacity(commands.len());
        let mut batch = WriteBatch::default();

        let factors_cf = self
            .db
            .cf_handle("factors")
            .ok_or_else(|| "missing column family: factors".to_string())?;
        let postings_cf = self
            .db
            .cf_handle("postings")
            .ok_or_else(|| "missing column family: postings".to_string())?;

        for &(prime, target_node) in commands {
            let src_node = registry::prime_to_node(prime)
                .ok_or_else(|| format!("Prime {} not in S0", prime))?;
            let dst_node = target_node;

            let current = self
                .current_exponent(entity, prime)?
                .unwrap_or(src_node as i32);
            let delta_i32 = (dst_node as i32) - current;
            if delta_i32 == 0 {
                continue; // no-op
            }

            let msd = Msd::from_int(delta_i32);
            let msd_digits = msd.as_vector().data().to_vec();

            let via_c = (src_node % 2 == 0 && dst_node % 2 == 1)
                && !matches!(
                    (src_node, dst_node),
                    (1, 2) | (5, 6) | (3, 0) | (7, 4) | (1, 0)
                );
            let src_node_enum = node_from_u8(src_node)
                .ok_or_else(|| format!("Invalid source node {}", src_node))?;
            let dst_node_enum = node_from_u8(dst_node)
                .ok_or_else(|| format!("Invalid target node {}", dst_node))?;

            let allowed = flow_rule::transition_allowed(src_node_enum, dst_node_enum);
            if !allowed && !via_c {
                return Err(format!("Transition {}→{} forbidden", src_node, dst_node));
            }

            if via_c {
                base_centroid = centroid::flip_digit(base_centroid);
            }

            let evt = LedgerEvent {
                entity_id: entity,
                prime,
                msd_digits: msd_digits.clone(),
                via_c,
                centroid_digit: base_centroid,
                timestamp: ts,
            };

            let mut log = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.log_path)
                .map_err(|e| e.to_string())?;
            writeln!(
                log,
                "{}",
                serde_json::to_string(&evt).map_err(|e| e.to_string())?
            )
            .map_err(|e| e.to_string())?;

            let new_exp = current + delta_i32;
            let f_key = format!("{}:{}", entity, prime);
            batch.put_cf(factors_cf, &f_key, new_exp.to_string().as_bytes());
            let p_key = format!("{}:{}", prime, entity);
            batch.put_cf(postings_cf, &p_key, new_exp.to_string().as_bytes());

            events.push(evt);
        }

        self.db.write(batch).map_err(|e| e.to_string())?;
        Ok(events)
    }

    fn current_exponent(&self, entity: u64, prime: u32) -> Result<Option<i32>, String> {
        let key = format!("{}:{}", entity, prime);
        let cf = self
            .db
            .cf_handle("factors")
            .ok_or_else(|| "missing column family: factors".to_string())?;
        match self.db.get_cf(cf, &key).map_err(|e| e.to_string())? {
            Some(v) => {
                let text = std::str::from_utf8(&v).map_err(|e| e.to_string())?;
                text.parse::<i32>().map(Some).map_err(|e| e.to_string())
            }
            None => Ok(None),
        }
    }
}

#[pyfunction]
fn py_anchor_batch(
    _py: Python,
    ledger: &Ledger,
    entity: u64,
    commands: Vec<(u32, u8)>,
) -> PyResult<Vec<LedgerEvent>> {
    Ledger::anchor_batch(ledger, entity, &commands)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
}

#[pymodule]
fn core(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Ledger>()?;
    m.add_class::<LedgerEvent>()?;
    m.add_function(wrap_pyfunction!(py_anchor_batch, m)?)?;
    Ok(())
}
