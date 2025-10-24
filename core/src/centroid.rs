pub type CentroidDigit = u8; // 0 or 1

pub fn centroid_now(ts_ms: u64) -> CentroidDigit {
    (ts_ms % 2) as u8
}

pub fn flip_digit(d: CentroidDigit) -> CentroidDigit {
    1 - d
}
