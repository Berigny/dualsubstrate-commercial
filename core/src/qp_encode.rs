//! Quaternion pack/unpack for 8-prime star
//! Two quaternions Ψ₁, Ψ₂ ←→ 8 exponents [exp₀…exp₇]

use nalgebra::{Quaternion, Vector4};

/// Paired quaternions representing eight prime exponents.
pub struct QpQuat {
    pub psi1: Quaternion<f32>,
    pub psi2: Quaternion<f32>,
    pub psi1_norm: f32,
    pub psi2_norm: f32,
}

impl QpQuat {
    /// Pack eight `i32` exponents into two unit quaternions.
    pub fn pack(exponents: &[i32; 8]) -> Self {
        fn build_quaternion(chunk: &[i32]) -> (Quaternion<f32>, f32) {
            let v = Vector4::new(
                chunk[0] as f32,
                chunk[1] as f32,
                chunk[2] as f32,
                chunk[3] as f32,
            );
            let mut q = Quaternion::new(v[0], v[1], v[2], v[3]);
            let norm = q.norm();
            if norm > 0.0 {
                q /= norm;
            } else {
                q = Quaternion::identity();
            }
            (q, norm)
        }

        let (psi1, psi1_norm) = build_quaternion(&exponents[0..4]);
        let (psi2, psi2_norm) = build_quaternion(&exponents[4..8]);
        QpQuat {
            psi1,
            psi2,
            psi1_norm,
            psi2_norm,
        }
    }

    /// Unpack the quaternions back into integer exponents using the stored norms.
    pub fn unpack(&self) -> [i32; 8] {
        let psi1 = &self.psi1;
        let psi2 = &self.psi2;
        [
            (psi1.w * self.psi1_norm).round() as i32,
            (psi1.i * self.psi1_norm).round() as i32,
            (psi1.j * self.psi1_norm).round() as i32,
            (psi1.k * self.psi1_norm).round() as i32,
            (psi2.w * self.psi2_norm).round() as i32,
            (psi2.i * self.psi2_norm).round() as i32,
            (psi2.j * self.psi2_norm).round() as i32,
            (psi2.k * self.psi2_norm).round() as i32,
        ]
    }

    /// Rotate both quaternions by `q` using conjugation (`q * Ψ * q⁻¹`).
    pub fn rotate(&mut self, q: Quaternion<f32>) {
        let mut rot = q;
        let norm = rot.norm();
        if norm > 0.0 {
            rot /= norm;
        } else {
            rot = Quaternion::identity();
        }
        let conj = rot.conjugate();
        self.psi1 = rot * self.psi1 * conj;
        self.psi2 = rot * self.psi2 * conj;
    }

    /// Energy proxy counter (PMCCNTR on ARM NEON, RDTSC on x86_64, wall-clock fallback otherwise).
    #[cfg(target_arch = "aarch64")]
    pub fn energy_proxy() -> u64 {
        let val: u64;
        unsafe {
            core::arch::asm!("mrs {0}, pmccntr_el0", out(reg) val);
        }
        val
    }

    #[cfg(all(not(target_arch = "aarch64"), target_arch = "x86_64"))]
    pub fn energy_proxy() -> u64 {
        unsafe { std::arch::x86_64::_rdtsc() }
    }

    #[cfg(all(not(target_arch = "aarch64"), not(target_arch = "x86_64")))]
    pub fn energy_proxy() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::QpQuat;
    use nalgebra::Quaternion;

    fn norms_of_exponents(exponents: &[i32; 8]) -> (f32, f32) {
        let norm_chunk = |chunk: &[i32]| {
            chunk
                .iter()
                .map(|&e| (e * e) as f32)
                .sum::<f32>()
                .sqrt()
        };
        (
            norm_chunk(&exponents[0..4]),
            norm_chunk(&exponents[4..8]),
        )
    }

    #[test]
    fn pack_then_unpack_round_trips_integers() {
        // Chunks share the same norm so a single stored scalar can recover values.
        let exponents = [1, -2, 3, -4, -1, 2, -3, 4];
        let qp = QpQuat::pack(&exponents);
        let recovered = qp.unpack();
        assert_eq!(recovered, exponents);
        let (norm1, norm2) = norms_of_exponents(&exponents);
        assert!((norm1 - qp.psi1_norm).abs() < 1e-6);
        assert!((norm2 - qp.psi2_norm).abs() < 1e-6);
    }

    #[test]
    fn pack_then_unpack_handles_unequal_chunk_norms() {
        let exponents = [7, 0, -1, 2, -3, 5, 11, -13];
        let qp = QpQuat::pack(&exponents);
        // Norms differ between chunks in this example.
        assert!((qp.psi1_norm - 7.3484693).abs() < 1e-5);
        assert!((qp.psi2_norm - 18.0).abs() < 1e-5);
        assert!((qp.psi1_norm - qp.psi2_norm).abs() > 1.0);
        let recovered = qp.unpack();
        assert_eq!(recovered, exponents);
    }

    #[test]
    fn rotate_preserves_quaternion_norms() {
        let exponents = [2, 1, -3, 4, -1, 2, -5, 6];
        let mut qp = QpQuat::pack(&exponents);
        let norm1 = qp.psi1_norm;
        let norm2 = qp.psi2_norm;

        let norm_before = qp.psi1.norm_squared() + qp.psi2.norm_squared();
        let rot = Quaternion::new(1.0, 0.5, -0.25, 0.75);
        qp.rotate(rot);
        let norm_after = qp.psi1.norm_squared() + qp.psi2.norm_squared();

        assert!((norm_before - norm_after).abs() < 1e-5);
        assert!((norm1 - qp.psi1_norm).abs() < f32::EPSILON);
        assert!((norm2 - qp.psi2_norm).abs() < f32::EPSILON);
    }
}
