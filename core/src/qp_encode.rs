//! Quaternion pack/unpack for 8-prime star
//! Two quaternions Ψ₁, Ψ₂ ←→ 8 exponents [exp₀…exp₇]

use nalgebra::{Quaternion, Vector4};

/// Paired quaternions representing eight prime exponents.
pub struct QpQuat {
    pub psi1: Quaternion<f32>,
    pub psi2: Quaternion<f32>,
}

impl QpQuat {
    /// Pack eight `i32` exponents into two unit quaternions.
    pub fn pack(exponents: &[i32; 8]) -> Self {
        fn build_quaternion(chunk: &[i32]) -> Quaternion<f32> {
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
            q
        }

        let psi1 = build_quaternion(&exponents[0..4]);
        let psi2 = build_quaternion(&exponents[4..8]);
        QpQuat { psi1, psi2 }
    }

    /// Unpack the quaternions back into integer exponents using the stored norm.
    pub fn unpack(&self, norm: f32) -> [i32; 8] {
        let psi1 = &self.psi1;
        let psi2 = &self.psi2;
        [
            (psi1.w * norm).round() as i32,
            (psi1.i * norm).round() as i32,
            (psi1.j * norm).round() as i32,
            (psi1.k * norm).round() as i32,
            (psi2.w * norm).round() as i32,
            (psi2.i * norm).round() as i32,
            (psi2.j * norm).round() as i32,
            (psi2.k * norm).round() as i32,
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

    fn norm_of_exponents(exponents: &[i32; 8]) -> f32 {
        let norm1 = exponents[0..4]
            .iter()
            .map(|&e| (e * e) as f32)
            .sum::<f32>()
            .sqrt();
        let norm2 = exponents[4..8]
            .iter()
            .map(|&e| (e * e) as f32)
            .sum::<f32>()
            .sqrt();
        assert!((norm1 - norm2).abs() < f32::EPSILON);
        norm1
    }

    #[test]
    fn pack_then_unpack_round_trips_integers() {
        // Chunks share the same norm so a single stored scalar can recover values.
        let exponents = [1, -2, 3, -4, -1, 2, -3, 4];
        let qp = QpQuat::pack(&exponents);
        let norm = norm_of_exponents(&exponents);
        let recovered = qp.unpack(norm);
        assert_eq!(recovered, exponents);
    }

    #[test]
    fn rotate_preserves_quaternion_norms() {
        let exponents = [2, 1, -3, 4, -1, 2, -5, 6];
        let mut qp = QpQuat::pack(&exponents);

        let norm_before = qp.psi1.norm_squared() + qp.psi2.norm_squared();
        let rot = Quaternion::new(1.0, 0.5, -0.25, 0.75);
        qp.rotate(rot);
        let norm_after = qp.psi1.norm_squared() + qp.psi2.norm_squared();

        assert!((norm_before - norm_after).abs() < 1e-5);
    }
}
