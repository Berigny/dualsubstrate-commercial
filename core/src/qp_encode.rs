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
        let q1 = self.psi1.as_vector();
        let q2 = self.psi2.as_vector();
        [
            (q1[0] * norm).round() as i32,
            (q1[1] * norm).round() as i32,
            (q1[2] * norm).round() as i32,
            (q1[3] * norm).round() as i32,
            (q2[0] * norm).round() as i32,
            (q2[1] * norm).round() as i32,
            (q2[2] * norm).round() as i32,
            (q2[3] * norm).round() as i32,
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
