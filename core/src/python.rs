use nalgebra::{Quaternion, Unit, UnitQuaternion, Vector3};
use pyo3::prelude::*;

use crate::qp_encode::QpQuat;

#[pyfunction]
pub fn py_pack_quaternion(exps: [i32; 8]) -> PyResult<([f32; 4], [f32; 4], f32)> {
    let q = QpQuat::pack(&exps);
    let norm = exps.iter().map(|&x| (x as f32).powi(2)).sum::<f32>().sqrt();
    let QpQuat { psi1, psi2 } = q;
    let q1: [f32; 4] = psi1.coords.into();
    let q2: [f32; 4] = psi2.coords.into();
    Ok((q1, q2, norm))
}

#[pyfunction]
pub fn py_unpack_quaternion(q1: [f32; 4], q2: [f32; 4], norm: f32) -> PyResult<[i32; 8]> {
    let qp = QpQuat {
        psi1: Quaternion::new(q1[0], q1[1], q1[2], q1[3]),
        psi2: Quaternion::new(q2[0], q2[1], q2[2], q2[3]),
    };
    Ok(qp.unpack(norm))
}

#[pyfunction]
pub fn py_rotate_quaternion(
    q1: [f32; 4],
    q2: [f32; 4],
    axis: [f32; 3],
    angle: f32,
) -> PyResult<([f32; 4], [f32; 4])> {
    let axis_vec = Vector3::new(axis[0], axis[1], axis[2]);
    let rotation = if axis_vec.norm_squared() == 0.0 {
        Quaternion::identity()
    } else {
        let unit_axis: Unit<Vector3<f32>> = Unit::new_normalize(axis_vec);
        UnitQuaternion::from_axis_angle(&unit_axis, angle).into_inner()
    };
    let mut qp = QpQuat {
        psi1: Quaternion::new(q1[0], q1[1], q1[2], q1[3]),
        psi2: Quaternion::new(q2[0], q2[1], q2[2], q2[3]),
    };
    qp.rotate(rotation);
    Ok((qp.psi1.coords.into(), qp.psi2.coords.into()))
}

#[pyfunction]
pub fn py_energy_proxy() -> u64 {
    QpQuat::energy_proxy()
}
