//! Modified-Signed-Digit radix-4 (digits ∈ {-2,-1,0,1,2})
use rulinalg::vector::Vector;

pub type Digit = i8;
pub struct Msd(Vec<Digit>);

impl Msd {
    pub fn from_int(n: i32) -> Self {
        if n == 0 {
            return Msd(vec![0]);
        }
        let mut out = Vec::with_capacity(8);
        let mut m = n.abs();
        while m != 0 {
            let rem = (m & 3) as i8;
            m >>= 2;
            let digit = if rem > 2 { rem - 4 } else { rem };
            out.push(digit);
            if rem > 2 {
                m += 1;
            }
        }
        if n < 0 {
            out.iter_mut().for_each(|d| *d = -*d);
        }
        Msd(normalize(out))
    }

    #[allow(dead_code)]
    pub fn to_int(&self) -> i32 {
        self.0
            .iter()
            .enumerate()
            .map(|(i, &d)| d as i32 * 4_i32.pow(i as u32))
            .sum()
    }

    #[allow(dead_code)]
    pub fn as_slice(&self) -> &[Digit] {
        &self.0
    }

    pub fn as_vector(&self) -> Vector<Digit> {
        Vector::new(self.0.clone())
    }
}

fn normalize(mut v: Vec<Digit>) -> Vec<Digit> {
    let mut carry = 0i8;
    for d in v.iter_mut() {
        let sum = *d + carry;
        if sum > 2 {
            *d = sum - 4;
            carry = 1;
        } else if sum < -2 {
            *d = sum + 4;
            carry = -1;
        } else {
            *d = sum;
            carry = 0;
        }
    }
    if carry != 0 {
        v.push(carry);
    }
    while v.len() > 1 && v.last() == Some(&0) {
        v.pop();
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_examples() {
        for n in -20..20 {
            let msd = Msd::from_int(n);
            assert_eq!(msd.to_int(), n);
        }
    }
}
