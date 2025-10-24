pub fn prime_to_node(p: u32) -> Option<u8> {
    match p {
        2 => Some(0),
        3 => Some(1),
        5 => Some(2),
        7 => Some(3),
        11 => Some(4),
        13 => Some(5),
        17 => Some(6),
        19 => Some(7),
        _ => None,
    }
}

#[allow(dead_code)]
pub fn node_to_prime(n: u8) -> Option<u32> {
    match n {
        0 => Some(2),
        1 => Some(3),
        2 => Some(5),
        3 => Some(7),
        4 => Some(11),
        5 => Some(13),
        6 => Some(17),
        7 => Some(19),
        _ => None,
    }
}
