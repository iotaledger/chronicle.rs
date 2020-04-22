use std::{
    io::{
        Read,
        Result,
    },
    ops::Shl,
};

fn copy_into_array<A, T>(slice: &[T]) -> A
where
    A: Default + AsMut<[T]>,
    T: Copy,
{
    let mut a = A::default();
    <A as AsMut<[T]>>::as_mut(&mut a).copy_from_slice(slice);
    a
}

/// Modified from https://github.com/stusmall/murmur3
///
/// Use the x64 variant of the 128 bit murmur3 to hash some [Read] implementation.
///
/// # Example
/// ```
/// use chronicle_cql::murmur3::murmur3::murmur3_cassandra_x64_128;
/// use std::io::Cursor;
/// let hash_result = murmur3_cassandra_x64_128(
///     &mut Cursor::new("EHUHSJRCMDJSZUQMNLDBSRFC9O9XCI9SMHFWWHNDYOOOWMSOJQHCC9GFUEGECEVVXCSXYTHSRJ9TZ9999"),
///     0,
/// );
/// ```
pub fn murmur3_cassandra_x64_128<T: Read>(source: &mut T, seed: u32) -> Result<i64> {
    const C1: i64 = -8663945395140668459i64; // 0x87c3_7b91_1142_53d5;
    const C2: i64 = 0x4cf5_ad43_2745_937f;
    const C3: i64 = 0x52dc_e729;
    const C4: i64 = 0x3849_5ab5;
    const R1: u32 = 27;
    const R2: u32 = 31;
    const R3: u32 = 33;
    const M: i64 = 5;
    let mut h1: i64 = seed as i64;
    let mut h2: i64 = seed as i64;
    let mut buf = [0; 16];
    let mut processed: usize = 0;
    loop {
        let read = source.read(&mut buf[..])?;
        processed += read;
        if read == 16 {
            let k1 = i64::from_le_bytes(copy_into_array(&buf[0..8]));
            let k2 = i64::from_le_bytes(copy_into_array(&buf[8..]));
            h1 ^= k1.wrapping_mul(C1).rotate_left(R2).wrapping_mul(C2);
            h1 = h1.rotate_left(R1).wrapping_add(h2).wrapping_mul(M).wrapping_add(C3);
            h2 ^= k2.wrapping_mul(C2).rotate_left(R3).wrapping_mul(C1);
            h2 = h2.rotate_left(R2).wrapping_add(h1).wrapping_mul(M).wrapping_add(C4);
        } else if read == 0 {
            h1 ^= processed as i64;
            h2 ^= processed as i64;
            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(h1);
            h1 = fmix64_i64(h1);
            h2 = fmix64_i64(h2);
            h1 = h1.wrapping_add(h2);
            // This is the original output
            // h2 = h2.wrapping_add(h1);
            // let x = ((h2 as i128) << 64) | (h1 as u64 as i128);
            let x = h1 as i64;
            return Ok(x);
        } else {
            let mut k1 = 0;
            let mut k2 = 0;
            if read >= 15 {
                k2 ^= (buf[14] as i8 as i64).shl(48);
            }
            if read >= 14 {
                k2 ^= (buf[13] as i8 as i64).shl(40);
            }
            if read >= 13 {
                k2 ^= (buf[12] as i8 as i64).shl(32);
            }
            if read >= 12 {
                k2 ^= (buf[11] as i8 as i64).shl(24);
            }
            if read >= 11 {
                k2 ^= (buf[10] as i8 as i64).shl(16);
            }
            if read >= 10 {
                k2 ^= (buf[9] as i8 as i64).shl(8);
            }
            if read >= 9 {
                k2 ^= buf[8] as i8 as i64;
                k2 = k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
                h2 ^= k2;
            }
            if read >= 8 {
                k1 ^= (buf[7] as i8 as i64).shl(56);
            }
            if read >= 7 {
                k1 ^= (buf[6] as i8 as i64).shl(48);
            }
            if read >= 6 {
                k1 ^= (buf[5] as i8 as i64).shl(40);
            }
            if read >= 5 {
                k1 ^= (buf[4] as i8 as i64).shl(32);
            }
            if read >= 4 {
                k1 ^= (buf[3] as i8 as i64).shl(24);
            }
            if read >= 3 {
                k1 ^= (buf[2] as i8 as i64).shl(16);
            }
            if read >= 2 {
                k1 ^= (buf[1] as i8 as i64).shl(8);
            }
            if read >= 1 {
                k1 ^= buf[0] as i8 as i64;
            }
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;
        }
    }
}

fn fmix64_i64(k: i64) -> i64 {
    const C1: u64 = 0xff51_afd7_ed55_8ccd;
    const C2: u64 = 0xc4ce_b9fe_1a85_ec53;
    const R: u32 = 33;
    let mut tmp = k as u64;
    tmp ^= tmp >> R;
    tmp = tmp.wrapping_mul(C1);
    tmp ^= tmp >> R;
    tmp = tmp.wrapping_mul(C2);
    tmp ^= tmp >> R;
    tmp as i64
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_tx_murmur3_token_generation() {
        let tx = "EHUHSJRCMDJSZUQMNLDBSRFC9O9XCI9SMHFWWHNDYOOOWMSOJQHCC9GFUEGECEVVXCSXYTHSRJ9TZ9999";
        let mut key = Cursor::new(tx);
        let hash_result = murmur3_cassandra_x64_128(&mut key, 0);
        assert_eq!(hash_result.unwrap(), -7733304998189415164);
    }

    #[test]
    fn test_address_murmur3_token_generation() {
        let addr = "NBBM9QWTLPXDQPISXWRJSMOKJQVHCIYBZTWPPAXJSRNRDWQOJDQNX9BZ9RQVLNVTOJBHKBDPP9NPGPGYAQGFDYOHLA";
        let mut key = Cursor::new(addr);
        let hash_result = murmur3_cassandra_x64_128(&mut key, 0);
        assert_eq!(hash_result.unwrap(), -5381343058315604526);
    }
}
