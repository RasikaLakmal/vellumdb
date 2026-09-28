use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Probabilistic set membership check that sits in front of an SSTable's
/// linear scan. `might_contain` returning `false` means the key is
/// *definitely* not in the file, no need to open it. Returning `true` means
/// it might be there (or might be a false positive), a real lookup is still
/// required. It never produces a false negative.
///
/// Sizing uses the standard formulas: m = -n*ln(p) / (ln 2)^2 bits for n
/// expected items at false positive rate p, k = (m/n)*ln 2 hash functions.
/// The k hash positions themselves come from two real hashes combined via
/// double hashing (Kirsch-Mitzenmacher), which avoids needing k independent
/// hash functions.
#[derive(Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let expected_items = expected_items.max(1);
        let num_bits = optimal_num_bits(expected_items, false_positive_rate);
        let num_hashes = optimal_num_hashes(num_bits, expected_items);
        let words = num_bits.div_ceil(64);
        BloomFilter { bits: vec![0u64; words], num_bits, num_hashes }
    }

    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_pair(key);
        for i in 0..self.num_hashes {
            let bit = self.bit_index(h1, h2, i as u64);
            self.bits[bit / 64] |= 1 << (bit % 64);
        }
    }

    pub fn might_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(key);
        (0..self.num_hashes).all(|i| {
            let bit = self.bit_index(h1, h2, i as u64);
            self.bits[bit / 64] & (1 << (bit % 64)) != 0
        })
    }

    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        let mut h1 = DefaultHasher::new();
        key.hash(&mut h1);
        let h1 = h1.finish();

        let mut h2 = DefaultHasher::new();
        key.hash(&mut h2);
        0x9E3779B97F4A7C15u64.hash(&mut h2); // odd constant so h2 diverges from h1
        let h2 = h2.finish();

        (h1, h2)
    }

    fn bit_index(&self, h1: u64, h2: u64, i: u64) -> usize {
        (h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits as u64) as usize
    }

    pub fn write(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        writer.write_all(&(self.num_bits as u64).to_le_bytes())?;
        writer.write_all(&(self.num_hashes as u64).to_le_bytes())?;
        for word in &self.bits {
            writer.write_all(&word.to_le_bytes())?;
        }
        writer.flush()
    }

    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);

        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)?;
        let num_bits = u64::from_le_bytes(buf8) as usize;
        reader.read_exact(&mut buf8)?;
        let num_hashes = u64::from_le_bytes(buf8) as usize;

        let words = num_bits.div_ceil(64);
        let mut bits = Vec::with_capacity(words);
        for _ in 0..words {
            reader.read_exact(&mut buf8)?;
            bits.push(u64::from_le_bytes(buf8));
        }

        Ok(BloomFilter { bits, num_bits, num_hashes })
    }
}

fn optimal_num_bits(n: usize, p: f64) -> usize {
    let m = -(n as f64) * p.ln() / std::f64::consts::LN_2.powi(2);
    (m.ceil() as usize).max(64)
}

fn optimal_num_hashes(num_bits: usize, n: usize) -> usize {
    let k = (num_bits as f64 / n as f64) * std::f64::consts::LN_2;
    (k.round() as usize).clamp(1, 32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_produces_a_false_negative() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        let keys: Vec<Vec<u8>> = (0..1000).map(|i| format!("key-{i}").into_bytes()).collect();

        for key in &keys {
            bloom.insert(key);
        }
        for key in &keys {
            assert!(bloom.might_contain(key), "false negative for {key:?}");
        }
    }

    #[test]
    fn false_positive_rate_is_in_the_right_ballpark() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        for i in 0..1000 {
            bloom.insert(format!("inserted-{i}").into_bytes().as_slice());
        }

        let trials = 10_000;
        let false_positives = (0..trials)
            .filter(|i| bloom.might_contain(format!("absent-{i}").into_bytes().as_slice()))
            .count();

        let rate = false_positives as f64 / trials as f64;
        // Configured for 1%, allow generous headroom, this just needs to
        // catch a badly broken implementation, not assert exact statistics.
        assert!(rate < 0.05, "false positive rate {rate} is way above the configured 1% target");
    }

    #[test]
    fn round_trips_through_disk() {
        let path = std::env::temp_dir()
            .join(format!("vellumdb_bloom_test_{}.bloom", std::process::id()));

        let mut bloom = BloomFilter::new(100, 0.01);
        bloom.insert(b"foo");
        bloom.insert(b"bar");
        bloom.write(&path).unwrap();

        let loaded = BloomFilter::load(&path).unwrap();
        assert!(loaded.might_contain(b"foo"));
        assert!(loaded.might_contain(b"bar"));

        std::fs::remove_file(&path).unwrap();
    }
}
