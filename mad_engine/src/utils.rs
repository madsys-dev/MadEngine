use std::error::Error;

use crc::{Crc, CRC_32_ISO_HDLC};
use serde::{Deserialize, Serialize};

const WORD_SIZE: u32 = 64;
const WORD_MASK: u32 = 63;

pub struct Hasher {
    ck_sum: Crc<u32>,
}

impl Hasher {
    pub fn new() -> Self {
        Self {
            ck_sum: Crc::<u32>::new(&CRC_32_ISO_HDLC),
        }
    }

    pub fn checksum(&self, data: &[u8]) -> u32 {
        self.ck_sum.checksum(data)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BitMap {
    count: u32,
    words: Vec<u64>,
}

impl BitMap {
    pub fn new(count: u32) -> Self {
        let word_count = (count + WORD_SIZE - 1) / WORD_SIZE;
        let words = vec![0; word_count as usize];
        Self { count, words }
    }

    pub fn get_size(&self) -> u32 {
        self.count
    }

    /// parse an index to specific location
    fn parse_index(index: u32) -> (u32, u32) {
        let word_index = index / WORD_SIZE;
        let word_bit_index = index & WORD_MASK;
        (word_index, word_bit_index)
    }

    /// check index is set or not
    pub fn get(&self, index: u32) -> bool {
        let (word_index, word_bit_index) = Self::parse_index(index);
        if self.words[word_index as usize] >> word_bit_index & 1 == 0 {
            return false;
        }
        true
    }

    /// set one bit
    pub fn set(&mut self, index: u32) -> bool {
        let (word_index, word_bit_index) = Self::parse_index(index);
        self.words[word_index as usize] |= (1 << word_bit_index) as u64;
        true
    }

    /// clear one bit
    pub fn clear(&mut self, index: u32) -> bool {
        let (word_index, word_bit_index) = Self::parse_index(index);
        self.words[word_index as usize] &= !(1 << word_bit_index) as u64;
        true
    }

    /// find first unset bit, return none if none
    ///
    /// note that always less significant first
    pub fn find(&self) -> Option<u32> {
        for (idx, word) in self.words.iter().enumerate() {
            if *word != u64::MAX {
                let bit_idx = word.trailing_ones();
                return Some(idx as u32 * WORD_SIZE + bit_idx);
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn test_hasher() {
        let h = Hasher::new();
        assert_eq!(0xCBF43926, h.checksum(b"123456789"));
        assert_eq!(0x3DCA6FAD, h.checksum(b"this is a hasher test"));
    }
}
