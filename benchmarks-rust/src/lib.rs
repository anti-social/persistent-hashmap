#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use flate2::bufread::GzDecoder;

    use rand::prelude::*;

    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{self, BufReader};
    use std::io::prelude::*;
    use std::path::PathBuf;

    use test::Bencher;

    const BENCHMARK_FILE: &str = "benchmark_data.txt.gz";

    #[bench]
    fn bench_hash_map_10M_rnd(b: &mut Bencher) -> io::Result<()>{
        bench_hash_map(b, BENCHMARK_FILE, Some(10_000_000), 1_000_000)
    }

    #[bench]
    fn bench_hash_map_20M_rnd(b: &mut Bencher) -> io::Result<()>{
        bench_hash_map(b, BENCHMARK_FILE, Some(20_000_000), 1_000_000)
    }

        #[bench]
    fn bench_hash_map_full_rnd(b: &mut Bencher) -> io::Result<()>{
        bench_hash_map(b, BENCHMARK_FILE, None, 1_000_000)
    }

    fn bench_hash_map(b: &mut Bencher, fname: &str, limit: Option<usize>, lookup_size: usize) -> io::Result<()> {
        let entries = read_weights(fname, limit)?;
        let map = create_map(&entries);
        let lookup_keys = random_lookup_keys(&entries, lookup_size);

        b.iter(|| {
            for key in &lookup_keys {
                test::black_box(map.get(key));
            }
        });

        Ok(())
    }

    fn read_weights(fname: &str, limit: Option<usize>) -> io::Result<Vec<(u32, f32)>> {
        let file = File::open(["..", fname].iter().collect::<PathBuf>())?;
        let mut reader = BufReader::new(GzDecoder::new(BufReader::new(file)));

        let mut num_rows = limit;
        let mut header = String::new();
        reader.read_line(&mut header)?;
        for h in header.split(' ') {
            if h == "#" {
                continue;
            }
            let mut h_parts = h.trim_end().splitn(2, '=');
            let h_key = h_parts.next().unwrap();
            let h_value = h_parts.next().unwrap();
            match h_key {
                "rows" if num_rows.is_none() => {
                    num_rows = Some(h_value.parse().unwrap());
                }
                _ => {
                    continue;
                }
            }
        }
        let num_rows = num_rows.unwrap();

        let mut entries = Vec::with_capacity(num_rows);
        for line in reader.lines().take(num_rows) {
            let line = line.unwrap();
            let mut line_parts = line.trim_end().splitn(2, '=');
            let key = line_parts.next().unwrap();
            let value = line_parts.next().unwrap();
            entries.push((key.parse().unwrap(), value.parse().unwrap()));
        }

        Ok(entries)
    }

    fn create_map(entries: &Vec<(u32, f32)>) -> HashMap<u32, f32> {
        let map = entries.iter().map(|(k, v)| (*k, *v)).collect::<HashMap<u32, f32>>();
        println!("Map size: {}", map.len());
        println!("Map capacity: {}", map.capacity());
        println!("Map load factor: {}", map.len() as f64 / map.capacity() as f64);
        map
    }

    fn random_lookup_keys(entries: &Vec<(u32, f32)>, size: usize) -> Vec<u32> {
        let mut lookup_keys = Vec::with_capacity(size);
        for _ in 0..size {
            let ix = random::<usize>() % size;
            lookup_keys.push(entries.get(ix).unwrap().0);
        }
        println!("Lookup keys size: {}", lookup_keys.len());
        // println!("First lookup keys: {:?} ...", &lookup_keys[..10]);
        lookup_keys
    }
}
