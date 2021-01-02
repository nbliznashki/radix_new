use std::collections::VecDeque;

use rayon::prelude::*;
#[derive(Debug)]
pub struct TableToTableMap {
    pub target_partition_size: usize,
    pub number_of_buckets: usize,
    pub bucket_bits: usize,
    pub write_offsets: Vec<VecDeque<usize>>, //(worker_thread_id, bucket_id)
    pub bucket_number_of_elements: Vec<usize>, //bucket_id
}

pub struct TableToTableBinaryMap {
    pub datau8_length: Vec<Vec<usize>>, //(worker_thread_id, bucket_id)
    pub bucket_u8_storage: Vec<usize>,  //bucket_id
}

impl TableToTableMap {
    pub(crate) fn new(
        hash: &Vec<Vec<u64>>,
        number_of_worker_threads: usize,
        bucket_bits: usize,
    ) -> Self {
        //Hash: [0,1,2,3,0,1,2,3]
        //Buckets: 4
        //Workers: 2
        let number_of_buckets = 2usize.pow(bucket_bits as u32);
        let bucket_mask = (number_of_buckets - 1) as u64;
        //Target Partition size: 8+2-1/2=4
        let target_partition_size =
            (hash.len() + number_of_worker_threads - 1) / number_of_worker_threads;

        //elements_per_worker_and_bucket
        //[1,1,1,1]
        //[1,1,1,1]

        let mut elements_per_worker_and_bucket: Vec<_> = hash
            .par_chunks(target_partition_size)
            .map(|hash| {
                let mut v: Vec<usize> = vec![0; 2 * number_of_buckets];
                v.truncate(number_of_buckets);
                hash.iter()
                    .for_each(|hash| hash.iter().for_each(|h| v[(h & bucket_mask) as usize] += 1));
                VecDeque::from(v)
            })
            .collect();

        let mut current_offset_per_worker: Vec<usize> = vec![0; number_of_buckets];

        //TO-DO: Very inefficient from CPU architecture perspective
        //Maybe switch to unsafe?
        elements_per_worker_and_bucket.iter_mut().for_each(|v| {
            v.par_iter_mut()
                .zip_eq(current_offset_per_worker.par_iter_mut())
                .for_each(|(v, cur)| {
                    let tmp = *v;
                    *v = *cur;
                    *cur += tmp;
                })
        });

        let write_offsets = elements_per_worker_and_bucket;
        let bucket_number_of_elements = current_offset_per_worker;
        Self {
            target_partition_size,
            number_of_buckets,
            bucket_bits,
            write_offsets,
            bucket_number_of_elements,
        }
    }
}
