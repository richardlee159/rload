use rand_distr::{Distribution, Exp};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    time::Duration,
};

pub fn new_exp(duration: Duration, rate: u64) -> Vec<Duration> {
    let dist = Exp::new(rate as f64).unwrap();
    dist.sample_iter(rand::thread_rng())
        .map(Duration::from_secs_f64)
        .scan(Duration::ZERO, |t, iat| {
            *t += iat;
            Some(*t)
        })
        .take_while(move |&t| t < duration)
        .collect()
}

pub fn new_tracefile(path: PathBuf) -> Vec<Duration> {
    let file = File::open(path).unwrap();
    BufReader::new(file)
        .lines()
        .map(|l| Duration::from_micros(l.unwrap().parse::<u64>().unwrap()))
        .collect()
}
