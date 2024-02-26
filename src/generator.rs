use rand_distr::{Distribution, Exp};
use std::time::Duration;

pub fn new_const(duration: Duration, rate: u64) -> Vec<Duration> {
    let mut starts = Vec::new();
    let mut start = Duration::ZERO;
    while start < duration {
        starts.push(start);
        start += Duration::from_secs_f64(1.0 / rate as f64);
    }
    starts
}

#[allow(unused)]
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
