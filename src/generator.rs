use rand_distr::{Distribution, Exp};
use std::time::Duration;

pub struct ConstGen {
    now: Duration,
    duration: Duration,
    iat: Duration,
    expected: usize,
}

impl ConstGen {
    pub fn new(duration: Duration, rate: u64) -> Self {
        Self {
            now: Duration::ZERO,
            duration,
            iat: Duration::from_secs_f64(1.0 / rate as f64),
            expected: (duration.as_secs() * rate) as usize,
        }
    }

    pub fn expected_len(&self) -> usize {
        self.expected
    }
}

impl Iterator for ConstGen {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        if self.now < self.duration {
            self.now += self.iat;
            Some(self.now)
        } else {
            None
        }
    }
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
