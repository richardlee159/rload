use std::time::Duration;

pub struct ConstGen {
    rate_per_sec: Vec<u64>,
    now: Duration,
}

impl ConstGen {
    pub fn new(rate_per_sec: Vec<u64>) -> Self {
        Self {
            rate_per_sec,
            now: Duration::ZERO,
        }
    }

    pub fn expected_len(&self) -> usize {
        self.rate_per_sec.iter().sum::<u64>() as _
    }
}

impl Iterator for ConstGen {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        let now_sec = self.now.as_secs() as usize;
        if now_sec < self.rate_per_sec.len() {
            let rate = self.rate_per_sec[now_sec];
            let iat = Duration::from_secs_f64(1.0 / rate as f64);
            self.now += iat;
            Some(self.now)
        } else {
            None
        }
    }
}
