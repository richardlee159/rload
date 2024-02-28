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
