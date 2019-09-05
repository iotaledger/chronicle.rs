#[derive(Debug)]
pub struct Validator<T> {
    pub receiver: crossbeam_channel::Receiver<T>,
}

impl<T> Validator<T> {
    pub fn new(r: crossbeam_channel::Receiver<T>) -> Validator<T> {
        Validator {
            receiver: r,
        }
    }
}

impl<T> Clone for Validator<T> {
    fn clone(&self) -> Self {
        Validator::new(self.receiver.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validator_create() {
        let (s, r) = crossbeam_channel::unbounded();
        let val = Validator::new(r);

        s.send("Hello World").unwrap();

        assert_eq!(val.receiver.recv(), Ok("Hello World"));
    }

    #[test]
    fn test_validator_clone() {
        let (s, r) = crossbeam_channel::unbounded();
        let val = Validator::new(r);
        let val2 = val.clone();

        s.send("Hello").unwrap();
        s.send("World").unwrap();

        assert_eq!(val.receiver.recv(), Ok("Hello"));
        assert_eq!(val2.receiver.recv(), Ok("World"));
    }
}