use futures::channel::mpsc;

#[derive(Debug)]
pub struct Validator<T> {
    pub rx: crossbeam_channel::Receiver<T>,
    pub tx: mpsc::Sender<T>,
}

impl<T> Validator<T> {
    pub fn new(r: crossbeam_channel::Receiver<T>, t: mpsc::Sender<T>) -> Validator<T> {
        Validator { rx: r, tx: t }
    }
}

impl<T> Clone for Validator<T> {
    fn clone(&self) -> Self {
        Validator::new(self.rx.clone(), self.tx.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validator_create() {
        let (t, r) = crossbeam_channel::unbounded();
        let (t2, _) = mpsc::channel(1);
        let val = Validator::new(r, t2);

        t.send("Hello World").unwrap();

        assert_eq!(val.rx.recv(), Ok("Hello World"));
    }

    #[test]
    fn test_validator_clone() {
        let (t, r) = crossbeam_channel::unbounded();
        let (t2, _) = mpsc::channel(1);
        let val = Validator::new(r, t2);
        let val2 = val.clone();

        t.send("Hello").unwrap();
        t.send("World").unwrap();

        assert_eq!(val.rx.recv(), Ok("Hello"));
        assert_eq!(val2.rx.recv(), Ok("World"));
    }
}
