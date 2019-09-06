#[derive(Debug)]
pub struct Subscriber<T> {
    pub tx: crossbeam_channel::Sender<T>,
}

impl<T> Subscriber<T> {
    pub fn new(s: crossbeam_channel::Sender<T>) -> Subscriber<T> {
        Subscriber { tx: s }
    }
}

impl<T> Clone for Subscriber<T> {
    fn clone(&self) -> Self {
        Subscriber::new(self.tx.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscriber_create() {
        let (s, r) = crossbeam_channel::unbounded();
        let sub = Subscriber::new(s);

        sub.tx.send("Hello World").unwrap();

        assert_eq!(r.recv(), Ok("Hello World"));
    }

    #[test]
    fn test_subscriber_clone() {
        let (s, r) = crossbeam_channel::unbounded();
        let sub = Subscriber::new(s);
        let sub2 = sub.clone();

        sub.tx.send("Hello").unwrap();
        sub2.tx.send("World").unwrap();

        assert_eq!(r.recv(), Ok("Hello"));
        assert_eq!(r.recv(), Ok("World"));
    }
}
