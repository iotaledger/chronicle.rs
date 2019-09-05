#[derive(Debug)]
pub struct Subscriber<T> {
    pub sender: crossbeam_channel::Sender<T>,
}

impl<T> Subscriber<T> {
    pub fn new(s: crossbeam_channel::Sender<T>) -> Subscriber<T> {
        Subscriber {
            sender: s,
        }
    }
}

impl<T> Clone for Subscriber<T> {
    fn clone(&self) -> Self {
        Subscriber::new(self.sender.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscriber_create() {
        let (s, r) = crossbeam_channel::unbounded();
        let sub = Subscriber::new(s);

        sub.sender.send("Hello World").unwrap();

        assert_eq!(r.recv(), Ok("Hello World"));
    }

    #[test]
    fn test_subscriber_clone() {
        let (s, r) = crossbeam_channel::unbounded();
        let sub = Subscriber::new(s);
        let sub2 = sub.clone();

        sub.sender.send("Hello").unwrap();
        sub2.sender.send("World").unwrap();

        assert_eq!(r.recv(), Ok("Hello"));
        assert_eq!(r.recv(), Ok("World"));
    }
}