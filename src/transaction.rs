use futures::channel::mpsc;

#[derive(Debug)]
pub struct Tvalidator<T> {
    pub rx: crossbeam_channel::Receiver<T>,
    pub tx: mpsc::Sender<T>,
}

impl<T> Tvalidator<T> {
    pub fn new(r: crossbeam_channel::Receiver<T>, t: mpsc::Sender<T>) -> Tvalidator<T> {
        Tvalidator { rx: r, tx: t }
    }
}

impl<T> Clone for Tvalidator<T> {
    fn clone(&self) -> Self {
        Tvalidator::new(self.rx.clone(), self.tx.clone())
    }
}

#[derive(Debug)]
pub struct Tgenerator<T> {
    pub tx: crossbeam_channel::Sender<T>,
}

impl<T> Tgenerator<T> {
    pub fn new(s: crossbeam_channel::Sender<T>) -> Tgenerator<T> {
        Tgenerator { tx: s }
    }
}

impl<T> Clone for Tgenerator<T> {
    fn clone(&self) -> Self {
        Tgenerator::new(self.tx.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let (t, r) = crossbeam_channel::unbounded();
        let (t2, _) = mpsc::channel(1);
        let val = Tvalidator::new(r, t2);

        t.send("Hello World").unwrap();

        assert_eq!(val.rx.recv(), Ok("Hello World"));
    }

    #[test]
    fn test_clone() {
        let (t, r) = crossbeam_channel::unbounded();
        let (t2, _) = mpsc::channel(1);
        let val = Tvalidator::new(r, t2);
        let val2 = val.clone();

        t.send("Hello").unwrap();
        t.send("World").unwrap();

        assert_eq!(val.rx.recv(), Ok("Hello"));
        assert_eq!(val2.rx.recv(), Ok("World"));
    }

    #[test]
    fn test_tgenerator_create() {
        let (s, r) = crossbeam_channel::unbounded();
        let txg = Tgenerator::new(s);

        txg.tx.send("Hello World").unwrap();

        assert_eq!(r.recv(), Ok("Hello World"));
    }

    #[test]
    fn test_tgenerator_clone() {
        let (s, r) = crossbeam_channel::unbounded();
        let txg = Tgenerator::new(s);
        let txg2 = txg.clone();

        txg.tx.send("Hello").unwrap();
        txg2.tx.send("World").unwrap();

        assert_eq!(r.recv(), Ok("Hello"));
        assert_eq!(r.recv(), Ok("World"));
    }
}
