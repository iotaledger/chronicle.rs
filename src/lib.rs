// TODO: This is temporary lint, remove it once lib.rs has implementation
#![allow(unused_imports)]

pub mod subscriber;
pub mod validator;
pub mod bundle;

use subscriber::Subscriber;
use validator::Validator;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mpmc() {
        let (s, r) = crossbeam_channel::unbounded();
        let sub = Subscriber::new(s);
        let val = Validator::new(r);
        let sub2 = sub.clone();
        let val2 = val.clone();

        sub.tx.send("Hello World!").unwrap();
        sub2.tx.send("Chronicle start Scribing!").unwrap();

        assert_eq!(val2.rx.recv(), Ok("Hello World!"));
        assert_eq!(val.rx.recv(), Ok("Chronicle start Scribing!"));
    }
}
