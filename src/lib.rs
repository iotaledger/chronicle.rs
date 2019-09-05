// TODO: This is temporary lint, remove it once lib.rs has implementation
#![allow(unused_imports)]

pub mod subscriber;
pub mod validator;

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

        sub.sender.send("Hello World!").unwrap();
        sub2.sender.send("Chronicle start Scribing!").unwrap();

        assert_eq!(val2.receiver.recv(), Ok("Hello World!"));
        assert_eq!(val.receiver.recv(), Ok("Chronicle start Scribing!"));
    }
}
