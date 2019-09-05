use crossbeam::channel::unbounded;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_channel() {
        // Make a structure to create Sender and Receiver
        let c = Chronicle::new();

        // Send a tx to it, let Sender send to Receiver
        let tx = [0; 2673];
        c.recv(tx.clone());

        // show that Receiver actually get the tx
        assert_eq!(c.queue(), tx);
    }
}
