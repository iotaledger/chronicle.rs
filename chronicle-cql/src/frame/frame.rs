
pub trait Frame {
    fn version(&self) -> u8;
    fn flags(&self) -> u8;
    fn stream(&self) -> i16;
    fn opcode(&self) -> u8;
    fn length(&self) -> usize;
    fn body(&self) -> &[u8];
}

impl Frame for Vec<u8> {
    fn version(&self) -> u8 {
        self[0]
    }
    fn flags(&self) -> u8 {
        self[1]
    }
    fn stream(&self) -> i16 {
        todo!()
    }
    fn opcode(&self) -> u8 {
        self[4]
    }
    fn length(&self) -> usize {
        todo!()
    }
    fn body(&self) -> &[u8] {
        &self[9..self.length()]
    }
}
