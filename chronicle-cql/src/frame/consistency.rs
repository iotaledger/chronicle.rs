#[repr(u16)]
pub enum Consistency {
    Any = 0x0,
    One = 0x1,
    Two = 0x2,
    Three = 0x3,
    Quorum = 0x4,
    All = 0x5,
    LocalQuorum = 0x6,
    EachQuorum = 0x7,
    Serial = 0x8,
    LocalSerial = 0x9,
    LocalOne = 0xA,
}
