#[repr(u8)]
pub enum Version {
    Request = 0x4,
    Response = 0x84,
}

#[repr(u8)]
pub enum Flags {
    Compression = 0x1,
    Tracing = 0x2,
    CustomPayload = 0x4,
    Warning = 0x8,
}

#[repr(u8)]
pub enum Opcode {
    Error = 0x0,
    Startup = 0x1,
    Ready = 0x2,
    Authenticate = 0x3,
    Options = 0x5,
    Supported = 0x6,
    Query = 0x7,
    Result = 0x8,
    Prepare = 0x9,
    Execute = 0xA,
    Register = 0xB,
    Event = 0xC,
    Batch = 0xD,
    AuthChallenge = 0xE,
    AuthResponse = 0xF,
    AuthSuccess = 0x10,
}

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

#[repr(u8)]
pub enum QueryFlags {
    Values = 0x1,
    SkipMetadata = 0x2,
    PageSize = 0x4,
    WarningPagingState = 0x8,
    WithSerailConsistency = 0x10,
    WithDefaultTimestamp = 0x20,
    WithNamesForValues = 0x40,
}

#[repr(u8)]
pub enum BatchFlags {
    WithSerailConsistency = 0x10,
    WithDefaultTimestamp = 0x20,
    WithNamesForValues = 0x40,
}

#[repr(i32)]
pub enum Results {
    Void = 0x1,
    Rows = 0x2,
    SetKeyspace = 0x3,
    Prepared = 0x4,
    SchemaChange = 0x5,
}

#[repr(i32)]
pub enum RowsFlags {
    GlobalTablesSpec = 0x1,
    HasMorePages = 0x2,
    NoMetadata = 0x4,
}

#[repr(u16)]
pub enum ColType {
    Custom = 0x0,
    Ascii = 0x1,
    Bigint = 0x2,
    Blob = 0x3,
    Boolean = 0x4,
    Counter = 0x5,
    Decimal = 0x6,
    Double = 0x7,
    Float = 0x8,
    Int = 0x9,
    Timestamp = 0xB,
    Uuid = 0xC,
    Varchar = 0xD,
    Varint = 0xE,
    Timeuuid = 0xF,
    Inet = 0x10,
    Date = 0x11,
    Time = 0x12,
    Smallint = 0x13,
    Tinyint = 0x14,
    List = 0x20,
    Map = 0x21,
    Set = 0x22,
    Udt = 0x30,
    Tuple = 0x31,
}

pub enum ErrorCodes {
    ServerError = 0x0,
    ProtocolError = 0xA,
    AuthenticationError = 0x100,
    UnavailableException = 0x1000,
    Overloaded = 0x1001,
    IsBoostrapping = 0x1002,
    TruncateError = 0x1003,
    WriteTimeout = 0x1100,
    ReadTimeout = 0x1200,
    ReadFailure = 0x1300,
    FunctionFailure = 0x1400,
    WriteFailure = 0x1500,
    SyntaxError = 0x2000,
    Unauthorized = 0x2100,
    Invalid = 0x2200,
    ConfigureError = 0x2300,
    AlreadyExists = 0x2400,
    Unprepared = 0x2500,
}
