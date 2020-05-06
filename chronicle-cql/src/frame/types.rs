#[repr(u8)]
pub enum Version {
    REQUEST = 0x4,
    RESPONSE = 0x84,
}

#[repr(u8)]
pub enum Flags {
    COMPRESSION = 0x1,
    TRACING = 0x2,
    CUSTOM_PAYLOAD = 0x4,
    WARNING = 0x8,
}

#[repr(u8)]
pub enum Opcode {
    ERROR = 0x0,
    STARTUP = 0x1,
    READY = 0x2,
    AUTHENTICATE = 0x3,
    OPTIONS = 0x5,
    SUPPORTED = 0x6,
    QUERY = 0x7,
    RESULT = 0x8,
    PREPARE = 0x9,
    EXECUTE = 0xA,
    REGISTER = 0xB,
    EVENT = 0xC,
    BATCH = 0xD,
    AUTH_CHALLENGE = 0xE,
    AUTH_RESPONSE = 0xF,
    AUTH_SUCCESS = 0x10,
}

#[repr(u8)]
pub enum QueryFlags {
    VALUES = 0x1,
    SDIP_METADATA = 0x2,
    PAGE_SIZE = 0x4,
    WARNING_PAGING_STATE = 0x8,
    WITH_SERAIL_CONSISTENCY = 0x10,
    WITH_DEFAULT_TIMESTAMP = 0x20,
    WITH_NAMES_FOR_VALUES = 0x40,
}

#[repr(u8)]
pub enum BatchFlags {
    WITH_SERAIL_CONSISTENCY = 0x10,
    WITH_DEFAULT_TIMESTAMP = 0x20,
    WITH_NAMES_FOR_VALUES = 0x40,
}

#[repr(i32)]
pub enum Results {
    VOID = 0x1,
    ROWS = 0x2,
    SET_KEYSPACE = 0x3,
    PREPARED = 0x4,
    SCHEMA_CHANGE = 0x5,
}

#[repr(i32)]
pub enum RowsFlags {
    GLOBAL_TABLES_SPEC = 0x1,
    HAS_MORE_PAGES = 0x2,
    NO_METADATA = 0x4,
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
    UDT = 0x30,
    Tuple = 0x31,
}

pub enum ErrorCodes {
    SERVER_ERROR = 0x0,
    PROTOCOL_ERROR = 0xA,
    AUTHENTICATION_ERROR = 0x100,
    UNAVAILABLE_EXCEPTION = 0x1000,
    OVERLOADED = 0x1001,
    IS_BOOSTRAPPING = 0x1002,
    TRUNCATE_ERROR = 0x1003,
    WRITE_TIMEOUT = 0x1100,
    READ_TIMEOUT = 0x1200,
    READ_FAILURE = 0x1300,
    FUNCTION_FAILURE = 0x1400,
    WRITE_FAILURE = 0x1500,
    SYNTAX_ERROR = 0x2000,
    UNAUTHORIZED = 0x2100,
    INVALID = 0x2200,
    CONFIGURE_ERROR = 0x2300,
    ALREADY_EXISTS = 0x2400,
    UNPREPARED = 0x2500,
}
