use rust_decimal::Decimal;


///
/// Logical/business data-type for any given csv column.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    UNKNOWN,  // Unable to map short-code to a known value.
    BOOLEAN,  // 1,0 - uses byte.
    BYTE,     // -128 <-> 127.
    CHAR,     // 2-byte single unicode character.
    DATE,     // 8-byte, long, millis-since epoch (time set to 00:00:00.000).
    DATETIME, // 8-byte, long, millis-since epoch.
    DECIMAL,  // 8-byte (rust-decimal).
    INTEGER,  // 4-byte (-2^31 <-> 2^31-1).
    LONG,     // 8-byte (-2^63 <-> 2^63-1).
    SHORT,    // 2-byte (-32,768 <-> 32,767).
    STRING,   // Null-terminated, UTF-8.
    UUID,     // 16-byte (UUID). A colum is added in memory if none is present in source file.
    // PROVIDED("PR") // 0-byte (value calculated on demand from column metadata).
}

pub const TRUE: &str  = "1";
pub const FALSE: &str = "0";

impl DataType {
    pub fn to_str(&self) -> &str {
        self.into()
    }
}


impl From<&str> for DataType {
    fn from(value: &str) -> Self {
        match value {
            "BO" => DataType::BOOLEAN,
            "BY" => DataType::BYTE,
            "CH" => DataType::CHAR,
            "DA" => DataType::DATE,
            "DT" => DataType::DATETIME,
            "DE" => DataType::DECIMAL,
            "IN" => DataType::INTEGER,
            "LO" => DataType::LONG,
            "SH" => DataType::SHORT,
            "ST" => DataType::STRING,
            "ID" => DataType::UUID,
            _    => DataType::UNKNOWN
        }
    }
}

impl From<&DataType> for &str {
    fn from(dt: &DataType) -> Self {
        match dt {
            DataType::UNKNOWN  => "ER",
            DataType::BOOLEAN  => "BO",
            DataType::BYTE     => "BY",
            DataType::CHAR     => "CH",
            DataType::DATE     => "DA",
            DataType::DATETIME => "DT",
            DataType::DECIMAL  => "DE",
            DataType::INTEGER  => "IN",
            DataType::LONG     => "LO",
            DataType::SHORT    => "SH",
            DataType::STRING   => "ST",
            DataType::UUID     => "ID",
        }
    }
}

///
/// Provide a wrapper around the custom Decimal type so we can use it in Lua scripts.
///
#[derive(Clone)]
pub struct LuaDecimal (pub Decimal);

impl rlua::UserData for LuaDecimal {
    fn add_methods<'lua, T: rlua::UserDataMethods<'lua, Self>>(methods: &mut T) {
        // Decimal with Decimal.
        methods.add_meta_method(rlua::MetaMethod::Add, |_, this, other: LuaDecimal| { Ok(LuaDecimal(this.0 + other.0)) });
        methods.add_meta_method(rlua::MetaMethod::Sub, |_, this, other: LuaDecimal| { Ok(LuaDecimal(this.0 - other.0)) });
        methods.add_meta_method(rlua::MetaMethod::Mul, |_, this, other: LuaDecimal| { Ok(LuaDecimal(this.0 * other.0)) });
        methods.add_meta_method(rlua::MetaMethod::Div, |_, this, other: LuaDecimal| { Ok(LuaDecimal(this.0 / other.0)) });
        methods.add_meta_method(rlua::MetaMethod::Lt,  |_, this, other: LuaDecimal| { Ok(this.0 < other.0) });
        methods.add_meta_method(rlua::MetaMethod::Le,  |_, this, other: LuaDecimal| { Ok(this.0 <= other.0) });
        methods.add_meta_method(rlua::MetaMethod::Eq,  |_, this, other: LuaDecimal| { Ok(this.0 == other.0) });
        methods.add_meta_method(rlua::MetaMethod::ToString, |_, this, _: ()| { Ok(this.0.to_string()) });
        // Decimal with other types can go here...
    }
}