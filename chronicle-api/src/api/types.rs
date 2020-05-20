use serde;
use serde::Serialize;
use serde::Deserialize;

pub struct Trytes81([u8;81]);

impl Serialize for Trytes81 {

    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    {
    let str = std::str::from_utf8(&self.0).unwrap();
    serializer.serialize_str(str)
    }
}

impl<'de> Deserialize<'de> for Trytes81 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Trytes81;

            fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                formatter.write_str("a string with 81-bytes length")
            }

            fn visit_str<E>(self, value: &str) -> Result<Trytes81, E>
                where E: ::serde::de::Error,
            {

                if value.len() == 81 {
                    let mut trytes81 = [0;81];
                    trytes81.copy_from_slice(value.as_bytes());
                    Ok(Trytes81(trytes81))
                } else {
                    Err(E::custom(format!("require 81, invalid length: {}", value.len())))
                }
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}
