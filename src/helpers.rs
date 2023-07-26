macro_rules! option {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        pub mod option {
            use super::*;

            struct $name(super::$name);

            impl Serialize for $name {
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    super::serialize(&self.0, serializer)
                }
            }

            impl<'de> Deserialize<'de> for $name {
                fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    super::deserialize(deserializer).map($name)
                }
            }

            pub fn serialize<S>(v: &Option<super::$name>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                v.clone().map($name).serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<super::$name>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let opt: Option<$name> = Deserialize::deserialize(deserializer)?;
                Ok(opt.map(|v| v.0))
            }
        }
    };
}


pub mod u64 {
    use ethers::types::U64;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(U64, "Ser/de `Option<U64>` to/from `Nullable(U64)`.");

    pub fn serialize<S: Serializer>(u: &U64, serializer: S) -> Result<S::Ok, S::Error> {
        u.0.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u64; 1] = Deserialize::deserialize(deserializer)?;
        Ok(U64(u))
    }
}

pub mod u256as64 {
    use ethers::types::U256;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(U256, "Ser/de `Option<U256>` to/from `Nullable(U256)`.");

    pub fn serialize<S: Serializer>(u: &U256, serializer: S) -> Result<S::Ok, S::Error> {
        u.as_u64().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let n: u64 = Deserialize::deserialize(deserializer)?;
        Ok(U256::from(n))
    }
}