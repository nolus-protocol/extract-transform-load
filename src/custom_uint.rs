use core::num::ParseIntError;

use thiserror::Error;

macro_rules! define_uint {
    ($(
        $custom_uint:ident(
            $signed:ty,
            $unsigned:ty $(,)?
        )
        $(from [
            $($from_custom_uint:ident),+ $(,)?
        ])?
    ),+ $(,)?) => {
        $(
            #[derive(
                ::core::fmt::Debug,
                ::core::clone::Clone,
                ::core::marker::Copy,
                ::core::cmp::PartialEq,
                ::core::cmp::Eq,
                ::core::cmp::PartialOrd,
                ::core::cmp::Ord,
                ::core::hash::Hash,
            )]
            #[repr(transparent)]
            pub struct $custom_uint($unsigned);

            impl $custom_uint {
                pub const BITS: u32 = <$unsigned>::BITS - 1;

                pub const ZERO: Self = Self(0);

                pub const MIN: Self = Self::ZERO;

                pub const MAX: Self = Self(Self::MAX_UNSIGNED);

                const MAX_UNSIGNED: $unsigned = <$unsigned>::MAX >> 1;

                pub const fn from_signed(value: $signed) -> ::core::option::Option<Self> {
                    if value >= 0 {
                        Some(Self(value as $unsigned))
                    } else {
                        None
                    }
                }

                pub const fn from_unsigned(value: $unsigned) -> ::core::option::Option<Self> {
                    if value <= Self::MAX_UNSIGNED {
                        Some(Self(value))
                    } else {
                        None
                    }
                }

                #[inline]
                pub const fn get(&self) -> $unsigned {
                    self.0
                }

                #[inline]
                pub const fn get_signed(&self) -> $signed {
                    self.0 as $signed
                }
            }

            impl ::core::default::Default for $custom_uint {
                #[inline]
                fn default() -> Self {
                    Self::ZERO
                }
            }

            $($(
                impl ::core::convert::From<$from_custom_uint> for $custom_uint {
                    #[inline]
                    fn from($from_custom_uint(value): $from_custom_uint) -> Self {
                        Self(value.into())
                    }
                }
            )+)?

            impl ::core::str::FromStr for $custom_uint {
                type Err = self::ParseError<{ Self::BITS }>;

                #[inline]
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    ::core::str::FromStr::from_str(s)
                        .map_err(::core::convert::From::from)
                        .and_then(|value| {
                            Self::from_unsigned(value).ok_or(const {
                                self::ParseError::OutOfRangeError(
                                    self::OutOfRangeError {}
                                )
                            })
                    })
                }
            }

            impl ::serde::ser::Serialize for $custom_uint {
                #[inline]
                fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                where
                    S: ::serde::ser::Serializer,
                {
                    self.get().serialize(serializer)
                }
            }

            impl<'de> ::serde::de::Deserialize<'de> for $custom_uint {
                fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
                where
                    D: ::serde::de::Deserializer<'de>,
                {
                    <$unsigned>::deserialize(deserializer).and_then(|value| {
                        Self::from_unsigned(value)
                            .ok_or_else(|| ::serde::de::Error::custom(
                                self::OutOfRangeError::<{ Self::BITS }> {}
                            ))
                    })
                }
            }

            impl<DB> ::sqlx::Type<DB> for $custom_uint
            where
                DB: ::sqlx::Database,
                $signed: ::sqlx::Type<DB>,
            {
                #[inline]
                fn type_info() -> DB::TypeInfo {
                    <$signed>::type_info()
                }
            }

            impl<'q, DB> ::sqlx::Encode<'q, DB> for $custom_uint
            where
                DB: ::sqlx::Database,
                $signed: ::sqlx::Encode<'q, DB>,
            {
                #[inline]
                fn encode_by_ref(
                    &self,
                    buf: &mut DB::ArgumentBuffer<'q>,
                ) -> ::core::result::Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                    self.get_signed().encode_by_ref(buf)
                }
            }

            impl<'r, DB> ::sqlx::Decode<'r, DB> for $custom_uint
            where
                DB: ::sqlx::Database,
                $signed: ::sqlx::Decode<'r, DB>,
            {
                fn decode(
                    value: DB::ValueRef<'r>,
                ) -> ::core::result::Result<Self, ::sqlx::error::BoxDynError> {
                    <$signed>::decode(value).and_then(|value| {
                        Self::from_signed(value).ok_or_else(|| {
                            ::std::boxed::Box::new(const {
                                self::OutOfRangeError::<{ Self::BITS }> {}
                            }) as ::std::boxed::Box<_>
                        })
                    })
                }
            }
        )+
    };
}

define_uint![
    UInt7(i8, u8),
    UInt15(i16, u16) from [UInt7],
    UInt31(i32, u32) from [UInt7, UInt15],
    UInt63(i64, u64) from [UInt7, UInt15, UInt31],
    UInt127(i128, u128) from [UInt7, UInt15, UInt31, UInt63],
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Error)]
#[error("Value is out of allowed range, [0, 2^{BITS})!")]
pub struct OutOfRangeError<const BITS: u32>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{0}")]
pub enum ParseError<const BITS: u32> {
    ParseFromInt(#[from] ParseIntError),
    OutOfRangeError(#[from] OutOfRangeError<BITS>),
}

macro_rules! test_cases {
    ($($test_case:ident ($custom_int:ty, $signed_int:ty)),+ $(,)?) => {
        $(
            #[test]
            fn $test_case() {
                const {
                    assert!(<$custom_int>::BITS + 1 == <$signed_int>::BITS);
                }

                assert_eq!(<$custom_int>::MIN.get(), 0);
                assert_eq!(<$custom_int>::MIN.get_signed(), 0);

                assert_eq!(<$custom_int>::MAX.get(), <$signed_int>::MAX.unsigned_abs());
                assert_eq!(<$custom_int>::MAX.get_signed(), <$signed_int>::MAX);

                assert_eq!(<$custom_int>::from_signed(-1), None);

                assert_eq!(<$custom_int>::from_unsigned(0), Some(<$custom_int>::MIN));
                assert_eq!(<$custom_int>::from_signed(0), Some(<$custom_int>::MIN));

                assert_eq!(
                    <$custom_int>::from_unsigned(<$signed_int>::MAX.unsigned_abs()),
                    Some(<$custom_int>::MAX)
                );
                assert_eq!(<$custom_int>::from_signed(<$signed_int>::MAX), Some(<$custom_int>::MAX));

                assert_eq!(<$custom_int>::from_unsigned(<$signed_int>::MAX.unsigned_abs() + 1), None);
            }
        )+
    };
}

test_cases![
    test_uint7(UInt7, i8),
    test_uint15(UInt15, i16),
    test_uint31(UInt31, i32),
    test_uint63(UInt63, i64),
    test_uint127(UInt127, i128),
];
