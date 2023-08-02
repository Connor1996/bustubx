use crate::catalog::column::DataType;

use super::{boolean::Boolean, integer::Integer, smallint::SmallInt, tinyint::TinyInt};

#[derive(Debug, Clone)]
pub enum Value {
    Boolean(Boolean),
    TinyInt(TinyInt),
    SmallInt(SmallInt),
    Integer(Integer),
}
impl Value {
    pub fn from_bytes(bytes: &[u8], data_type: DataType) -> Self {
        match bytes.len() {
            1 => Self::TinyInt(TinyInt::from_bytes(bytes)),
            2 => Self::SmallInt(SmallInt::from_bytes(bytes)),
            _ => panic!("Not implemented"),
        }
    }

    pub fn from_sqlparser_value(value: &sqlparser::ast::Value, data_type: DataType) -> Self {
        match value {
            sqlparser::ast::Value::Number(v, _) => match data_type {
                DataType::TinyInt => Self::TinyInt(TinyInt::new(v.parse::<i8>().unwrap())),
                DataType::SmallInt => Self::SmallInt(SmallInt::new(v.parse::<i16>().unwrap())),
                DataType::Integer => Self::Integer(Integer::new(v.parse::<i32>().unwrap())),
                _ => panic!("Not implemented"),
            },
            // sqlparser::ast::Value::SingleQuotedString(_) => {}
            sqlparser::ast::Value::Boolean(b) => Value::Boolean(Boolean::new(*b)),
            _ => unreachable!(),
        }
    }

    pub fn compare(&self, other: &Self) -> std::cmp::Ordering {
        match self {
            Self::Boolean(v1) => match other {
                Self::Boolean(v2) => v1.value.cmp(&v2.value),
                _ => panic!("Not implemented"),
            },
            Self::TinyInt(v1) => match other {
                Self::TinyInt(v2) => v1.value.cmp(&v2.value),
                _ => panic!("Not implemented"),
            },
            Self::SmallInt(v1) => match other {
                Self::SmallInt(v2) => v1.value.cmp(&v2.value),
                _ => panic!("Not implemented"),
            },
            Self::Integer(v1) => match other {
                Self::Integer(v2) => v1.value.cmp(&v2.value),
                _ => panic!("Not implemented"),
            },
        }
    }
}
