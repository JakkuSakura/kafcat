use anyhow::Error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::Any;
use std::marker::PhantomData;

pub trait Coder {
    fn code(&mut self, msg: Box<dyn Any>) -> Result<Box<dyn Any>, anyhow::Error>;
    fn get_type_name(&self) -> &str { std::any::type_name::<Self>() }
}

struct UnitCoder;
impl Coder for UnitCoder {
    fn code(&mut self, msg: Box<dyn Any>) -> Result<Box<dyn Any>, Error> { Ok(msg) }
}

pub struct JsonEncoder<T: 'static> {
    phantom: PhantomData<&'static T>,
}

impl<T: Serialize + 'static> Coder for JsonEncoder<T> {
    fn code(&mut self, msg: Box<dyn Any>) -> Result<Box<dyn Any>, anyhow::Error> {
        if let Ok(obj) = msg.downcast::<T>() {
            let msg = serde_json::to_vec(&obj)?;
            Ok(Box::new(msg))
        } else {
            panic!("JsonEncoder<{}> only supports {}", std::any::type_name::<T>(), std::any::type_name::<T>())
        }
    }
}

pub struct JsonDecoder<T: 'static> {
    phantom: PhantomData<&'static T>,
}

impl<T: DeserializeOwned + 'static> Coder for JsonDecoder<T> {
    fn code(&mut self, msg: Box<dyn Any>) -> Result<Box<dyn Any>, Error> {
        if let Ok(json) = msg.downcast::<Vec<u8>>() {
            let msg: T = serde_json::from_slice(&json)?;
            Ok(Box::new(msg))
        } else {
            panic!("{} only supports Vec<u8>", self.get_type_name())
        }
    }
}
