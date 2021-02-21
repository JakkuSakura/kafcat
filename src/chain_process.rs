pub trait Coder {
    type Error: std::error::Error;
    fn code(&mut self, input: Vec<u8>) -> Vec<u8>;
    fn error(&mut self) -> Option<Self::Error>;
}
