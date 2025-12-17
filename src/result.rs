use super::errors::{
    GLobalError,
    IndexError,
    IndexFieldError,
};

pub type IndexResult<T> = Result<T,IndexError>;
pub type IndexFieldResult<T> = Result<T,IndexFieldError>;
pub type GlobalResult<T> = Result<T,GLobalError>;