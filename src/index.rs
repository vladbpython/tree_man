pub mod bit;
pub mod field;
pub mod storage;
pub mod text;

use std::sync::Arc;

pub const INDEX_FIELD: &str = "index_field";
pub const INDEX_TEXT: &str = "text";

#[derive(Debug,Clone,PartialEq)]
#[allow(dead_code)]
pub enum CompatibilityAction {
    Check,
    Replace
}

pub type ExtractorFieldValue<T> = Arc<dyn Fn(&T) -> field::FieldValue + Send + Sync>;

pub enum IndexType<T> 
where T: Send + Sync + 'static
{
    
    Field((field::IndexFieldEnum,ExtractorFieldValue<T>)),
    Text(text::TextIndex<T>),
}

impl<T> IndexType<T> 
where T: Send + Sync + 'static
{

    pub fn index_type(&self) -> &str {
        match self {
            Self::Field(_) => INDEX_FIELD,
            Self::Text(_) => INDEX_TEXT,
        }
    }
    
    pub fn as_text(&self) -> Option<&text::TextIndex<T>> {
        match self {
            Self::Text(index) => Some(index),
            _ => None,
        }
    }

    pub fn as_field(&self) -> Option<(&field::IndexFieldEnum,&ExtractorFieldValue<T>)> {
        match self {
            Self::Field((field,extractor)) => Some((field,extractor)),
            _ => None,
        }
    }
    
    pub fn is_text(&self) -> bool {
        matches!(self, Self::Text(_))
    }

    pub fn is_field(&self) -> bool {
        matches!(self, Self::Field(_))
    }

    pub fn is_valid(&self) -> bool {
        match self {
            Self::Text(_) => true,
            Self::Field(_) => true,
        }
    }

}