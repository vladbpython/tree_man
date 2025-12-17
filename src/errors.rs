use std::fmt::Display;

#[derive(Debug,Clone)]
pub enum IndexFieldError {
    ConvertType{
        field_type: String,
        operation: String,
    },
    OperationListEmpty,
    OperationEq{
        field_type: String
    },
    OperationNotEq{
        field_type: String
    },
    OperationGt{
        field_type: String
    },
    OperationGte{
        field_type: String
    },
    OperationLt{
        field_type: String
    },
    OperationLte{
        field_type: String
    },
    OperationIn{
        field_type: String
    },
    OperationNotIn{
        field_type: String
    },
    OperationRange{
        field_type: String
    },
    OperationUndefinedType{
        field_type: String
    }
}

impl Display for IndexFieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConvertType { field_type, operation } => write!(
                f,"can not conver type {field_type} in operation: {operation}"
            ),
            Self::OperationListEmpty => write!(f,"operations list is empty"),
            Self::OperationEq { field_type } => write!(f,"operation failed 'eq' for {field_type}"),
            Self::OperationNotEq { field_type } => write!(f,"operation failed 'not_eq' for {field_type}"),
            Self::OperationGt { field_type } => write!(f,"operation failed 'gt' for {field_type}"),
            Self::OperationGte { field_type } => write!(f,"operation failed 'gte' for {field_type}"),
            Self::OperationLt { field_type } => write!(f,"operation failed 'lt' for {field_type}"),
            Self::OperationLte { field_type } => write!(f,"operation failed 'lte' for {field_type}"),
            Self::OperationIn { field_type } => write!(f,"operation failed 'in' for {field_type}"),
            Self::OperationNotIn { field_type } => write!(f,"operation failed 'not_in' for {field_type}"),
            Self::OperationRange { field_type } => write!(f,"operation failed 'range' for {field_type}"),
            Self::OperationUndefinedType { field_type } => write!(f,"operation failed, undefined for {field_type}")
        }
    }
}

#[derive(Debug,Clone)]
pub enum IndexError {
    Build{
        name: String,
        reason: String,
    },
    Compatibility{
        name:String, 
        type_exist: String,
        type_expect: String,
    },
    Field(IndexFieldError),
    Replace{
        name:String, 
        type_exist: String,
        type_expect: String,
    },
    NotFound{
        name: String,
    },
    NotFoundMany{
        names: Vec<String>,
    }
}

impl Display for IndexError{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Build { name, reason } => write!(f,"can not build index: {name}, reason: {reason}"),
            Self::Compatibility { name, type_exist, type_expect } => write!(
                f,
                "compability error on index name: '{name}', index type existing: '{type_exist}', index type expect: '{type_expect}'"
            ),
            Self::Field(err) => write!(f, "index_field contains error: {err}"),
            Self::Replace { name, type_exist, type_expect } => write!(
                f,
                "cannot replace index name: '{name}': existing type is '{type_exist}', trying to create '{type_expect}'"
            ),
            Self::NotFound { name }   => write!(f,"index with name: {name} not found"),
            Self::NotFoundMany { names } => write!(f,"indexes with names: {} are not found",names.join(",")), 
        }
    }
}

#[derive(Debug,Clone)]
pub enum FieldError {
    PredicatorNotFound{
        field_name: String,
    }
}

impl Display for FieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PredicatorNotFound { field_name } => write!(f,"predicator not found for field: '{field_name}'"),
            
        }
    }
}

#[derive(Debug,Clone)]
pub enum FilterDataError {
    EmptyOperations,
    MaxHistoryExceeded{
        current: usize,
        max: usize
    },
    DataNotFound,
    DataNotFoundByIndex{
        name: String,
    },
    Field(FieldError),
    NotMatchIndexes{
        names: Vec<String>,
    },
    DataNotFoundByIndexCurrent{
        name: String,
    },
    NotMatchIndexesCurrent{
        names: Vec<String>,
    },
    ParentDataIsEmpty,
    WrongSaveDataOwned,
    WrongSaveDataIndexed,
}

impl Display for FilterDataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self{
            Self::EmptyOperations => write!(f,"empty operarions"),
            Self::MaxHistoryExceeded { current, max } => write!(
                f,
                "max history exceeded: current {current} levels, max allowed {max}. Use reset_to_source() to clear history."
            ),
            Self::DataNotFound => write!(f,"data not found"),
            Self::DataNotFoundByIndex { name } => write!(f,"data not found by index: {name}"),
            Self::Field(err) => write!(f,"{err}"),
            Self::NotMatchIndexes { names } => write!(f,"indexes : {} no match", names.join(",")),
            Self::DataNotFoundByIndexCurrent { name } => write!(f,"data not found in current level by index: {name}"),
            Self::NotMatchIndexesCurrent { names } => write!(f,"idexs: {} (no matches in current)",names.join(",")),
            Self::ParentDataIsEmpty => write!(f,"parent data is empty"),
            Self::WrongSaveDataOwned => write!(f,"can not save data owned storage!"),
            Self::WrongSaveDataIndexed => write!(f,"can not save data indexed storage!"),
        }
    }
}

#[derive(Debug,Clone)]
pub enum GLobalError {
    Index(IndexError),
    FilterData(FilterDataError),
    ParentDataIsEmpty,
}

impl Display for GLobalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Index(err) => write!(f, "{err}"),
            Self::FilterData(err) => write!(f,"{err}"),
            Self::ParentDataIsEmpty => write!(f, "parent data is empty"),
        }
    }
}