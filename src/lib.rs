pub mod allocator;
pub mod errors;
pub mod result;
pub(crate) mod index;
pub mod model;
pub mod filter;
pub mod group;

pub use index::{
    bit::Op,
    field::{
        FieldOperation,
        FieldValue,
    },
};

pub use group::GroupData;
pub use filter::{FilterData};
pub use ordered_float::OrderedFloat;
