use proc_macro2::Span;
use std::cmp;
use syn::{spanned::Spanned, LitByteStr};

/// Tracks a piece of data and it's originating `Span`
pub struct AddedSpan<T> {
    value: T,
    span: Span,
}

impl<T> AddedSpan<T> {
    /// Construct an `AddedSpan` from a value that has a span.
    pub fn from<U: Spanned>(value: U, f: impl FnOnce(U) -> T) -> Self {
        let span = value.span();
        let value = f(value);
        AddedSpan { span, value }
    }

    /// Returns an `AddedSpan` of a reference of the value.
    pub fn as_ref(&self) -> AddedSpan<&T> {
        let AddedSpan { span, ref value } = *self;
        AddedSpan { value, span }
    }

    /// Converts the contained value, preserving the span.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> AddedSpan<U> {
        let AddedSpan { span, value } = self;
        let value = f(value);
        AddedSpan { value, span }
    }
}

impl<T: AsRef<[u8]>> AddedSpan<T> {
    /// Converts the value and span into a value suitable for
    /// including in the generated code.
    pub fn to_byte_str(&self) -> LitByteStr {
        LitByteStr::new(self.value.as_ref(), self.span)
    }
}

impl<T> Spanned for AddedSpan<T> {
    fn span(&self) -> Span {
        self.span
    }
}

impl<T: PartialEq> cmp::PartialEq for AddedSpan<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl<T: Eq> cmp::Eq for AddedSpan<T> {}

impl<T> cmp::PartialOrd for AddedSpan<T>
where
    Self: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl<T: Ord> cmp::Ord for AddedSpan<T> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.value.cmp(&other.value)
    }
}
