#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
//     missing_docs, // fixme
    clippy::explicit_iter_loop,
    clippy::use_self
)]

//! Implements the `DataPoint` derive macro

use influxdb2_client_core::*;
use proc_macro::TokenStream;
use quote::quote;
use quote::ToTokens;
use std::collections::{BTreeMap, BTreeSet};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    spanned::Spanned,
    Attribute, Data, DeriveInput, Field, Fields, Ident, LitByteStr, LitStr,
};

mod util;
use util::*;

type Error = syn::Error;
type Result<T, E = Error> = std::result::Result<T, E>;

/// Write an escaped influx value to a Vec
macro_rules! escape {
    ($value:expr, $method:ident) => {{
        let mut v = Vec::new();
        $value
            .$method(&mut v)
            .expect("Must always be able to escape value");
        v
    }};
}

#[proc_macro_derive(DataPoint, attributes(influx))]
pub fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let parsed = match Parsed::from(&input) {
        Ok(parsed) => parsed,
        Err(e) => return e.to_compile_error().into(),
    };
    TokenStream::from(parsed.into_token_stream())
}

/// Holds the intermediate parsed state.
struct Parsed<'a> {
    type_name: &'a Ident,
    measurement_name: LitByteStr,
    tags: BTreeMap<AddedSpan<String>, &'a Field>,
    fields: BTreeMap<AddedSpan<String>, &'a Field>,
    timestamps: BTreeSet<&'a Ident>,
}

impl<'a> Parsed<'a> {
    fn from(input: &'a DeriveInput) -> Result<Parsed<'a>> {
        let type_name = &input.ident;

        let mut type_arguments = Arguments::from(&input.attrs)?;

        // TESTME: with and without `measurement`
        let measurement_arg = type_arguments.drain(Argument::into_measurement)?;
        let measurement_name = measurement_arg
            .map(|a| AddedSpan::from(a.name_token, |t| t.value()))
            .unwrap_or_else(|| AddedSpan::from(type_name, |n| n.to_string()))
            .map(|v| escape!(v, write_measurement_to))
            .to_byte_str();

        // TESTME: promote to an actual error
        assert!(type_arguments.is_empty(), "Had remaining type arguments?");

        let mut tags = BTreeMap::new();
        let mut fields = BTreeMap::new();
        let mut timestamps = BTreeSet::new();

        match &input.data {
            Data::Struct(s) => match &s.fields {
                Fields::Named(fs) => {
                    for f in fs.named.iter() {
                        let mut field_arguments = Arguments::from(&f.attrs)?;

                        let name_arg_or_field = |a: ArgumentValue<LitStr>| {
                            // TESTME: with and without a name
                            a.into_option()
                                .map(|v| AddedSpan::from(v, |v| v.value()))
                                .or_else(|| {
                                    f.ident
                                        .as_ref()
                                        .map(|i| AddedSpan::from(i, |i| i.to_string()))
                                })
                                .ok_or_else(|| Error::new(f.span(), "Needs a name"))
                        };

                        if let Some(a) = field_arguments.drain(Argument::into_tag)? {
                            let name = name_arg_or_field(a.name)?;
                            if let Some(prev) = tags.insert(name, f) {
                                // TESTME
                                return Err(Error::new(prev.span(), "This argument is ignored"));
                            }
                        }

                        if let Some(a) = field_arguments.drain(Argument::into_field)? {
                            let name = name_arg_or_field(a.name)?;
                            if let Some(prev) = fields.insert(name, f) {
                                // TESTME
                                return Err(Error::new(prev.span(), "This argument is ignored"));
                            }
                        }

                        if let Some(_) = field_arguments.drain(Argument::into_timestamp)? {
                            let i = f.ident.as_ref().ok_or_else(|| {
                                Error::new(f.span(), "Must have a name for the timestamp field")
                            })?;
                            if let Some(prev) = timestamps.replace(i) {
                                // TESTME
                                return Err(Error::new(prev.span(), "This argument is ignored"));
                            }
                        }

                        // TESTME: promote to an actual error
                        assert!(type_arguments.is_empty(), "Had remaining field arguments?");
                    }
                }
                // TESTME: promote to an actual error
                _ => unimplemented!("Only know named fields"),
            },
            // TESTME: promote to an actual error
            _ => unimplemented!("Only know how to derive for structs"),
        }

        // TESTME: promote to an actual error
        assert!(!fields.is_empty(), "Must have at least one field");
        // TESTME: promote to an actual error
        assert!(timestamps.len() <= 1, "Must not have multiple timestamps");

        Ok(Parsed {
            type_name,
            measurement_name,
            tags,
            fields,
            timestamps,
        })
    }
}

impl ToTokens for Parsed<'_> {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let type_name = self.type_name;
        let measurement_name_literal = &self.measurement_name;

        let tags = self.tags.iter().map(|(tag_key, field)| {
            let tag_key = tag_key.as_ref().map(|v| {
                let mut v = escape!(v, write_tag_key_to);
                v.insert(0, b',');
                v.push(b'=');
                v
            }).to_byte_str();

            let field_name = &field.ident;

            quote! {
                w.write_all(#tag_key)?;
                influxdb2_client_core::WriteTagValue::write_tag_value_to(&self.#field_name, &mut w)?;
            }
        });

        let fields = self.fields.iter().enumerate().map(|(i, (field_key, field))| {
            let d = if i == 0 { b' ' } else { b',' };

            let field_key = field_key.as_ref().map(|v| {
                let mut v = escape!(v, write_field_key_to);
                v.insert(0, d);
                v.push(b'=');
                v
            }).to_byte_str();

            let field_name = &field.ident;

            quote! {
                w.write_all(#field_key)?;
                influxdb2_client_core::WriteFieldValue::write_field_value_to(&self.#field_name, &mut w)?;
            }
        });

        let timestamps = self.timestamps.iter().map(|field_name| {
            quote! {
                w.write_all(b" ")?;
                influxdb2_client_core::WriteTimestamp::write_timestamp_to(&self.#field_name, &mut w)?;
            }
        });

        tokens.extend(quote! {
            impl influxdb2_client_core::WriteDataPoint for #type_name {
                fn write_data_point_to<W>(&self, mut w: W) -> io::Result<()>
                where
                    W: io::Write
                {
                    w.write_all(#measurement_name_literal)?;
                    #(#tags)*;
                    #(#fields)*;
                    #(#timestamps)*;
                    Ok(())
                }
            }
        });
    }
}

mod kw {
    syn::custom_keyword!(measurement);
    syn::custom_keyword!(tag);
    syn::custom_keyword!(field);
    syn::custom_keyword!(timestamp);
}

struct MeasurementArg {
    _measurement_token: kw::measurement,
    _eq_token: syn::Token![=],
    name_token: LitStr,
}

struct TagArg {
    _tag_token: kw::tag,
    name: ArgumentValue<LitStr>,
}

struct FieldArg {
    _field_token: kw::field,
    name: ArgumentValue<LitStr>,
}

struct TimestampArg {
    _timestamp_token: kw::timestamp,
}

macro_rules! unpack_enum {
    ($($fn_name:ident, $var_name:ident, $struct_name:ident,)*) => {
        $(
            fn $fn_name(self) -> Result<$struct_name, Self> {
                match self {
                    Self::$var_name(m) => Ok(m),
                    _ => Err(self),
                }
            }
        )*
    }
}

enum Argument {
    Measurement(MeasurementArg),
    Tag(TagArg),
    Field(FieldArg),
    Timestamp(TimestampArg),
}

impl Argument {
    unpack_enum! {
        into_measurement, Measurement, MeasurementArg,
        into_tag, Tag, TagArg,
        into_field, Field, FieldArg,
        into_timestamp, Timestamp, TimestampArg,
    }
}

impl Parse for Argument {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(kw::measurement) {
            Ok(Argument::Measurement(MeasurementArg {
                _measurement_token: input.parse::<kw::measurement>()?,
                _eq_token: input.parse()?,
                name_token: input.parse()?,
            }))
        } else if lookahead.peek(kw::tag) {
            Ok(Argument::Tag(TagArg {
                _tag_token: input.parse::<kw::tag>()?,
                name: input.parse()?,
            }))
        } else if lookahead.peek(kw::field) {
            Ok(Argument::Field(FieldArg {
                _field_token: input.parse::<kw::field>()?,
                name: input.parse()?,
            }))
        } else if lookahead.peek(kw::timestamp) {
            Ok(Argument::Timestamp(TimestampArg {
                _timestamp_token: input.parse::<kw::timestamp>()?,
            }))
        } else {
            Err(lookahead.error())
        }
    }
}

enum ArgumentValue<T> {
    None,
    Some { _eq_token: syn::Token![=], value: T },
}

impl<T> ArgumentValue<T> {
    fn into_option(self) -> Option<T> {
        match self {
            ArgumentValue::Some { value, .. } => Some(value),
            ArgumentValue::None => None,
        }
    }
}

impl<T> Parse for ArgumentValue<T>
where
    T: Parse,
{
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(syn::Token![=]) {
            Ok(ArgumentValue::Some {
                _eq_token: input.parse()?,
                value: input.parse()?,
            })
        } else {
            Ok(ArgumentValue::None)
        }
    }
}

struct Arguments(Vec<Argument>);

impl Arguments {
    fn from(attrs: &[Attribute]) -> Result<Self> {
        attrs
            .iter()
            .filter(|a| a.path.get_ident().map_or(false, |i| i == "influx"))
            .map(|a| a.parse_args())
            .collect::<Result<_>>()
            .map(Self)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Removes **all** matching arguments. If there were zero or one
    /// matching arguments, it's returned.
    fn drain<T>(
        &mut self,
        mut f: impl FnMut(Argument) -> Result<T, Argument>,
    ) -> Result<Option<T>> {
        let mut remaining = Vec::new();
        let mut matching = Vec::new();

        for a in std::mem::take(&mut self.0) {
            match f(a) {
                Ok(v) => matching.push(v),
                Err(v) => remaining.push(v),
            }
        }

        self.0 = remaining;

        // TESTME: promote to an actual error
        assert!(matching.len() <= 1, "Had duplicate arguments");
        Ok(matching.pop())
    }
}
