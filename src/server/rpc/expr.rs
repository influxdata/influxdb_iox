//! Handles translating predicates from gRPC into delorean_storage_interface::Predicates
use delorean_arrow::datafusion::{
    logical_plan::{Expr, Operator},
    scalar::ScalarValue,
};
use delorean_generated_types::{
    node::Comparison as RPCComparison, node::Logical as RPCLogical, node::Value as RPCValue,
    Node as RPCNode, Predicate as RPCPredicate,
};
use delorean_storage::exec::Predicate as StoragePredicate;
//use snafu::{ResultExt, Snafu};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unexpected empty predicate: Node"))]
    EmptyPredicateNode {},

    #[snafu(display("Unexpected empty predicate: Value"))]
    EmptyPredicateValue {},

    #[snafu(display("Regular expression predicates are not supported: {}", regexp))]
    RegExpLiteralNotSupported { regexp: String },

    #[snafu(display("Regular expression predicates are not supported"))]
    RegExpNotSupported {},

    #[snafu(display("Not Regular expression predicates are not supported"))]
    NotRegExpNotSupported {},

    #[snafu(display("StartsWith comparisons not supported"))]
    StartsWithNotSupported {},

    #[snafu(display("Unexpected children for predicate: {:?}", value))]
    UnexpectedChildren { value: RPCValue },

    #[snafu(display("Unknown logical node type: {}", logical))]
    UnknownLogicalNode { logical: i32 },

    #[snafu(display("Unknown comparison node type: {}", comparison))]
    UnknownComparisonNode { comparison: i32 },

    #[snafu(display(
        "Unsupported number of children in binary operator {:?}: {} (must be 2)",
        op,
        num_children
    ))]
    UnsupportedNumberOfChildren { op: Operator, num_children: usize },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// converts the Node (predicate tree) into a datafusion based Expr
/// for evaluation
pub fn convert_predicate(predicate: Option<RPCPredicate>) -> Result<Option<StoragePredicate>> {
    match predicate {
        // no input predicate, is fine
        None => Ok(None),
        Some(predicate) => {
            let RPCPredicate { root } = predicate;
            let expr = convert_node(root)?;
            Ok(Some(StoragePredicate { expr }))
        }
    }
}

// converts a Node from the RPC layer into a datafusion logical expr
fn convert_node(node: Option<RPCNode>) -> Result<Expr> {
    match node {
        None => EmptyPredicateNode {}.fail(),
        Some(node) => {
            let RPCNode { children, value } = node;
            let inputs = children
                .into_iter()
                .map(|child| convert_node(Some(child)))
                .collect::<Result<Vec<_>>>()?;

            match value {
                // I don't really understand what a None value
                // means. So until we know they are needed error on
                // them here instead.
                None => EmptyPredicateValue {}.fail(),
                Some(value) => build_node(value, inputs),
            }
        }
    }
}

// Builds an Expr given the Value and the converted children
fn build_node(value: RPCValue, inputs: Vec<Expr>) -> Result<Expr> {
    // Only logical / comparison ops can have inputs.
    let can_have_children = match &value {
        RPCValue::Logical(_) | RPCValue::Comparison(_) => true,
        _ => false,
    };

    if !can_have_children && !inputs.is_empty() {
        return UnexpectedChildren { value }.fail();
    }

    match value {
        RPCValue::StringValue(s) => Ok(Expr::Literal(ScalarValue::Utf8(Some(s)))),
        RPCValue::BoolValue(b) => Ok(Expr::Literal(ScalarValue::Boolean(Some(b)))),
        RPCValue::IntValue(v) => Ok(Expr::Literal(ScalarValue::Int64(Some(v)))),
        RPCValue::UintValue(v) => Ok(Expr::Literal(ScalarValue::UInt64(Some(v)))),
        RPCValue::FloatValue(f) => Ok(Expr::Literal(ScalarValue::Float64(Some(f)))),
        RPCValue::RegexValue(regexp) => RegExpLiteralNotSupported { regexp }.fail(),
        RPCValue::TagRefValue(tag_name) => Ok(Expr::Column(tag_name)),
        RPCValue::FieldRefValue(field_name) => Ok(Expr::Column(field_name)),
        RPCValue::Logical(logical) => build_logical_node(logical, inputs),
        RPCValue::Comparison(comparison) => build_comparison_node(comparison, inputs),
    }
}

/// Creates an expr from a "Logical" Node
fn build_logical_node(logical: i32, inputs: Vec<Expr>) -> Result<Expr> {
    // This ideally could be a match, but I couldn't find a safe way
    // to match an i32 to RPCLogical except for ths

    if logical == RPCLogical::And as i32 {
        build_binary_expr(Operator::And, inputs)
    } else if logical == RPCLogical::Or as i32 {
        build_binary_expr(Operator::Or, inputs)
    } else {
        UnknownLogicalNode { logical }.fail()
    }
}

/// Creates an expr from a "Comparsion" Node
fn build_comparison_node(comparison: i32, inputs: Vec<Expr>) -> Result<Expr> {
    // again, this would ideally be a match but I couldn't figure out how to
    // match an i32 to the enum values

    if comparison == RPCComparison::Equal as i32 {
        build_binary_expr(Operator::Eq, inputs)
    } else if comparison == RPCComparison::NotEqual as i32 {
        build_binary_expr(Operator::NotEq, inputs)
    } else if comparison == RPCComparison::StartsWith as i32 {
        StartsWithNotSupported {}.fail()
    } else if comparison == RPCComparison::Regex as i32 {
        RegExpNotSupported {}.fail()
    } else if comparison == RPCComparison::NotRegex as i32 {
        NotRegExpNotSupported {}.fail()
    } else if comparison == RPCComparison::Lt as i32 {
        build_binary_expr(Operator::Lt, inputs)
    } else if comparison == RPCComparison::Lte as i32 {
        build_binary_expr(Operator::LtEq, inputs)
    } else if comparison == RPCComparison::Gt as i32 {
        build_binary_expr(Operator::Gt, inputs)
    } else if comparison == RPCComparison::Gte as i32 {
        build_binary_expr(Operator::GtEq, inputs)
    } else {
        UnknownComparisonNode { comparison }.fail()
    }
}

/// Creates a datafusion binary expression with the specified operator
fn build_binary_expr(op: Operator, inputs: Vec<Expr>) -> Result<Expr> {
    // convert input vector to options so we can "take" elements out of it
    let mut inputs = inputs.into_iter().map(Some).collect::<Vec<_>>();

    let num_children = inputs.len();
    match num_children {
        2 => Ok(Expr::BinaryExpr {
            left: Box::new(inputs[0].take().unwrap()),
            op,
            right: Box::new(inputs[1].take().unwrap()),
        }),
        _ => UnsupportedNumberOfChildren { op, num_children }.fail(),
    }
}
