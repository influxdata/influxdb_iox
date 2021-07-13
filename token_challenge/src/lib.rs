//! Tooling to issue tokens that prevent fat-fingers.
//!
//! In case the server can be sure that a user has admin rights (this is important!) and the admin wants to perform a
//! potentially fatal action (like dropping a database), you wanna make sure that the admin is not doing the right
//! action on the wrong target (e.g. deleting prod data when they meant to act on staging). Tokens can be a way to make
//! this fat-finger problem less likely. They are usually emitted via another channel (e.g. while the admin is using the
//! CLI or gRPC interface, they are emitted via logs) and must be manually copy-pasted to perform the action. This way
//! they admin has to be wrong twice (e.g. while looking at the logs and while performing the action) lowering the
//! final disaster probably.
#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod hmac;
pub mod log;
