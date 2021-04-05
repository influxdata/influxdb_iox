use async_trait::async_trait;

use crate::context::Context;

#[async_trait]
pub trait Command {
    /// If the line specifies to run this command, runs the command
    /// and returns Ok(true) for success or Err on error. if the line
    /// does not match this comamnd, returns Ok(false)
    async fn matches(&self, line: &str, ctx: &mut Context) -> Result<bool, String>;
}
