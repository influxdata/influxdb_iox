use std::time::Instant;

use async_trait::async_trait;

use crate::context::Context;

/// Command that runs queries against local copes of remote data that has been
/// feteched This is the default fallback
#[derive(Debug, Default)]
pub struct LocalQuery {}

#[async_trait]
impl crate::command::Command for LocalQuery {
    async fn matches(
        &mut self,
        line: &str,
        context: &mut Context,
    ) -> std::result::Result<bool, String> {
        // In this case, assume the line is always a string

        let now = Instant::now();

        let df = context.ctx.sql(line).map_err(|e| e.to_string())?;
        let results = df.collect().await.map_err(|e| e.to_string())?;

        if results.is_empty() {
            println!(
                "0 rows in set. Query took {} seconds.",
                now.elapsed().as_secs()
            );
        } else {
            arrow_deps::arrow::util::pretty::print_batches(&results).map_err(|e| e.to_string())?;

            let row_count: usize = results.iter().map(|b| b.num_rows()).sum();

            println!(
                "{} rows in set. Query took {} seconds.",
                row_count,
                now.elapsed().as_secs()
            );
        }

        Ok(true)
    }
}
