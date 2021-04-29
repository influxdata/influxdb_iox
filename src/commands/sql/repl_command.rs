use std::convert::TryInto;

use observability_deps::tracing::{debug, warn};

/// Represents the parsed command from the user (which may be over many lines)
#[derive(Debug, PartialEq)]
pub enum ReplCommand {
    Help,
    ShowDatabases,
    UseDatabase { db_name: String },
    SqlCommand { sql: String },
    Exit,
}

impl TryInto<ReplCommand> for String {
    type Error = Self;

    #[allow(clippy::if_same_then_else)]
    fn try_into(self) -> Result<ReplCommand, Self::Error> {
        debug!(%self, "tokenizing to ReplCommand");

        if self.trim().is_empty() {
            return Err("No command specified".to_string());
        }

        // tokenized commands, normalized whitespace but original case
        let raw_commands = self
            .trim()
            // chop off trailing semicolon
            .strip_suffix(";")
            .unwrap_or(&self)
            // tokenize on whitespace
            .split(' ')
            .map(|c| c.trim())
            .filter(|c| !c.is_empty())
            .collect::<Vec<_>>();

        // normalized commands (all lower case)
        let commands = raw_commands
            .iter()
            .map(|c| c.to_ascii_lowercase())
            .collect::<Vec<_>>();

        debug!(?raw_commands, ?commands, "processing tokens");

        if !commands.is_empty() && commands[0] == "help" {
            if commands.len() > 1 {
                let extra_content = commands[1..].join(" ");
                warn!(%extra_content, "ignoring tokens after 'help'");
            }
            Ok(ReplCommand::Help)
        } else if commands.len() == 1 && commands[0] == "exit" {
            Ok(ReplCommand::Exit)
        } else if commands.len() == 1 && commands[0] == "quit" {
            Ok(ReplCommand::Exit)
        } else if commands.len() == 2 && commands[0] == "use" && commands[1] == "database" {
            // USE DATABASE
            Err("name not specified. Usage: USE DATABASE <name>".to_string())
        } else if commands.len() == 3 && commands[0] == "use" && commands[1] == "database" {
            // USE DATABASE <name>
            Ok(ReplCommand::UseDatabase {
                db_name: raw_commands[2].to_string(),
            })
        } else if commands.len() == 2 && commands[0] == "use" {
            // USE <name>
            Ok(ReplCommand::UseDatabase {
                db_name: raw_commands[1].to_string(),
            })
        } else if commands.len() == 2 && commands[0] == "show" && commands[1] == "databases" {
            Ok(ReplCommand::ShowDatabases)
        } else {
            // Default is to treat the entire string like SQL
            Ok(ReplCommand::SqlCommand { sql: self })
        }
    }
}

impl ReplCommand {
    /// Information for each command
    pub fn help() -> &'static str {
        r#"
Available commands (not case sensitive):
HELP (this one)

SHOW DATABASES: List databases available on the server

USE [DATABASE] <name>: Set the current database to name

[EXIT | QUIT]: Quit this session and exit the program

# Examples:
SHOW DATABASES;
USE DATABASE foo;

# Basic IOx SQL Primer

;; Explore Schema:
SHOW TABLES; ;; Show available tables
SHOW COLUMNS FROM my_table; ;; Show columns in the table

;; Show storage usage across partitions and tables
SELECT
   partition_key, table_name, storage,
   count(*) as chunk_count,
   sum(estimated_bytes)/(1024*1024) as size_mb
FROM
  system.chunks
GROUP BY
   partition_key, table_name, storage
ORDER BY
  size_mb DESC
LIMIT 20
;

"#
    }
}

#[cfg(test)]
impl TryInto<ReplCommand> for &str {
    type Error = String;

    fn try_into(self) -> Result<ReplCommand, Self::Error> {
        self.to_string().try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::unnecessary_wraps)]
    fn sql_cmd(sql: &str) -> Result<ReplCommand, String> {
        Ok(ReplCommand::SqlCommand {
            sql: sql.to_string(),
        })
    }

    #[test]
    fn empty() {
        let expected: Result<ReplCommand, String> = Err("No command specified".to_string());

        assert_eq!("".try_into(), expected);
        assert_eq!("  ".try_into(), expected);
        assert_eq!(" \t".try_into(), expected);
    }

    #[test]
    fn help() {
        let expected = Ok(ReplCommand::Help);
        assert_eq!("help;".try_into(), expected);
        assert_eq!("help".try_into(), expected);
        assert_eq!("  help".try_into(), expected);
        assert_eq!("  help  ".try_into(), expected);
        assert_eq!("  HELP  ".try_into(), expected);
        assert_eq!("  Help;  ".try_into(), expected);
        assert_eq!("  help  ; ".try_into(), expected);
        assert_eq!("  help me;  ".try_into(), expected);
    }

    #[test]
    fn show_databases() {
        let expected = Ok(ReplCommand::ShowDatabases);
        assert_eq!("show databases".try_into(), expected);
        assert_eq!("show  Databases".try_into(), expected);
        assert_eq!("show  databases;".try_into(), expected);
        assert_eq!("SHOW DATABASES".try_into(), expected);

        assert_eq!("SHOW DATABASES DD".try_into(), sql_cmd("SHOW DATABASES DD"));
    }

    #[test]
    fn use_database() {
        let expected = Ok(ReplCommand::UseDatabase {
            db_name: "Foo".to_string(),
        });
        assert_eq!("use Foo".try_into(), expected);
        assert_eq!("use Database Foo;".try_into(), expected);
        assert_eq!("use Database Foo ;".try_into(), expected);
        assert_eq!(" use Database Foo;   ".try_into(), expected);
        assert_eq!("   use Database Foo;   ".try_into(), expected);

        // ensure that database name is case sensitive
        let expected = Ok(ReplCommand::UseDatabase {
            db_name: "FOO".to_string(),
        });
        assert_eq!("use FOO".try_into(), expected);
        assert_eq!("use DATABASE FOO;".try_into(), expected);
        assert_eq!("USE DATABASE FOO;".try_into(), expected);

        let expected: Result<ReplCommand, String> =
            Err("name not specified. Usage: USE DATABASE <name>".to_string());
        assert_eq!("use Database;".try_into(), expected);
        assert_eq!("use DATABASE".try_into(), expected);
        assert_eq!("use database".try_into(), expected);

        let expected = sql_cmd("use database foo bar");
        assert_eq!("use database foo bar".try_into(), expected);

        let expected = sql_cmd("use database foo BAR");
        assert_eq!("use database foo BAR".try_into(), expected);
    }

    #[test]
    fn sql_command() {
        let expected = sql_cmd("SELECT * from foo");
        assert_eq!("SELECT * from foo".try_into(), expected);
        // ensure that we aren't messing with capitalization
        assert_ne!("select * from foo".try_into(), expected);

        let expected = sql_cmd("select * from foo");
        assert_eq!("select * from foo".try_into(), expected);

        // default to sql command
        let expected = sql_cmd("blah");
        assert_eq!("blah".try_into(), expected);
    }

    #[test]
    fn exit() {
        let expected = Ok(ReplCommand::Exit);
        assert_eq!("exit".try_into(), expected);
        assert_eq!("exit;".try_into(), expected);
        assert_eq!("exit ;".try_into(), expected);
        assert_eq!("EXIT".try_into(), expected);

        assert_eq!("quit".try_into(), expected);
        assert_eq!("quit;".try_into(), expected);
        assert_eq!("quit ;".try_into(), expected);
        assert_eq!("QUIT".try_into(), expected);

        let expected = sql_cmd("quit dragging");
        assert_eq!("quit dragging".try_into(), expected);
    }
}
