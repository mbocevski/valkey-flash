// FLASH.LPUSHX and FLASH.RPUSHX are implemented in lpush.rs
// (do_push with only_if_exists=true). This module re-exports them.
pub use crate::commands::lpush::{flash_lpushx_command, flash_rpushx_command};
