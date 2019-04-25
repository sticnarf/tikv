// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! The macros crate contains all useful needed macros.

/// Gets the count of macro's arguments.
///
/// # Examples
///
/// ```
/// # #[macro_use] extern crate tikv_util;
/// # fn main() {
/// assert_eq!(count_args!(), 0);
/// assert_eq!(count_args!(1), 1);
/// assert_eq!(count_args!(1, 2), 2);
/// assert_eq!(count_args!(1, 2, 3), 3);
/// # }
/// ```
#[macro_export]
macro_rules! count_args {
    () => { 0 };
    ($head:expr $(, $tail:expr)*) => { 1 + count_args!($($tail),*) };
}

/// Initializes a `HashMap` with specified key-value pairs.
///
/// # Examples
///
/// ```
/// # #[macro_use] extern crate tikv_util;
/// # fn main() {
/// // empty map
/// let m: std::collections::HashMap<u8, u8> = map!();
/// assert!(m.is_empty());
///
/// // one initial kv pairs.
/// let m = map!("key" => "value");
/// assert_eq!(m.len(), 1);
/// assert_eq!(m["key"], "value");
///
/// // initialize with multiple kv pairs.
/// let m = map!("key1" => "value1", "key2" => "value2");
/// assert_eq!(m.len(), 2);
/// assert_eq!(m["key1"], "value1");
/// assert_eq!(m["key2"], "value2");
/// # }
/// ```
#[macro_export]
macro_rules! map {
    () => {
        {
            std::collections::HashMap::new()
        }
    };
    ( $( $k:expr => $v:expr ),+ ) => {
        {
            let mut temp_map =
                std::collections::HashMap::with_capacity(
                    count_args!($(($k, $v)),+)
                );
            $(
                temp_map.insert($k, $v);
            )+
            temp_map
        }
    };
}

/// Boxes error first, and then does the same thing as `try!`.
#[macro_export]
macro_rules! box_try {
    ($expr:expr) => {{
        match $expr {
            Ok(r) => r,
            Err(e) => return Err(box_err!(e)),
        }
    }};
}

/// A shortcut to box an error.
#[macro_export]
macro_rules! box_err {
    ($e:expr) => ({
        use std::error::Error;
        let e: Box<dyn Error + Sync + Send> = format!("[{}:{}]: {}", file!(), line!(),  $e).into();
        e.into()
    });
    ($f:tt, $($arg:expr),+) => ({
        box_err!(format!($f, $($arg),+))
    });
}

/// Logs slow operations with `warn!`.
#[macro_export]
macro_rules! slow_log {
    ($t:expr, $($arg:tt)*) => {{
        if $t.is_slow() {
            warn!($($arg)*; "takes" => ?$t.elapsed());
        }
    }}
}

/// Makes a thread name with an additional tag inherited from the current thread.
#[macro_export]
macro_rules! thd_name {
    ($name:expr) => {{
        $crate::get_tag_from_thread_name()
            .map(|tag| format!("{}::{}", $name, tag))
            .unwrap_or_else(|| $name.to_owned())
    }};
}

/// Simulates Go's defer.
///
/// Please note that, different from go, this defer is bound to scope.
/// When exiting the scope, its deferred calls are executed in last-in-first-out order.
#[macro_export]
macro_rules! defer {
    ($t:expr) => {
        let __ctx = $crate::DeferContext::new(|| $t);
    };
}

/// Waits for async operation. It returns `Option<Res>` after the expression gets executed.
/// It only accepts a `Result` expression.
#[macro_export]
macro_rules! wait_op {
    ($expr:expr) => {
        wait_op!(IMPL $expr, None)
    };
    ($expr:expr, $timeout:expr) => {
        wait_op!(IMPL $expr, Some($timeout))
    };
    (IMPL $expr:expr, $timeout:expr) => {{
        use std::sync::mpsc;
        let (tx, rx) = mpsc::channel();
        let cb = Box::new(move |res| {
            // we don't care error actually.
            let _ = tx.send(res);
        });
        $expr(cb)?;
        match $timeout {
            None => rx.recv().ok(),
            Some(timeout) => rx.recv_timeout(timeout).ok(),
        }
    }};
}

/// Checks `Result<Option<T>>`, and returns early when it meets `Err` or `Ok(None)`.
#[macro_export]
macro_rules! try_opt {
    ($expr:expr) => {{
        match $expr {
            Err(e) => return Err(e.into()),
            Ok(None) => return Ok(None),
            Ok(Some(res)) => res,
        }
    }};
}

/// Checks `Result<Option<T>>`, and returns early when it meets `Err` or `Ok(None)`.
/// return `Ok(or)` when met `Ok(None)`.
#[macro_export]
macro_rules! try_opt_or {
    ($expr:expr, $or:expr) => {{
        match $expr {
            Err(e) => return Err(e.into()),
            Ok(None) => return Ok($or),
            Ok(Some(res)) => res,
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    #[test]
    fn test_box_error() {
        let file_name = file!();
        let line_number = line!();
        let e: Box<dyn Error + Send + Sync> = box_err!("{}", "hi");
        assert_eq!(
            format!("{}", e),
            format!("[{}:{}]: hi", file_name, line_number + 1)
        );
    }
}
