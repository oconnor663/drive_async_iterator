#![feature(async_iterator)]
#![feature(async_for_loop)]
#![feature(gen_blocks)]
#![feature(yield_expr)]
#![allow(unused_features)]

use drive_async_iterator::drive;

#[tokio::test]
async fn test_drive() {
    let mut finished = false;
    drive!(futures::stream::iter([1, 2, 3]), {
        assert_eq!(next().await, Some(1));
        assert_eq!(next().await, Some(2));
        assert_eq!(next().await, Some(3));
        assert_eq!(next().await, None);
        finished = true;
    });
    assert!(finished);
}
