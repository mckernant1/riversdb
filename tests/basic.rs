use std::{fs, io::ErrorKind, path::PathBuf};

use bytes::Bytes;
use riversdb::{
    client::Rivers,
    models::put::PutResponse,
    models::{get::GetRequest, put::PutRequest},
};

#[test]
fn base_test() {
    match fs::remove_dir_all("store").map_err(|it| it.kind()) {
        Err(ErrorKind::NotFound) => (),
        Err(e) => panic!("{}", e),
        Ok(_) => (),
    };
    fs::create_dir("store").unwrap();
    let path = PathBuf::from("store");
    let mut river = Rivers::new(path.clone()).unwrap();

    if let PutResponse::Error { err } = river.put(PutRequest {
        key: Bytes::from("hello"),
        value: Bytes::from("Yessir"),
    }) {
        panic!("{}", err)
    };

    match river.get(GetRequest {
        key: Bytes::from("hello"),
    }) {
        riversdb::models::get::GetResponse::Success { .. } => {}
        riversdb::models::get::GetResponse::DoesNotExist => panic!("Does not exist"),
        riversdb::models::get::GetResponse::Error { err } => panic!("{}", err),
    }

    drop(river);
    let river = Rivers::new(path).unwrap();
    match river.get(GetRequest {
        key: Bytes::from("hello"),
    }) {
        riversdb::models::get::GetResponse::Success { .. } => {}
        riversdb::models::get::GetResponse::DoesNotExist => panic!("Does not exist"),
        riversdb::models::get::GetResponse::Error { err } => panic!("{}", err),
    }

    fs::remove_dir_all("store").unwrap();
}
