extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
extern crate r2d2;
extern crate time;

mod db;
mod psedo_bundle;

use psedo_bundle::PseudoBundle;
#[cfg(test)]
mod tests {
    #[test]
    fn scylladb_access() {
        println!("connecting to db");
        let mut session = connect_to_db();
        println!("adding a psudo bundle");
        let test_bundle = PseudoBundle {
            bundle: String::from("Psedo Bundle 1"),
            time: time::Timespec::new(10001, 0),
            info: String::from("Info of Psedo Bundle 1"),
        };
        db::add_bundle(&mut session, test_bundle).expect("add bundle error");
        let prepared_query = db::prepare_add_bundle(&mut session).expect("prepare query error");
        db::execute_add_bundle(
            &mut session,
            &prepared_query,
            PseudoBundle {
                bundle: String::from("Psedo Bundle 2"),
                time: time::Timespec::new(10002, 0),
                info: String::from("Info of Psedo Bundle 2"),
            },
        )
        .expect("execute add bundle error");
        let bundles = db::select_bundles_by_time_range(
            &mut session,
            time::Timespec::new(10000, 0),
            time::Timespec::new(10010, 0),
        )
        .expect("select bundles error");
        println!("     >> bundles: {:?}", bundles);
    }
    fn connect_to_db() -> db::CurrentSession {
        let mut session = db::create_db_session().expect("create db session error");
        db::create_keyspace(&mut session).expect("create keyspace error");
        db::create_bundle_table(&mut session).expect("create keyspace error");
        session
    }
}
