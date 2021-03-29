use clap::{
    load_yaml,
    App,
    ArgMatches,
};
use permanode_common::config::Config;

fn main() {
    let yaml = load_yaml!("../cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    if matches.is_present("start") {
        todo!("Start the chronicle instance");
    } else {
        let config = Config::load().expect("No config file found for Chronicle!");
        todo!("Connect to the websocket");

        if matches.is_present("stop") {
            todo!("Stop the chronicle instance");
        } else if matches.is_present("rebuild") {
            todo!("Rebuild the RING instance");
        } else {
            match matches.subcommand() {
                ("node", Some(subcommand)) => node(subcommand),
                ("brokers", Some(subcommand)) => brokers(subcommand),
                ("archive", Some(subcommand)) => archive(subcommand),
                _ => (),
            }
        }
    }
}

fn node(matches: &ArgMatches) {
    match matches.subcommand() {
        ("add", Some(subcommand)) => {
            let command_address = subcommand.value_of("command-address").unwrap();
            let scylladb_address = subcommand.value_of("scylladb-address").unwrap();
            todo!("Send message");
        }
        ("remove", Some(subcommand)) => {
            let id = subcommand.value_of("id").unwrap();
            todo!("Send message");
        }
        ("list", Some(subcommand)) => {
            todo!("List nodes");
        }
        _ => (),
    }
}

fn brokers(matches: &ArgMatches) {
    match matches.subcommand() {
        ("add", Some(subcommand)) => {
            let mqtt_address = subcommand.value_of("mqtt-address").unwrap();
            let endpoint_address = subcommand.value_of("endpoint-address").unwrap();
            todo!("Send message");
        }
        ("remove", Some(subcommand)) => {
            let id = subcommand.value_of("id").unwrap();
            todo!("Send message");
        }
        ("list", Some(subcommand)) => {
            todo!("List brokers");
        }
        _ => (),
    }
}

fn archive(matches: &ArgMatches) {
    match matches.subcommand() {
        ("import", Some(subcommand)) => {
            let dir = subcommand.value_of("directory");
            let range = subcommand.value_of("range");
            todo!("Send message");
        }
        _ => (),
    }
}
