use clap::{App, Arg, SubCommand};
use multimap::MultiMap;
use rand::SeedableRng;
use rand_pcg::Pcg64Mcg;
use std::ffi::OsStr;
use std::fs::{remove_file, File};
use std::io::Write;
use std::path::Path;

use chat_log_parser_lib::*;

fn get_all_conversations(zip: &mut zip::ZipArchive<File>) -> MultiMap<String, usize> {
    get_names(zip)
        .unwrap()
        .iter()
        .filter(|(name, _)| Path::new(&name).components().count() == 4)
        .map(|(name, idx)| {
            (
                String::from(
                    Path::new(&name)
                        .parent()
                        .unwrap()
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap(),
                ),
                *idx,
            )
        })
        .collect()
}

fn list(fb_file: &str) -> Vec<String> {
    let zip_file = File::open(fb_file).unwrap();
    let mut zip = zip::ZipArchive::new(zip_file).unwrap();

    let all_conversations: MultiMap<String, usize> = get_all_conversations(&mut zip);
    let all_conversations: Vec<(String, Vec<usize>)> = all_conversations.into_iter().collect();
    println!("{:?}", all_conversations);
    let all_conversations: Vec<String> = all_conversations
        .iter()
        .map(|(name, _)| name.clone())
        .collect();

    all_conversations
}

fn main() {
    // this is kind of gross and doesn't work well,
    // refactor later
    let matches = App::new("Facebook Messenger Chatbot Dataset Generator")
        .version("0.1")
        .author("Srinvas Kaza <kazasrinivas3@gmail.com>")
        .about(
            "Converts a zip of Facebook messenger data into
               a suitable format for GPT2",
        )
        .subcommand(
            SubCommand::with_name("list")
                .about("List people in messenger chat history")
                .arg(
                    Arg::with_name("input")
                        .value_name("FILE")
                        .required(true)
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("generate")
                .about("Generates GPT2 dataset from chat logs")
                .arg(
                    Arg::with_name("name")
                        .long("name")
                        .required(false)
                        .short("n")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("input")
                        .value_name("FILE")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("test")
                        .long("test")
                        .short("t")
                        .value_name("test ratio")
                        .required(false)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("seed")
                        .long("seed")
                        .short("s")
                        .value_name("RNG seed")
                        .required(false)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("output")
                        .long("output")
                        .short("o")
                        .value_name("FILE")
                        .required(true)
                        .takes_value(true),
                ),
        )
        .get_matches();

    match matches.subcommand_name() {
        Some("list") => {
            // refactor this later
            let fb_file = matches
                .subcommand_matches("list")
                .unwrap()
                .value_of("input")
                .unwrap();

            for conversation in list(fb_file) {
                println!("{}", conversation);
            }
        }
        Some("generate") => {
            let generate_match = matches.subcommand_matches("generate").unwrap();
            let (fb_file, name, output_file_name, test_ratio, seed) = (
                generate_match.value_of("input").unwrap(),
                generate_match.value_of("name"),
                generate_match.value_of("output").unwrap(),
                match generate_match.value_of("test") {
                    None => None,
                    Some(test_ratio) => Some(test_ratio.parse::<f32>().unwrap()),
                },
                match generate_match.value_of("seed") {
                    None => None,
                    Some(seed) => Some(seed.parse::<u64>().unwrap()),
                },
            );
            match test_ratio {
                None => {}
                Some(test_ratio) => {
                    assert!(test_ratio < 1.0 && test_ratio > 0.0);
                }
            };
            let zip_file = File::open(fb_file).unwrap();
            let mut zip = zip::ZipArchive::new(zip_file).unwrap();

            let all_conversations: MultiMap<String, usize> = get_all_conversations(&mut zip);

            let conversation_idx = match name {
                Some(name) => all_conversations.get_vec(name).unwrap().clone(),
                None => {
                    let mut convos: Vec<_> =
                        all_conversations.iter().map(|(name, idx)| *idx).collect();
                    convos.sort();
                    convos.dedup();
                    convos
                }
            };

            let mut title = None;
            let mut prev_participants = None;
            let mut all_messages: Vec<Message> = Vec::new();

            for (i, &idx) in conversation_idx.iter().enumerate() {
                let mut zip_file = zip.by_index(idx).unwrap();
                let (_title, _participants, mut messages) = parse_messages(&mut zip_file).unwrap();
                if name.is_some() {
                    match prev_participants {
                        Some(prev_participants) => {
                            assert!(prev_participants == _participants);
                        }
                        None => {}
                    };
                }
                prev_participants = Some(_participants);
                title = match name {
                    Some(_) => Some(_title),
                    None => Some("".into()),
                };
                println!(
                    "Parsed {} messages from compressed json file {} -- {:.2} MB",
                    &messages.len(),
                    i,
                    (zip_file.size() as f64) / (1 << 20) as f64
                );
                all_messages.append(&mut messages);
            }

            all_messages.sort_by_key(|a| a.timestamp);
            println!("Sorted {} messages by timestamp", all_messages.len());

            println!(
                "\n\nConversation title: {}\nParticipants: {:?}",
                title.unwrap(),
                match name {
                    Some(_) => prev_participants
                        .unwrap()
                        .iter()
                        .map(|p| p.name.clone())
                        .collect::<Vec<String>>(),
                    None => vec!["Many".into()],
                }
            );

            let write_msgs = |msgs, suffix: Option<&'static str>| {
                let out_path = Path::new(output_file_name);
                let output_file_path = match suffix {
                    None => String::from(output_file_name),
                    Some(suffix) => format!(
                        "{}_{}.{}",
                        out_path.file_stem().and_then(OsStr::to_str).unwrap(),
                        suffix,
                        out_path.extension().and_then(OsStr::to_str).unwrap()
                    ),
                };

                match remove_file(&output_file_path) {
                    Ok(_) => println!("Warning: Overwriting {}", &output_file_path),
                    Err(_) => {}
                };

                let mut output_file = File::create(output_file_path).unwrap();
                let formatted_messages = format_conversation(msgs, "|EOM|", "<|endoftext|>");
                output_file.write(formatted_messages.as_bytes()).unwrap();
            };

            match test_ratio {
                None => {
                    write_msgs(&all_messages, None);
                }
                Some(test_ratio) => {
                    let mut rng = match seed {
                        Some(seed) => Pcg64Mcg::seed_from_u64(seed),
                        None => Pcg64Mcg::from_entropy(),
                    };
                    let (train_messages, test_messages) =
                        train_test(all_messages, test_ratio, &mut rng);

                    write_msgs(&train_messages, Some("train"));
                    write_msgs(&test_messages, Some("test"));
                }
            };
        }
        e => {
            println!("Invalid option {:?}!", e);
        }
    };
}
