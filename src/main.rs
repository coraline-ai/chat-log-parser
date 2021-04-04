use clap::{App, Arg, SubCommand};
use multimap::MultiMap;
use rand::SeedableRng;
use rand_pcg::Pcg64Mcg;
use std::fs::{create_dir, remove_file, File};
use std::io::Write;
use std::path::Path;

use chat_log_parser_lib::*;

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
            let (fb_file, name, output_file_path, test_ratio, seed) = (
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

            if !Path::new(output_file_path).exists() {
                create_dir(output_file_path).unwrap();
            }

            match test_ratio {
                None => {}
                Some(test_ratio) => {
                    assert!(test_ratio < 1.0 && test_ratio > 0.0);
                }
            };
            let zip_file = File::open(fb_file).unwrap();
            let mut zip = zip::ZipArchive::new(zip_file).unwrap();

            // Maps from a String of the conversation name -> all conversation zip file IDs
            let all_conversations: MultiMap<String, usize> = get_all_conversations(&mut zip);

            let all_conversations = match name {
                Some(name) => {
                    let mut map: MultiMap<String, usize> = MultiMap::new();
                    map.insert_many_from_slice(
                        name.into(),
                        all_conversations.get_vec(name).unwrap(),
                    );
                    map
                }
                None => all_conversations,
            };

            let write_msgs = |msgs: &[Message],
                              participants: &[Participant],
                              name: &str,
                              suffix: Option<&'static str>| {
                let out_parent_path = Path::new(output_file_path);
                let output_file_name: String = match suffix {
                    None => String::from(name),
                    Some(suffix) => format!("{}_{}.{}", name, suffix, ".txt"),
                }
                .into();
                let output_file_name = Path::new(&output_file_name);

                let out_path = out_parent_path.join(output_file_name);

                match remove_file(&out_path) {
                    Ok(_) => println!("Warning: Overwriting {:?}", &out_path),
                    Err(_) => {}
                };

                let mut output_file = File::create(out_path).unwrap();
                let formatted_messages =
                    format_conversation(msgs, participants, "|EOM|", "<|endoftext|>");
                output_file.write(formatted_messages.as_bytes()).unwrap();
            };

            for conversation_name in all_conversations.keys() {
                let mut title = None;
                let mut prev_participants = None;

                let conversation_idx = all_conversations.get_vec(conversation_name).unwrap();

                let mut conversation_messages: Vec<Message> = Vec::new();

                // TODO: Tagged message is probably not the right approach. We need to segment
                // each conversation into exchanges first, and then insert those by timestamp.
                // format_conversation will need to be removed
                for (i, &idx) in conversation_idx.iter().enumerate() {
                    let mut zip_file = zip.by_index(idx).unwrap();
                    let (_title, _participants, mut messages) =
                        parse_messages(&mut zip_file).unwrap();

                    if messages.len() == 0 {
                        continue;
                    }

                    // In a given conversation, we don't expect the participants to change
                    match prev_participants {
                        Some(prev_participants) => {
                            assert!(prev_participants == _participants);
                        }
                        None => {}
                    };

                    prev_participants = Some(_participants);
                    title = Some(_title);
                    println!(
                        "Parsed {} messages from compressed json file {} -- {:.2} MB",
                        &messages.len(),
                        i,
                        (zip_file.size() as f64) / (1 << 20) as f64
                    );

                    conversation_messages.append(&mut messages);
                }

                conversation_messages.sort_by_key(|a| a.timestamp);
                println!(
                    "Sorted {} messages by timestamp",
                    conversation_messages.len()
                );

                let prev_participants = prev_participants.unwrap();

                println!(
                    "\n\nConversation title: {}\nParticipants: {:?}",
                    title.unwrap(),
                    prev_participants
                );

                match test_ratio {
                    None => {
                        write_msgs(
                            &conversation_messages,
                            &prev_participants,
                            conversation_name,
                            None,
                        );
                    }
                    Some(test_ratio) => {
                        let mut rng = match seed {
                            Some(seed) => Pcg64Mcg::seed_from_u64(seed),
                            None => Pcg64Mcg::from_entropy(),
                        };
                        let (train_messages, test_messages) =
                            train_test(&conversation_messages, test_ratio, &mut rng);

                        write_msgs(
                            &train_messages,
                            &prev_participants,
                            conversation_name,
                            Some("train"),
                        );
                        write_msgs(
                            &test_messages,
                            &prev_participants,
                            conversation_name,
                            Some("test"),
                        );
                    }
                };
            }
        }
        e => {
            println!("Invalid option {:?}!", e);
        }
    };
}
