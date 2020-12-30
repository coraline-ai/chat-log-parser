use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Utc};
use clap::{App, Arg, SubCommand};
use multimap::MultiMap;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64Mcg;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{remove_file, File};
use std::io::{Read, Write};
use std::path::Path;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// AFK for more than 10 minutes means new conversation
const CONVERSATION_TIMEOUT: i64 = 10 * 60;

const TRAIN_TEST_TIMEOUT: i64 = 1;

#[derive(Debug, Clone)]
struct Message {
    content: String,
    author: String,
    timestamp: DateTime<Utc>,
}

trait HasURI {
    fn uri<'a>(self: &'a Self) -> &'a str;
    fn header<'a>(self: &'a Self) -> &'static str;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Participant {
    name: String,
}
#[derive(Serialize, Deserialize, Clone)]
struct Photo {
    uri: String,
    creation_timestamp: i64,
}
#[derive(Serialize, Deserialize, Clone)]
struct Gif {
    uri: String,
}
#[derive(Serialize, Deserialize, Clone)]
struct Thumbnail {
    uri: String,
}
#[derive(Serialize, Deserialize, Clone)]
struct Video {
    uri: String,
    creation_timestamp: i64,
    thumbnail: Thumbnail,
}
#[derive(Serialize, Deserialize, Clone)]
struct Reaction {
    reaction: String,
    actor: String,
}
#[derive(Serialize, Deserialize, Clone)]
struct Share {
    link: Option<String>,
}
#[derive(Serialize, Deserialize, Clone)]
struct Sticker {
    uri: String,
}
#[derive(Serialize, Deserialize, Clone)]
struct RawMessage {
    sender_name: String,
    timestamp_ms: i64,
    photos: Option<Vec<Photo>>,
    content: Option<String>,
    sticker: Option<Sticker>,
    gifs: Option<Vec<Gif>>,
    videos: Option<Vec<Video>>,
    reactions: Option<Vec<Reaction>>,
    share: Option<Share>,
    r#type: String,
}

impl HasURI for Sticker {
    fn uri<'a>(self: &'a Self) -> &'a str {
        &self.uri
    }

    fn header<'a>(self: &'a Self) -> &'static str {
        "STICKER"
    }
}
impl HasURI for Video {
    fn uri<'a>(self: &'a Self) -> &'a str {
        &self.uri
    }

    fn header<'a>(self: &'a Self) -> &'static str {
        "VIDEOS"
    }
}
impl HasURI for Gif {
    fn uri<'a>(self: &'a Self) -> &'a str {
        &self.uri
    }

    fn header<'a>(self: &'a Self) -> &'static str {
        "GIFS"
    }
}
impl HasURI for Photo {
    fn uri<'a>(self: &'a Self) -> &'a str {
        &self.uri
    }

    fn header<'a>(self: &'a Self) -> &'static str {
        "PHOTOS"
    }
}

fn get_uris<T: HasURI>(input: &Vec<T>) -> String {
    format!(
        "{}: {}",
        if input.len() > 0 {
            input[0].header()
        } else {
            "NONE"
        },
        input
            .iter()
            .map(|v| (v.uri()))
            .fold(String::new(), |a, b| format!("{}-{}", a, b))
    )
}

fn get_names(
    zip: &mut zip::read::ZipArchive<std::fs::File>,
) -> std::io::Result<HashMap<String, usize>> {
    Ok((0..zip.len())
        .map(|i| {
            let file = zip.by_index(i).unwrap();
            match Path::new(file.name()).extension().and_then(OsStr::to_str) {
                Some("json") => {
                    if file.name().contains("_") {
                        Some((String::from(file.name()), i))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .collect())
}

fn parse_messages(
    file: &mut zip::read::ZipFile,
) -> serde_json::Result<(String, Vec<Participant>, Vec<Message>)> {
    let mut u8_repr = Vec::new();
    file.read_to_end(&mut u8_repr).unwrap();

    // facebook doesn't encode unicode in JSON correctly -- they use
    // \u{UTF-8 sequence here} instead of just embedding the unicode
    // sequence or using a UTF codepoint. forgive me for this awful fsm

    let mut no_awful_unicode = Vec::with_capacity(u8_repr.len());
    let mut a = 0;
    while a < u8_repr.len() {
        // detect unicode code point
        let mut cond = u8_repr[a] == b'\\' && u8_repr[a + 1] == b'u';
        if !cond {
            // if this chunk of text was not intended to represent a
            // unicode sequence
            no_awful_unicode.push(char::from(u8_repr[a]));
            a += 1;
        } else {
            // if we've discovered a unicode sequence, parse out each
            // utf-8 code unit
            let mut char_buf = Vec::new();
            while cond {
                // single code-unit, represented like \uXXXX,
                // where XXXX is an 8-bit hex literal
                let u8_buf = [
                    u8_repr[a + 2],
                    u8_repr[a + 3],
                    u8_repr[a + 4],
                    u8_repr[a + 5],
                ];
                let u8_buf: &str = std::str::from_utf8(&u8_buf).unwrap();
                let u8_elem: u8 = u8::from_str_radix(&u8_buf, 16).unwrap();
                char_buf.push(u8_elem);
                a += 6;
                cond = u8_repr[a] == b'\\' && u8_repr[a + 1] == b'u';
            }

            let mut c_buf: Vec<char> = String::from_utf8(char_buf).unwrap().chars().collect();
            no_awful_unicode.append(&mut c_buf);
        }
    }
    let mut no_awful_unicode: String = no_awful_unicode.into_iter().collect();

    let file: serde_json::Value = simd_json::from_str(&mut no_awful_unicode).unwrap();
    let title: String = serde_json::from_value(file["title"].clone()).unwrap();

    let participants: Vec<Participant> = serde_json::from_value(file["participants"].clone())?;
    let messages: Vec<RawMessage> = serde_json::from_value(file["messages"].clone())?;
    let messages: Vec<Message> = messages
        .iter()
        .map(|v: &RawMessage| Message {
            author: v.sender_name.clone(),
            timestamp: DateTime::from_utc(
                NaiveDateTime::from_timestamp(
                    v.timestamp_ms / 1000,
                    (v.timestamp_ms % 1000) as u32 * 1000,
                ),
                Utc,
            ),
            content: match &v.content {
                Some(content) => content.clone(),
                None => {
                    // awful hack
                    match &v.photos {
                        Some(photos) => get_uris(&photos),
                        None => match &v.gifs {
                            Some(gifs) => get_uris(&gifs),
                            None => match &v.videos {
                                Some(videos) => get_uris(&videos),
                                None => String::from(match &v.sticker {
                                    Some(sticker) => sticker.uri(),
                                    None => "UNKOWN CONTENT TYPE",
                                }),
                            },
                        },
                    }
                }
            },
        })
        .collect();

    Ok((title, participants, messages))
}

fn train_test(
    conversation: Vec<Message>,
    ratio: f32,
    rng: &mut Pcg64Mcg,
) -> (Vec<Message>, Vec<Message>) {
    let mut train_msgs: Vec<Message> = Vec::new();
    let mut test_msgs: Vec<Message> = Vec::new();
    let mut is_train: bool = true;
    let mut conversation_timestamp: DateTime<Utc> = conversation[0].timestamp;
    let mut last_timestamp: DateTime<Utc> = conversation[0].timestamp;

    for message in conversation {
        let last_diff: Duration = message.timestamp.signed_duration_since(last_timestamp);
        let convo_diff: Duration = message
            .timestamp
            .signed_duration_since(conversation_timestamp);

        // If it's been a while, consider moving a conversation
        // to the other set
        if last_diff.num_seconds() > CONVERSATION_TIMEOUT
            && convo_diff.num_days() > TRAIN_TEST_TIMEOUT
        {
            is_train = rng.gen::<f32>() > ratio;
            conversation_timestamp = message.timestamp;
        }
        last_timestamp = message.timestamp;

        if is_train {
            train_msgs.push(message);
        } else {
            test_msgs.push(message);
        }
    }

    (train_msgs, test_msgs)
}

fn format_conversation(conversation: &Vec<Message>, eom: &str, eoc: &str) -> String {
    let mut all_message_strs: Vec<String> = Vec::new();
    let mut current_conversation_strs: Vec<String> = Vec::new();
    let mut conversation_timestamp: DateTime<Utc> = conversation[0].timestamp;
    let mut last_timestamp: DateTime<Utc> = conversation[0].timestamp;

    for message in conversation {
        let diff: Duration = message.timestamp.signed_duration_since(last_timestamp);
        if diff.num_seconds() > CONVERSATION_TIMEOUT {
            conversation_timestamp = message.timestamp;
            all_message_strs.push(current_conversation_strs.join(eom));
            current_conversation_strs.clear();
        }
        let diff: Duration = message
            .timestamp
            .signed_duration_since(conversation_timestamp);

        let formatted_msg = format!(
            "|{} {} {} {}|: {}\n",
            conversation_timestamp.month(),
            conversation_timestamp.year(),
            diff.num_seconds(),
            message.author,
            message.content
        );

        current_conversation_strs.push(formatted_msg);

        last_timestamp = message.timestamp;
    }

    return all_message_strs.join(eoc);
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
                        .required(true)
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

    let get_all_conversations = |zip| {
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
    };

    match matches.subcommand_name() {
        Some("list") => {
            // refactor this later
            let fb_file = matches
                .subcommand_matches("list")
                .unwrap()
                .value_of("input")
                .unwrap();

            let zip_file = File::open(fb_file).unwrap();
            let mut zip = zip::ZipArchive::new(zip_file).unwrap();

            let all_conversations: MultiMap<String, usize> = get_all_conversations(&mut zip);
            let all_conversations: Vec<(String, Vec<usize>)> =
                all_conversations.into_iter().collect();
            let all_conversations: Vec<String> = all_conversations
                .iter()
                .map(|(name, _)| name.clone())
                .collect();

            for conversation in all_conversations {
                println!("{}", conversation);
            }
        }
        Some("generate") => {
            let generate_match = matches.subcommand_matches("generate").unwrap();
            let (fb_file, name, output_file_name, test_ratio, seed) = (
                generate_match.value_of("input").unwrap(),
                generate_match.value_of("name").unwrap(),
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
            let conversation_idx: &Vec<usize> = all_conversations.get_vec(name).unwrap();

            let mut all_messages = Vec::new();
            let mut title = None;
            let mut prev_participants = None;
            for (i, &idx) in conversation_idx.iter().enumerate() {
                let mut zip_file = zip.by_index(idx).unwrap();
                let (_title, _participants, mut messages) = parse_messages(&mut zip_file).unwrap();
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
                all_messages.append(&mut messages);
            }
            all_messages.sort_by_key(|a| a.timestamp);
            println!("Sorted {} messages by timestamp", all_messages.len());

            println!(
                "\n\nConversation title: {}\nParticipants: {:?}",
                title.unwrap(),
                prev_participants
                    .unwrap()
                    .iter()
                    .map(|p| p.name.clone())
                    .collect::<Vec<String>>()
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
