use chrono::{DateTime, NaiveDateTime, Utc};

use clap::{App, Arg, SubCommand};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    link: String,
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

fn get_names(zip_path: &str) -> std::io::Result<HashMap<String, usize>> {
    let zip_file = File::open(zip_path)?;

    let mut zip = zip::ZipArchive::new(zip_file)?;

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
    mut file: zip::read::ZipFile,
) -> serde_json::Result<(String, Vec<Participant>, Vec<Message>)> {
    let mut str_repr = String::new();
    file.read_to_string(&mut str_repr).unwrap();
    let file: serde_json::Value = simd_json::from_str(&mut str_repr).unwrap();
    let title: String = serde_json::from_value(file["title"].clone()).unwrap();

    let participants: Vec<Participant> = serde_json::from_value(file["participants"].clone())?;
    let messages: Vec<RawMessage> = serde_json::from_value(file["messages"].clone())?;
    let messages: Vec<Message> = messages
        .iter()
        .map(|v: &RawMessage| Message {
            author: v.sender_name.clone(),
            timestamp: DateTime::from_utc(NaiveDateTime::from_timestamp(v.timestamp_ms, 0), Utc),
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

fn main() {
    let matches = App::new("Coraline Dataset Generator")
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
                ),
        )
        .get_matches();

    let get_all_conversations = |fb_file| {
        get_names(fb_file)
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

            let all_conversations: HashMap<String, usize> = get_all_conversations(&fb_file);
            let mut all_conversations: Vec<(String, usize)> =
                all_conversations.into_iter().collect();
            all_conversations.sort_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap());
            let all_conversations: Vec<String> = all_conversations
                .iter()
                .map(|(name, _)| name.clone())
                .collect();

            for conversation in all_conversations {
                println!("{}", conversation);
            }
        }
        Some("generate") => {
            // refactor this later
            let fb_file = matches
                .subcommand_matches("generate")
                .unwrap()
                .value_of("input")
                .unwrap();
            let name = matches
                .subcommand_matches("generate")
                .unwrap()
                .value_of("name")
                .unwrap();
            let all_conversations: HashMap<String, usize> = get_all_conversations(&fb_file);
            let conversation_idx = all_conversations[name];
            let zip_file = File::open(fb_file).unwrap();
            let mut zip = zip::ZipArchive::new(zip_file).unwrap();
            let messages = parse_messages(zip.by_index(conversation_idx).unwrap());

            println!("{:?}", messages);
        }
        e => {
            println!("Invalid option {:?}!", e);
        }
    };
}
