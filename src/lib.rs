use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Utc};
use multimap::MultiMap;
use rand::Rng;
use rand_pcg::Pcg64Mcg;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::Path;

//use mimalloc::MiMalloc;

//#[global_allocator]
//static GLOBAL: MiMalloc = MiMalloc;

// AFK for more than 10 minutes means new conversation
pub const CONVERSATION_TIMEOUT: i64 = 10 * 60;

pub const TRAIN_TEST_TIMEOUT: i64 = 1;

pub struct TaggedMessage {
    pub content: String,
    pub author: String,
    pub timestamp: DateTime<Utc>,
    pub conversation_id: usize,
}

impl TaggedMessage {
    pub fn new(m: &Message, conversation_id: usize) -> TaggedMessage {
        TaggedMessage {
            content: m.content.clone(),
            author: m.author.clone(),
            timestamp: m.timestamp,
            conversation_id: conversation_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub content: String,
    pub author: String,
    pub timestamp: DateTime<Utc>,
}

pub trait HasURI {
    fn uri<'a>(self: &'a Self) -> &'a str;
    fn header<'a>(self: &'a Self) -> &'static str;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Participant {
    pub name: String,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct Photo {
    pub uri: String,
    pub creation_timestamp: i64,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct Gif {
    pub uri: String,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct Thumbnail {
    pub uri: String,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct Video {
    pub uri: String,
    pub creation_timestamp: i64,
    pub thumbnail: Thumbnail,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct Reaction {
    pub reaction: String,
    pub actor: String,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct Share {
    pub link: Option<String>,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct Sticker {
    pub uri: String,
}
#[derive(Serialize, Deserialize, Clone)]
pub struct RawMessage {
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

pub fn get_names(
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

fn unfuck_facebook_unicode_escapes(json_data: &[u8]) -> String {
    // facebook doesn't encode unicode in JSON correctly -- they use
    // \u{UTF-8 sequence here} instead of just embedding the unicode
    // sequence or using a UTF codepoint. forgive me for this awful fsm

    // we also have to handle the terrible case of the escaped \u in the
    // json -- e.g \\urealdatahere. notably this isn't an issue if you have
    // \\\unicodehere -- that renders as \ unicode here
    let mut prev_backslashes = 0;

    let mut no_awful_unicode = Vec::with_capacity(json_data.len());
    let mut a = 0;
    while a < json_data.len() {
        // detect unicode code point
        let mut cond =
            (json_data[a] == b'\\' && json_data[a + 1] == b'u') && prev_backslashes % 2 == 0;
        if !cond {
            if json_data[a] == b'\\' {
                prev_backslashes += 1;
            } else {
                prev_backslashes = 0;
            }
            // if this chunk of text was not intended to represent a
            // unicode sequence
            no_awful_unicode.push(char::from(json_data[a]));
            a += 1;
        } else {
            // if we've discovered a unicode sequence, parse out each
            // utf-8 code unit
            let mut char_buf = Vec::new();
            while cond {
                // single code-unit, represented like \uXXXX,
                // where XXXX is an 8-bit hex literal
                let u8_buf = [
                    json_data[a + 2],
                    json_data[a + 3],
                    json_data[a + 4],
                    json_data[a + 5],
                ];
                let u8_buf: &str = std::str::from_utf8(&u8_buf).unwrap();
                let u8_elem: u8 = u8::from_str_radix(&u8_buf, 16).unwrap();
                char_buf.push(u8_elem);
                a += 6;

                if a >= json_data.len() {
                    break;
                }
                cond = json_data[a] == b'\\' && json_data[a + 1] == b'u';
            }

            let mut c_buf: Vec<char> = String::from_utf8(char_buf).unwrap().chars().collect();
            no_awful_unicode.append(&mut c_buf);
            prev_backslashes = 0;
        }
    }
    let no_awful_unicode: String = no_awful_unicode.into_iter().collect();
    no_awful_unicode
}

// TODO: Create trait for message parsing, and move this into its own
// impl
pub fn parse_messages(
    file: &mut zip::read::ZipFile,
) -> serde_json::Result<(String, Vec<Participant>, Vec<Message>)> {
    let mut u8_repr = Vec::new();
    file.read_to_end(&mut u8_repr).unwrap();

    let mut no_awful_unicode = unfuck_facebook_unicode_escapes(&u8_repr);

    let file: serde_json::Value = match serde_json::from_str(&mut no_awful_unicode) {
        Ok(file) => file,
        Err(e) => panic!("Failed on {:?} -- {:?}", no_awful_unicode, e),
    };
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

pub fn train_test(
    conversation: Vec<TaggedMessage>,
    ratio: f32,
    rng: &mut Pcg64Mcg,
) -> (Vec<TaggedMessage>, Vec<TaggedMessage>) {
    let mut train_msgs: Vec<_> = Vec::new();
    let mut test_msgs: Vec<_> = Vec::new();
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

pub fn format_conversation(
    idx_to_participants: &HashMap<usize, Vec<Participant>>,
    conversation: &Vec<TaggedMessage>,
    eom: &str,
    eoc: &str,
) -> String {
    if conversation.len() == 0 {
        return String::new();
    }

    let mut all_message_strs: Vec<String> = Vec::new();
    let mut current_conversation_strs: Vec<String> = Vec::new();
    let mut current_conversation_id: usize = conversation.first().unwrap().conversation_id;
    let mut conversation_timestamp: DateTime<Utc> = conversation[0].timestamp;
    let mut last_timestamp: DateTime<Utc> = conversation[0].timestamp;

    for message in conversation {
        let diff: Duration = message.timestamp.signed_duration_since(last_timestamp);
        if message.conversation_id != current_conversation_id
            || diff.num_seconds() > CONVERSATION_TIMEOUT
        {
            conversation_timestamp = message.timestamp;

            // Add convo header
            let header = format!(
                "|{}|\n",
                idx_to_participants[&current_conversation_id]
                    .iter()
                    .map(|p| p.name.clone())
                    .collect::<Vec<String>>()
                    .join(" ")
            );
            current_conversation_strs.insert(0, header);

            all_message_strs.push(current_conversation_strs.join(eom));
            current_conversation_strs.clear();

            current_conversation_id = message.conversation_id;
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

pub fn get_all_conversations(zip: &mut zip::ZipArchive<File>) -> MultiMap<String, usize> {
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

pub fn list(fb_file: &str) -> Vec<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_unfuck_facebook_unicode_escapes_basic() {
        let input = b"asdf";
        assert_eq!(unfuck_facebook_unicode_escapes(input), "asdf");
        let input = b"\\u0041A";
        assert_eq!(unfuck_facebook_unicode_escapes(input), "AA");
        let input = b"Rados\\u00c5\\u0082aw";
        assert_eq!(unfuck_facebook_unicode_escapes(input), "Radosław");
        let input = b"No to trzeba ostatnie treningi zrobi\\u00c4\\u0087 xD";
        assert_eq!(
            unfuck_facebook_unicode_escapes(input),
            "No to trzeba ostatnie treningi zrobić xD"
        );
    }

    #[test]
    fn test_unfuck_facebook_unicode_escapes_boundaries() {
        let input = b"\\u0041";
        assert_eq!(unfuck_facebook_unicode_escapes(input), "A");

        let input = b"\\\\u0041A";
        assert_eq!(unfuck_facebook_unicode_escapes(input), "\\\\u0041A");
    }
}
