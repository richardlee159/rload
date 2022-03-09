use rand::{
    distributions::{Alphanumeric, Uniform},
    Rng,
};

fn string_random(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

fn decimal_random(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(Uniform::from('0'..='9'))
        .take(length)
        .collect()
}

pub fn compose_post() -> String {
    let mut rng = rand::thread_rng();
    let user_index = rng.gen_range(1..=962);
    let username = format!("username_{user_index}");
    let user_id = user_index.to_string();
    let mut text = string_random(256);
    let num_user_mentions = rng.gen_range(0..=5);
    let num_urls = rng.gen_range(0..=5);
    let num_media = rng.gen_range(0..=4);
    let mut media_ids = '['.to_string();
    let mut media_types = '['.to_string();

    for _ in 0..=num_user_mentions {
        let user_mention_id = loop {
            let id = rng.gen_range(1..=962);
            if user_index != id {
                break id;
            }
        };
        text.push_str(" @username_");
        text.push_str(&user_mention_id.to_string());
    }

    for _ in 0..=num_urls {
        text.push_str(" http://");
        text.push_str(&string_random(64));
    }

    for _ in 0..=num_media {
        let media_id = decimal_random(18);
        media_ids.push_str(&format!("\"{}\",", media_id));
        media_types.push_str("\"png\",");
    }

    media_ids.pop();
    media_ids.push(']');
    media_types.pop();
    media_types.push(']');

    let body = if num_media > 0 {
        format!("username={username}&user_id={user_id}&text={text}&media_ids={media_ids}&media_types={media_types}&post_type=0")
    } else {
        format!("username={username}&user_id={user_id}&text={text}&media_ids=&post_type=0")
    };

    body
}
