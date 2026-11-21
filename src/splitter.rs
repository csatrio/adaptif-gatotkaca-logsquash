//! Word splitting (DESIGN.md 4.1). A splitter cuts text into words; concatenating the words
//! gives the original text back, so splitting is always lossless.

/// The longest default word, in characters. Longer alphanumeric runs are cut into chunks.
pub const MAX_WORD_CHARS: usize = 64;

/// Cuts text into words. Implementations must be lossless: the words of a string, concatenated
/// in order, are exactly that string.
pub trait Splitter: Send + Sync {
    /// Byte length of the first word of `rest`. `rest` is never empty; the result is at least 1
    /// and ends on a character boundary.
    fn word_len(&self, rest: &str) -> usize;

    /// Identifier stored in the folder manifest, so a folder is only opened with its own splitter.
    fn id(&self) -> u16;
}

/// Alphanumeric runs (up to 64 characters) are words; every other character is a word.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultSplitter;

/// Like [`DefaultSplitter`], but a backslash plus the next character is one word, and `\u` plus four
/// hex digits is one word, so JSON escapes do not glue onto the following word.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonEscapeSplitter;

fn default_word_len(rest: &str) -> usize {
    let first = rest.chars().next().expect("rest is not empty");
    if !first.is_alphanumeric() {
        return first.len_utf8();
    }
    for (count, (index, ch)) in rest.char_indices().enumerate() {
        if !ch.is_alphanumeric() || count == MAX_WORD_CHARS {
            return index;
        }
    }
    rest.len()
}

impl Splitter for DefaultSplitter {
    fn word_len(&self, rest: &str) -> usize {
        default_word_len(rest)
    }

    fn id(&self) -> u16 {
        1
    }
}

impl Splitter for JsonEscapeSplitter {
    fn word_len(&self, rest: &str) -> usize {
        let Some(after) = rest.strip_prefix('\\') else {
            return default_word_len(rest);
        };
        match after.chars().next() {
            None => 1,
            Some('u') if after.len() >= 5 && after.as_bytes()[1..5].iter().all(u8::is_ascii_hexdigit) => 6,
            Some(c) => 1 + c.len_utf8(),
        }
    }

    fn id(&self) -> u16 {
        2
    }
}

/// Splits `text` into its words (used by tests and tools).
pub fn split_words<'a>(splitter: &dyn Splitter, mut text: &'a str) -> Vec<&'a str> {
    let mut words = Vec::new();
    while !text.is_empty() {
        let n = splitter.word_len(text);
        let (word, rest) = text.split_at(n);
        words.push(word);
        text = rest;
    }
    words
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn default_rules() {
        let w = split_words(&DefaultSplitter, "Hello, wörld_42!");
        assert_eq!(w, ["Hello", ",", " ", "wörld", "_", "42", "!"]);
    }

    #[test]
    fn long_runs_are_chunked_at_64_characters() {
        let text = "a".repeat(130);
        let w = split_words(&DefaultSplitter, &text);
        assert_eq!(w.iter().map(|s| s.len()).collect::<Vec<_>>(), [64, 64, 2]);
    }

    #[test]
    fn json_escapes_do_not_glue_onto_words() {
        assert_eq!(split_words(&DefaultSplitter, r"line1\nfoo"), ["line1", "\\", "nfoo"]);
        assert_eq!(split_words(&JsonEscapeSplitter, r"line1\nfoo"), ["line1", "\\n", "foo"]);
        assert_eq!(split_words(&JsonEscapeSplitter, r#"a\"b\\c\u00e9d"#), ["a", "\\\"", "b", "\\\\", "c", "\\u00e9", "d"]);
        assert_eq!(split_words(&JsonEscapeSplitter, "end\\"), ["end", "\\"]);
        assert_eq!(split_words(&JsonEscapeSplitter, r"\uZZZZ"), ["\\u", "ZZZZ"]);
    }

    proptest! {
        #[test]
        fn splitting_is_lossless(text in ".{0,300}") {
            for splitter in [&DefaultSplitter as &dyn Splitter, &JsonEscapeSplitter] {
                let words = split_words(splitter, &text);
                prop_assert!(words.iter().all(|w| !w.is_empty()));
                prop_assert_eq!(words.concat(), text.clone());
            }
        }

        #[test]
        fn lossless_with_escapes_and_long_runs(parts in prop::collection::vec(
            prop_oneof!["[a-zA-Z0-9]{1,100}", r"\\[a-z\\\x22]", r"\\u[0-9a-f]{0,5}", " |,|:|\n|é"], 0..20)) {
            let text = parts.concat();
            for splitter in [&DefaultSplitter as &dyn Splitter, &JsonEscapeSplitter] {
                prop_assert_eq!(split_words(splitter, &text).concat(), text.clone());
            }
        }
    }
}
