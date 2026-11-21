//! The tokenizer (DESIGN.md 4): turns text into a symbol stream and back.
//!
//! A stream is `[varint symbol count][symbol...]`. Each symbol is a varint whose low two bits are a tag:
//! `00` word ordinal, `01` single character, `10` inline opaque run, `11` inline decoded run
//! (base64, UUID or hex; see [`TokenizerOptions`]).

use crate::error::{Error, Result};
use crate::heap::{Heap, MemHeap};
use crate::splitter::Splitter;
use crate::varint;
use crate::words::{SharedWords, WordStore};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use bytes::{BufMut, BytesMut};

const TAG_WORD: u64 = 0;
const TAG_CHAR: u64 = 1;
const TAG_OPAQUE: u64 = 2;
const TAG_DECODED: u64 = 3;

/// Variants of a decoded inline run (the 3 bits above the tag).
const V_B64_STD: u8 = 0;
const V_B64_STD_NO_PAD: u8 = 1;
const V_B64_URL: u8 = 2;
const V_B64_URL_NO_PAD: u8 = 3;
const V_UUID_LOWER: u8 = 4;
const V_UUID_UPPER: u8 = 5;
const V_HEX_LOWER: u8 = 6;
const V_HEX_UPPER: u8 = 7;

/// Thresholds for inline runs. The entropy values are first guesses to be calibrated on real logs.
#[derive(Debug, Clone)]
pub struct TokenizerOptions {
    /// Minimum length of a base64 run.
    pub base64_min_len: usize,
    /// Minimum Shannon entropy (bits per character) of a base64 run.
    pub base64_entropy_bits: f64,
    /// Minimum (even) length of a hex run.
    pub hex_min_len: usize,
    /// Minimum length of an opaque run.
    pub opaque_min_len: usize,
    /// Minimum Shannon entropy (bits per character) of an opaque run.
    pub opaque_entropy_bits: f64,
}

impl Default for TokenizerOptions {
    fn default() -> Self {
        Self { base64_min_len: 64, base64_entropy_bits: 4.5, hex_min_len: 16, opaque_min_len: 24, opaque_entropy_bits: 4.0 }
    }
}

/// Converts text to symbol streams and back, using a shared [`WordStore`].
pub struct Tokenizer<H: Heap = MemHeap> {
    words: SharedWords<H>,
    options: TokenizerOptions,
}

/// Collects the symbols of one string.
#[derive(Default)]
struct Symbols {
    body: Vec<u8>,
    count: u64,
}

impl Symbols {
    fn char(&mut self, c: char) {
        varint::put(&mut self.body, (u64::from(c) << 2) | TAG_CHAR);
        self.count += 1;
    }

    fn ordinal(&mut self, ordinal: u32) {
        varint::put(&mut self.body, (u64::from(ordinal) << 2) | TAG_WORD);
        self.count += 1;
    }

    fn opaque(&mut self, bytes: &[u8]) {
        varint::put(&mut self.body, ((bytes.len() as u64) << 2) | TAG_OPAQUE);
        self.body.extend_from_slice(bytes);
        self.count += 1;
    }

    fn decoded(&mut self, variant: u8, bytes: &[u8]) {
        varint::put(&mut self.body, ((((bytes.len() as u64) << 3) | u64::from(variant)) << 2) | TAG_DECODED);
        self.body.extend_from_slice(bytes);
        self.count += 1;
    }

    fn word<H: Heap>(&mut self, words: &mut WordStore<H>, word: &str) -> Result<()> {
        let mut chars = word.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) => self.char(c),
            _ => self.ordinal(words.intern(word)?),
        }
        Ok(())
    }
}

impl<H: Heap> Tokenizer<H> {
    /// A tokenizer over a shared word store.
    pub fn new(words: SharedWords<H>, options: TokenizerOptions) -> Self {
        Self { words, options }
    }

    /// The shared word store.
    pub fn words(&self) -> &SharedWords<H> {
        &self.words
    }

    /// Encodes `text` and appends its stream to `out`. If this fails, no word of `text` stays in the store.
    pub fn encode(&self, text: &str, splitter: &dyn Splitter, out: &mut BytesMut) -> Result<()> {
        let mut words = self.words.write().expect("word store lock poisoned");
        let checkpoint = words.checkpoint();
        let result = self.encode_with(&mut words, text, splitter, out);
        if result.is_err() {
            words.rollback(checkpoint);
        }
        result
    }

    /// Like [`Tokenizer::encode`] on an already locked store. The caller rolls back on failure.
    pub fn encode_with(&self, words: &mut WordStore<H>, text: &str, splitter: &dyn Splitter, out: &mut BytesMut) -> Result<()> {
        let opts = &self.options;
        let bytes = text.as_bytes();
        let mut symbols = Symbols { body: Vec::with_capacity(text.len() / 2 + 8), count: 0 };
        let mut pos = 0;
        let mut base64_failed_until = 0;
        while pos < text.len() {
            if let Some(len) = match_uuid(text, pos) {
                let (variant, raw) = decode_uuid(&text[pos..pos + len]);
                symbols.decoded(variant, &raw);
                pos += len;
                continue;
            }
            if pos >= base64_failed_until && is_base64_byte(bytes[pos]) {
                let end = base64_run_end(bytes, pos);
                if end - pos >= opts.base64_min_len {
                    if let Some((variant, raw)) = try_base64(&text[pos..end], opts.base64_entropy_bits) {
                        symbols.decoded(variant, &raw);
                        pos = end;
                        continue;
                    }
                }
                base64_failed_until = end;
            }
            let first = text[pos..].chars().next().expect("pos is inside the text");
            if first.is_alphanumeric() {
                let end = pos + text[pos..].find(|c: char| !c.is_alphanumeric()).unwrap_or(text.len() - pos);
                let run = &text[pos..end];
                if let Some((variant, raw)) = try_hex(run, opts.hex_min_len) {
                    symbols.decoded(variant, &raw);
                } else if run.len() >= opts.opaque_min_len
                    && entropy_bits(run.as_bytes()) >= opts.opaque_entropy_bits
                    && words.lookup(run).is_none()
                {
                    symbols.opaque(run.as_bytes());
                } else {
                    while pos < end {
                        let n = splitter.word_len(&text[pos..end]);
                        symbols.word(words, &text[pos..pos + n])?;
                        pos += n;
                    }
                }
                pos = end;
            } else {
                let n = splitter.word_len(&text[pos..]);
                symbols.word(words, &text[pos..pos + n])?;
                pos += n;
            }
        }
        varint::put(out, symbols.count);
        out.extend_from_slice(&symbols.body);
        Ok(())
    }

    /// Decodes the stream at the start of `stream`, appends the text to `out` and returns the bytes consumed.
    pub fn decode(&self, stream: &[u8], out: &mut impl BufMut) -> Result<usize> {
        let words = self.words.read().expect("word store lock poisoned");
        decode_with(&words, stream, out)
    }

    /// Decodes the stream at the start of `stream` into a `String`.
    pub fn decode_string(&self, stream: &[u8]) -> Result<String> {
        let mut out = Vec::new();
        self.decode(stream, &mut out)?;
        String::from_utf8(out).map_err(|_| Error::Corrupt("decoded text is not UTF-8"))
    }
}

/// Decodes a stream using an already locked store; returns the number of bytes consumed.
pub fn decode_with<H: Heap>(words: &WordStore<H>, stream: &[u8], out: &mut impl BufMut) -> Result<usize> {
    let mut pos = 0;
    let count = varint::get(stream, &mut pos)?;
    for _ in 0..count {
        let value = varint::get(stream, &mut pos)?;
        match value & 3 {
            TAG_WORD => {
                let ordinal = u32::try_from(value >> 2).map_err(|_| Error::Corrupt("word ordinal out of range"))?;
                out.put_slice(words.get(ordinal).ok_or(Error::Corrupt("unknown word ordinal"))?.as_bytes());
            }
            TAG_CHAR => {
                let code = u32::try_from(value >> 2).ok().and_then(char::from_u32);
                let c = code.ok_or(Error::Corrupt("invalid character"))?;
                out.put_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
            }
            TAG_OPAQUE => out.put_slice(take(stream, &mut pos, value >> 2)?),
            _ => {
                let variant = ((value >> 2) & 7) as u8;
                render(variant, take(stream, &mut pos, value >> 5)?, out)?;
            }
        }
    }
    Ok(pos)
}

/// Composition of a symbol stream (for tools that calibrate the tokenizer thresholds).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StreamStats {
    /// Symbols that are word ordinals.
    pub words: u64,
    /// Symbols that are single characters.
    pub chars: u64,
    /// Inline opaque runs and the bytes they hold.
    pub opaque_runs: u64,
    /// Bytes held by inline opaque runs.
    pub opaque_bytes: u64,
    /// Inline decoded runs (base64, UUID, hex).
    pub decoded_runs: u64,
    /// Bytes held by inline decoded runs.
    pub decoded_bytes: u64,
}

/// Counts what a stream consists of without needing the word store.
pub fn analyze(stream: &[u8]) -> Result<StreamStats> {
    let mut stats = StreamStats::default();
    let mut pos = 0;
    for _ in 0..varint::get(stream, &mut pos)? {
        let value = varint::get(stream, &mut pos)?;
        match value & 3 {
            TAG_WORD => stats.words += 1,
            TAG_CHAR => stats.chars += 1,
            TAG_OPAQUE => {
                stats.opaque_runs += 1;
                stats.opaque_bytes += take(stream, &mut pos, value >> 2)?.len() as u64;
            }
            _ => {
                stats.decoded_runs += 1;
                stats.decoded_bytes += take(stream, &mut pos, value >> 5)?.len() as u64;
            }
        }
    }
    Ok(stats)
}

fn take<'a>(stream: &'a [u8], pos: &mut usize, len: u64) -> Result<&'a [u8]> {
    let end = usize::try_from(len).ok().and_then(|len| pos.checked_add(len)).ok_or(Error::Corrupt("run length"))?;
    let bytes = stream.get(*pos..end).ok_or(Error::Corrupt("inline run is cut off"))?;
    *pos = end;
    Ok(bytes)
}

/// Writes the original text of a decoded inline run.
fn render(variant: u8, raw: &[u8], out: &mut impl BufMut) -> Result<()> {
    match variant {
        V_B64_STD => out.put_slice(STANDARD.encode(raw).as_bytes()),
        V_B64_STD_NO_PAD => out.put_slice(STANDARD_NO_PAD.encode(raw).as_bytes()),
        V_B64_URL => out.put_slice(URL_SAFE.encode(raw).as_bytes()),
        V_B64_URL_NO_PAD => out.put_slice(URL_SAFE_NO_PAD.encode(raw).as_bytes()),
        V_UUID_LOWER | V_UUID_UPPER => {
            if raw.len() != 16 {
                return Err(Error::Corrupt("UUID run is not 16 bytes"));
            }
            for (i, byte) in raw.iter().enumerate() {
                if matches!(i, 4 | 6 | 8 | 10) {
                    out.put_u8(b'-');
                }
                push_hex(out, *byte, variant == V_UUID_UPPER);
            }
        }
        _ => raw.iter().for_each(|byte| push_hex(out, *byte, variant == V_HEX_UPPER)),
    }
    Ok(())
}

fn push_hex(out: &mut impl BufMut, byte: u8, upper: bool) {
    let digits: &[u8; 16] = if upper { b"0123456789ABCDEF" } else { b"0123456789abcdef" };
    out.put_u8(digits[(byte >> 4) as usize]);
    out.put_u8(digits[(byte & 15) as usize]);
}

fn hex_value(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        _ => b - b'A' + 10,
    }
}

/// Whether all letters of `hex` have one case; returns `Some(upper)` (digits-only counts as lower case).
fn hex_case(hex: impl Iterator<Item = u8>) -> Option<bool> {
    let (mut lower, mut upper) = (false, false);
    for b in hex {
        lower |= matches!(b, b'a'..=b'f');
        upper |= matches!(b, b'A'..=b'F');
    }
    (!(lower && upper)).then_some(upper)
}

/// Length of a UUID (36 bytes) starting at `pos`, if one starts there on word boundaries.
fn match_uuid(text: &str, pos: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let candidate = bytes.get(pos..pos + 36)?;
    let hex_ok = candidate.iter().enumerate().all(|(i, &b)| if matches!(i, 8 | 13 | 18 | 23) { b == b'-' } else { b.is_ascii_hexdigit() });
    if !hex_ok || hex_case(candidate.iter().copied().filter(u8::is_ascii_hexdigit)).is_none() {
        return None;
    }
    let before_ok = !text[..pos].chars().next_back().is_some_and(char::is_alphanumeric);
    let after_ok = !text[pos + 36..].chars().next().is_some_and(char::is_alphanumeric);
    (before_ok && after_ok).then_some(36)
}

fn decode_uuid(text: &str) -> (u8, [u8; 16]) {
    let digits: Vec<u8> = text.bytes().filter(|&b| b != b'-').collect();
    let mut raw = [0u8; 16];
    for (i, pair) in digits.chunks(2).enumerate() {
        raw[i] = (hex_value(pair[0]) << 4) | hex_value(pair[1]);
    }
    let upper = hex_case(digits.iter().copied()).unwrap_or(false);
    (if upper { V_UUID_UPPER } else { V_UUID_LOWER }, raw)
}

fn try_hex(run: &str, min_len: usize) -> Option<(u8, Vec<u8>)> {
    let bytes = run.as_bytes();
    if bytes.len() < min_len || bytes.len() % 2 != 0 || !bytes.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let upper = hex_case(bytes.iter().copied())?;
    let raw = bytes.chunks(2).map(|pair| (hex_value(pair[0]) << 4) | hex_value(pair[1])).collect();
    Some((if upper { V_HEX_UPPER } else { V_HEX_LOWER }, raw))
}

fn is_base64_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'-' | b'_')
}

/// End of the base64-looking run starting at `pos`, including up to two `=` of padding.
fn base64_run_end(bytes: &[u8], pos: usize) -> usize {
    let mut end = pos;
    while end < bytes.len() && is_base64_byte(bytes[end]) {
        end += 1;
    }
    let padding = bytes[end..].iter().take(2).take_while(|&&b| b == b'=').count();
    end + padding
}

/// Decodes `run` as base64 if it is long and random enough and re-encodes to exactly the same text.
fn try_base64(run: &str, min_entropy: f64) -> Option<(u8, Vec<u8>)> {
    let body = run.trim_end_matches('=');
    let padded = body.len() != run.len();
    let (std_chars, url_chars) = (body.contains(['+', '/']), body.contains(['-', '_']));
    if std_chars && url_chars || entropy_bits(body.as_bytes()) < min_entropy {
        return None;
    }
    let variant = match (url_chars, padded) {
        (false, true) => V_B64_STD,
        (false, false) => V_B64_STD_NO_PAD,
        (true, true) => V_B64_URL,
        (true, false) => V_B64_URL_NO_PAD,
    };
    let engine: &dyn Fn(&str) -> Option<Vec<u8>> = match variant {
        V_B64_STD => &|s| STANDARD.decode(s).ok(),
        V_B64_STD_NO_PAD => &|s| STANDARD_NO_PAD.decode(s).ok(),
        V_B64_URL => &|s| URL_SAFE.decode(s).ok(),
        _ => &|s| URL_SAFE_NO_PAD.decode(s).ok(),
    };
    let raw = engine(run)?;
    let mut again = Vec::with_capacity(run.len());
    render(variant, &raw, &mut again).ok()?;
    (again == run.as_bytes()).then_some((variant, raw))
}

/// Shannon entropy of the byte distribution, in bits per byte.
fn entropy_bits(bytes: &[u8]) -> f64 {
    if bytes.is_empty() {
        return 0.0;
    }
    let mut counts = [0u32; 256];
    for &b in bytes {
        counts[b as usize] += 1;
    }
    let n = bytes.len() as f64;
    counts.iter().filter(|&&c| c > 0).map(|&c| f64::from(c) / n).map(|p| -p * p.log2()).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dictionary::{Dictionary, ENTRY_COST_BYTES};
    use crate::splitter::{DefaultSplitter, JsonEscapeSplitter};
    use proptest::prelude::*;

    fn tokenizer(max_entries: usize) -> Tokenizer {
        let words = WordStore::new(MemHeap::new(), Dictionary::new(max_entries * ENTRY_COST_BYTES));
        Tokenizer::new(words.into_shared(), TokenizerOptions::default())
    }

    fn encode(t: &Tokenizer, text: &str, splitter: &dyn Splitter) -> BytesMut {
        let mut out = BytesMut::new();
        t.encode(text, splitter, &mut out).unwrap();
        out
    }

    fn word_count(t: &Tokenizer) -> u32 {
        t.words().read().unwrap().word_count()
    }

    /// Pseudo-random bytes without pulling in a random number crate.
    fn noise(len: usize, mut seed: u64) -> Vec<u8> {
        (0..len)
            .map(|_| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (seed >> 33) as u8
            })
            .collect()
    }

    #[test]
    fn plain_text_round_trips_and_deduplicates() {
        let t = tokenizer(100);
        let text = "the quick brown fox, the quick brown dog";
        let stream = encode(&t, text, &DefaultSplitter);
        assert_eq!(t.decode_string(&stream).unwrap(), text);
        // the, quick, brown, fox, dog (single characters and repeated words are not new words)
        assert_eq!(word_count(&t), 5);
        assert!(stream.len() < text.len());
    }

    #[test]
    fn empty_string_and_unicode() {
        let t = tokenizer(100);
        for text in ["", " ", "héllo wörld — 日本語 😀", "tab\tnew\nline\r\n"] {
            assert_eq!(t.decode_string(&encode(&t, text, &DefaultSplitter)).unwrap(), text);
        }
    }

    #[test]
    fn uuid_is_stored_as_16_bytes_and_never_enters_the_dictionary() {
        let t = tokenizer(100);
        for uuid in ["28cb7d12-4175-485f-8b91-02be9c532039", "28CB7D12-4175-485F-8B91-02BE9C532039"] {
            let text = format!("session {uuid} ended");
            let stream = encode(&t, &text, &DefaultSplitter);
            assert_eq!(t.decode_string(&stream).unwrap(), text);
        }
        assert_eq!(word_count(&t), 2); // "session", "ended"
        // mixed case is not a canonical UUID: it falls back to ordinary words
        let mixed = "28cb7d12-4175-485F-8b91-02be9c532039";
        assert_eq!(t.decode_string(&encode(&t, mixed, &DefaultSplitter)).unwrap(), mixed);
        assert!(word_count(&t) > 2);
        // a UUID glued to letters is not a UUID
        let glued = "x28cb7d12-4175-485f-8b91-02be9c532039";
        assert_eq!(t.decode_string(&encode(&t, glued, &DefaultSplitter)).unwrap(), glued);
    }

    #[test]
    fn hex_runs_are_stored_as_bytes() {
        let t = tokenizer(100);
        for hex in ["4608ae6d1f0c9b2a", "4608AE6D1F0C9B2A3C5E", "0123456789012345"] {
            let stream = encode(&t, hex, &DefaultSplitter);
            assert_eq!(t.decode_string(&stream).unwrap(), hex);
            assert!(stream.len() <= hex.len() / 2 + 3);
        }
        assert_eq!(word_count(&t), 0);
        // mixed-case hex, odd length and short runs are ordinary words
        for text in ["4608aE6d1f0c9b2a", "4608ae6d1f0c9b2a1", "4608ae6"] {
            assert_eq!(t.decode_string(&encode(&t, text, &DefaultSplitter)).unwrap(), text);
        }
        assert_eq!(word_count(&t), 3);
    }

    #[test]
    fn base64_in_all_four_variants_round_trips_as_bytes() {
        let t = tokenizer(1000);
        let raw = noise(300, 7);
        let forms = [
            STANDARD.encode(&raw),
            STANDARD_NO_PAD.encode(&raw),
            URL_SAFE.encode(&raw),
            URL_SAFE_NO_PAD.encode(&raw),
            STANDARD.encode(&raw[..299]),
        ];
        for form in forms {
            let text = format!("data:image/png;base64,{form} end");
            let stream = encode(&t, &text, &DefaultSplitter);
            assert_eq!(t.decode_string(&stream).unwrap(), text);
            assert!(stream.len() < text.len() * 8 / 10, "{} vs {}", stream.len(), text.len());
        }
        // only "data", "image", "png", "base64" and "end" are words
        assert_eq!(word_count(&t), 5);
    }

    #[test]
    fn non_canonical_base64_falls_back_to_words() {
        let t = tokenizer(1000);
        let mut text = STANDARD_NO_PAD.encode(noise(100, 3)); // 134 characters, canonical
        text.pop();
        text.push('B'); // 133 characters; the last character carries non-zero trailing bits
        assert_eq!(t.decode_string(&encode(&t, &text, &DefaultSplitter)).unwrap(), text);
        assert!(word_count(&t) > 0);
    }

    #[test]
    fn long_plain_text_without_spaces_is_not_mistaken_for_base64() {
        let t = tokenizer(1000);
        let text = "src/components/ui/forms/inputs/text_field/validation/rules/required-field-checks/index";
        assert_eq!(t.decode_string(&encode(&t, text, &DefaultSplitter)).unwrap(), text);
        assert!(word_count(&t) > 5);
    }

    #[test]
    fn opaque_runs_skip_the_dictionary_unless_already_known() {
        let t = tokenizer(100);
        let id = "toolu_01UAgYH3kvbEkEaocV5KPN7N";
        let stream = encode(&t, id, &DefaultSplitter);
        assert_eq!(t.decode_string(&stream).unwrap(), id);
        assert_eq!(word_count(&t), 1); // only "toolu"; the 24-character id is inline
        // a known word is always an ordinal, even if it looks random
        let known = "01UAgYH3kvbEkEaocV5KPN7N";
        t.words().write().unwrap().intern(known).unwrap();
        let before = word_count(&t);
        let stream = encode(&t, known, &DefaultSplitter);
        assert_eq!((word_count(&t), stream.len()), (before, 2));
        assert_eq!(t.decode_string(&stream).unwrap(), known);
    }

    #[test]
    fn json_escape_splitter_keeps_words_clean() {
        let t = tokenizer(100);
        let text = r#"{"text":"line one\nline two\n\"quoted\" é"}"#;
        for splitter in [&DefaultSplitter as &dyn Splitter, &JsonEscapeSplitter] {
            assert_eq!(t.decode_string(&encode(&t, text, splitter)).unwrap(), text);
        }
        let t2 = tokenizer(100);
        encode(&t2, text, &JsonEscapeSplitter);
        let words = t2.words().read().unwrap();
        let all: Vec<_> = (0..words.word_count()).map(|i| words.get(i).unwrap().to_string()).collect();
        assert!(all.contains(&"\\n".to_string()) && !all.iter().any(|w| w.starts_with("nline")), "{all:?}");
    }

    #[test]
    fn a_failed_encode_leaves_no_words_behind() {
        let t = tokenizer(2);
        assert!(matches!(t.encode("aa bb cc", &DefaultSplitter, &mut BytesMut::new()), Err(Error::DictionaryFull)));
        assert_eq!(word_count(&t), 0);
        assert!(t.encode("aa bb", &DefaultSplitter, &mut BytesMut::new()).is_ok());
    }

    #[test]
    fn corrupt_streams_are_errors_not_panics() {
        let t = tokenizer(100);
        let stream = encode(&t, "hello 8f14e45fceea167a5a36dedd4bea2543 world", &DefaultSplitter);
        for cut in 0..stream.len() {
            let _ = t.decode_string(&stream[..cut]); // must not panic
        }
        let mut unknown = vec![1];
        varint::put(&mut unknown, 99 << 2); // word ordinal 99 does not exist
        assert!(t.decode_string(&unknown).is_err());
    }

    fn piece() -> impl Strategy<Value = String> {
        prop_oneof![
            "[a-zA-Z0-9]{1,30}",
            "[a-zA-Z0-9]{60,150}",
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            "[0-9a-fA-F]{16,40}",
            prop::collection::vec(any::<u8>(), 40..200).prop_map(|b| STANDARD.encode(b)),
            prop::collection::vec(any::<u8>(), 40..200).prop_map(|b| URL_SAFE_NO_PAD.encode(b)),
            r"\\[a-z\\\x22]|\\u[0-9a-f]{4}",
            " |,|:|\n|\t|-|_|/|\\+|=|é|日",
            ".{0,10}",
        ]
    }

    proptest! {
        #[test]
        fn encode_decode_round_trips(parts in prop::collection::vec(piece(), 0..12)) {
            let text = parts.concat();
            for splitter in [&DefaultSplitter as &dyn Splitter, &JsonEscapeSplitter] {
                let t = tokenizer(10_000);
                let stream = encode(&t, &text, splitter);
                prop_assert_eq!(t.decode_string(&stream).unwrap(), text.clone());
                // a second encode of the same text adds no words and gives the same stream
                let count = word_count(&t);
                prop_assert_eq!(encode(&t, &text, splitter), stream);
                prop_assert_eq!(word_count(&t), count);
            }
        }
    }
}
