use serde::Deserialize;
use std::{fs::File, io::BufReader};

#[derive(Clone, Debug)]
pub struct ByteRange {
    offset: usize,
    size: usize,
}

impl ByteRange {
    /// Calculate start and (inclusive) end byte positions for the range, given the total content length.
    pub fn start_end(&self, content_length: usize) -> (usize, usize) {
        let end = (self.offset + self.size - 1).min(content_length - 1);
        (self.offset, end)
    }
}

#[derive(Clone, Debug)]
pub enum Range {
    Single(ByteRange),
    Multi(Vec<ByteRange>),
}

impl Range {
    /// Convert the range into a string suitable for use in an HTTP Range header.
    ///
    /// The `content_length` parameter is used to trim ranges to the end of the content if they exceed it.
    pub fn to_header_value(&self, content_length: usize) -> String {
        match self {
            Range::Single(br) => {
                let (start, end) = br.start_end(content_length);
                format!("bytes={}-{}", start, end)
            }
            Range::Multi(brs) => {
                let ranges: Vec<String> = brs
                    .iter()
                    .filter(|r| r.offset < content_length)
                    .map(|br| {
                        let (start, end) = br.start_end(content_length);
                        format!("{}-{}", start, end)
                    })
                    .collect();
                format!("bytes={}", ranges.join(","))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum Action {
    /// Request some data
    Request(Range),
    /// Wait for some time before the next request
    Sleep(u64),
}

#[derive(Clone, Debug)]
pub struct Trace {
    actions: Vec<Action>,
}

impl Trace {
    /// Iterator over the actions in the trace.
    pub fn actions(&self) -> impl Iterator<Item = &Action> + '_ {
        self.actions.iter()
    }
}

#[derive(Clone, Debug, Deserialize)]
struct RawTraceEntry {
    vector: bool,
    chunks: Vec<(usize, usize)>,
    start_ms: u64,
    end_ms: u64,
}

impl Trace {
    /// Read in a trace from a JSON file.
    ///
    /// The expected format of the file is:
    /// ```json
    /// [
    ///     {
    ///         "vector":false,
    ///         "chunks":[[427385177,458513]],
    ///         "start_ms":31743,
    ///         "end_ms":31849
    ///     },
    ///     {
    ///         "vector":true,
    ///         "chunks":[[341807302,3882],[348213079,3854],[354581176,3902]],
    ///         "start_ms":32144,
    ///         "end_ms":32297
    ///     }
    /// ]
    /// ```
    pub fn read_json(path: &std::path::Path) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        // Auto-detect gzip compression based on file extension
        let raw: Vec<RawTraceEntry> = if path.extension().is_some_and(|ext| ext == "gz") {
            let decoder = flate2::read::GzDecoder::new(file);
            let reader = BufReader::new(decoder);
            serde_json::from_reader(reader)?
        } else {
            let reader = BufReader::new(file);
            serde_json::from_reader(reader)?
        };

        let mut actions = Vec::with_capacity(raw.len() * 2 - 1);
        let mut prev_end = None;
        // TODO: enforce that the trace is sorted by start_ms and that there are no overlapping requests
        for entry in raw {
            if let Some(end) = prev_end.replace(entry.end_ms) {
                let sleep_duration = entry.start_ms.saturating_sub(end);
                actions.push(Action::Sleep(sleep_duration));
            }

            let request = match entry.vector {
                true => Action::Request(Range::Multi(
                    entry
                        .chunks
                        .into_iter()
                        .map(|(offset, size)| ByteRange { offset, size })
                        .collect(),
                )),
                false => {
                    let (offset, size) = entry.chunks.into_iter().next().ok_or_else(|| {
                        anyhow::anyhow!("Expected at least one chunk for non-vector request")
                    })?;
                    Action::Request(Range::Single(ByteRange { offset, size }))
                }
            };
            actions.push(request);
        }
        Ok(Trace { actions })
    }
}
