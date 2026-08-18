//! Heads-up display of in-flight client drivers.
//!
//! [`RunCounterLayer`] tracks open `driver.run` spans in a pair of atomics.
//! Span *lifetime* is the signal: `#[instrument]` on an `async fn` opens the
//! span when the future is created and closes it when the future is dropped,
//! so an open span is exactly one client run still in flight. (Entered/exited
//! would instead count polls.)
//!
//! An [`indicatif`] spinner reads those atomics at draw time, so nothing has
//! to push updates to it and the span hooks stay on relaxed atomics rather
//! than taking the spinner's lock. Log output is routed through [`HudWriter`],
//! which suspends the spinner around each line so the two never fight over
//! the terminal.
use std::{
    fmt,
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use tracing::{Subscriber, span};
use tracing_subscriber::{
    Layer,
    filter::{FilterFn, LevelFilter, filter_fn},
    fmt::MakeWriter,
    layer::{Context, Filter},
    registry::LookupSpan,
};

/// Name of the span whose lifetime marks one in-flight client run.
///
/// Must match the `name` given to `#[tracing::instrument]` on
/// [`crate::driver::ClientDriver::run`].
pub const DRIVER_SPAN: &str = "driver.run";

/// Level of [`DRIVER_SPAN`], needed so the layer's filter can advertise an
/// accurate max-level hint.
const DRIVER_SPAN_LEVEL: LevelFilter = LevelFilter::DEBUG;

/// How often the HUD repaints.
const REFRESH: Duration = Duration::from_millis(250);

/// Shared tallies of driver runs, written by [`RunCounterLayer`].
#[derive(Clone, Debug, Default)]
pub struct RunCounter {
    active: Arc<AtomicUsize>,
    finished: Arc<AtomicUsize>,
}

impl RunCounter {
    /// Number of client runs currently in flight.
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    /// Number of client runs that have completed.
    pub fn finished(&self) -> usize {
        self.finished.load(Ordering::Relaxed)
    }
}

/// A [`Layer`] that tracks open [`DRIVER_SPAN`] spans in a [`RunCounter`].
#[derive(Clone, Debug, Default)]
pub struct RunCounterLayer {
    counter: RunCounter,
}

impl RunCounterLayer {
    pub fn new(counter: RunCounter) -> Self {
        Self { counter }
    }

    /// Filter restricting this layer to just the driver span.
    ///
    /// The max-level hint matters: without it the filter claims no opinion,
    /// which forces the global static max level to `TRACE` and makes every
    /// callsite in the program do runtime work.
    fn filter() -> FilterFn<impl Fn(&tracing::Metadata<'_>) -> bool + Clone> {
        filter_fn(|meta: &tracing::Metadata<'_>| meta.is_span() && meta.name() == DRIVER_SPAN)
            .with_max_level_hint(DRIVER_SPAN_LEVEL)
    }

    /// This layer paired with its filter, ready to hand to `.with(..)`.
    fn filtered<S>(counter: RunCounter) -> impl Layer<S>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let filter: Box<dyn Filter<S> + Send + Sync> = Box::new(Self::filter());
        Self::new(counter).with_filter(filter)
    }
}

impl<S> Layer<S> for RunCounterLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, _id: &span::Id, _ctx: Context<'_, S>) {
        // The per-layer filter already narrowed this to DRIVER_SPAN, but check
        // anyway so the layer is still correct if installed unfiltered.
        if attrs.metadata().name() == DRIVER_SPAN {
            self.counter.active.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        // Span data is still in the registry during on_close, so we can look
        // the metadata back up to confirm which span is closing.
        if ctx.metadata(&id).is_some_and(|m| m.name() == DRIVER_SPAN) {
            self.counter.active.fetch_sub(1, Ordering::Relaxed);
            self.counter.finished.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The heads-up display: a counter, and the spinner that renders it.
///
/// Construct before installing the subscriber, since both the counting layer
/// and the log writer are derived from it. The elapsed clock starts here, so
/// it covers startup work (notably parsing the trace file) as well as the run.
#[derive(Clone, Debug)]
pub struct Hud {
    counter: RunCounter,
    bar: ProgressBar,
    enabled: bool,
}

impl Hud {
    /// Build a HUD. When `enabled` is false the spinner is hidden and the
    /// counting layer is never installed, leaving the hot path untouched.
    pub fn new(enabled: bool) -> Self {
        let counter = RunCounter::default();
        let bar = if enabled {
            let bar = ProgressBar::new_spinner().with_style(Self::style(&counter));
            // indicatif owns the repaint thread; the style pulls fresh values
            // from the atomics on each tick, so nothing needs to push updates.
            bar.enable_steady_tick(REFRESH);
            bar
        } else {
            ProgressBar::hidden()
        };
        Self {
            counter,
            bar,
            enabled,
        }
    }

    /// Spinner template, with the live values supplied by custom keys.
    ///
    /// Widths are applied inside each closure rather than in the template so
    /// the columns stay put without relying on template padding syntax.
    fn style(counter: &RunCounter) -> ProgressStyle {
        let active = counter.clone();
        let finished = counter.clone();
        let rate = counter.clone();
        ProgressStyle::with_template(
            "{spinner} [{elapsed_precise}] active clients: {active}  finished: {finished} ({rate})",
        )
        .expect("HUD template is valid")
        .with_key(
            "active",
            move |_: &ProgressState, w: &mut dyn fmt::Write| {
                let _ = write!(w, "{:>6}", active.active());
            },
        )
        .with_key(
            "finished",
            move |_: &ProgressState, w: &mut dyn fmt::Write| {
                let _ = write!(w, "{:>7}", finished.finished());
            },
        )
        .with_key(
            "rate",
            move |state: &ProgressState, w: &mut dyn fmt::Write| {
                let secs = state.elapsed().as_secs_f64();
                let per_sec = if secs > 0.0 {
                    rate.finished() as f64 / secs
                } else {
                    0.0
                };
                let _ = write!(w, "{per_sec:>7.1}/s");
            },
        )
    }

    /// The counting layer, or `None` when the HUD is disabled.
    ///
    /// An absent layer hints `LevelFilter::OFF`, so the global static max
    /// level is left alone and DEBUG callsites stay compiled out of the hot
    /// path.
    pub fn layer<S>(&self) -> Option<impl Layer<S>>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        self.enabled
            .then(|| RunCounterLayer::filtered(self.counter.clone()))
    }

    /// A writer for the `fmt` layer that keeps log lines off the spinner.
    pub fn writer(&self) -> HudWriter {
        HudWriter {
            bar: self.bar.clone(),
        }
    }

    /// Stop the spinner and clear its line.
    ///
    /// Safe to call whether or not the HUD is enabled, and safe to call on
    /// the error path, so callers can bracket the whole run with it.
    pub fn finish(&self) {
        self.bar.finish_and_clear();
    }
}

/// [`MakeWriter`] that emits each log line with the spinner suspended.
///
/// `suspend` clears the spinner, runs the write, and redraws, so log lines
/// scroll normally while the HUD stays pinned to the bottom.
#[derive(Clone, Debug)]
pub struct HudWriter {
    bar: ProgressBar,
}

impl<'a> MakeWriter<'a> for HudWriter {
    type Writer = HudLine;

    fn make_writer(&'a self) -> Self::Writer {
        HudLine {
            bar: self.bar.clone(),
            buf: Vec::new(),
        }
    }
}

/// One buffered log line. The `fmt` layer may write a line in several pieces,
/// so the bytes are accumulated and emitted once, on drop.
#[derive(Debug)]
pub struct HudLine {
    bar: ProgressBar,
    buf: Vec<u8>,
}

impl HudLine {
    fn emit(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let buf = std::mem::take(&mut self.buf);
        self.bar.suspend(|| {
            let mut err = io::stderr().lock();
            err.write_all(&buf)?;
            err.flush()
        })
    }
}

impl Write for HudLine {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit()
    }
}

impl Drop for HudLine {
    fn drop(&mut self) {
        let _ = self.emit();
    }
}
