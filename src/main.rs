#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use std::fmt::Display;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing_appender::rolling::Rotation;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, reload};

use crate::accounts::{AccountId, AccountSummary};
use crate::player_data::ExportSettings;

mod accounts;
mod activity;
mod admin;
mod app;
mod capture;
mod decoder_events;
mod good;
mod monitor;
mod player_data;
mod update;
mod wish;

const APP_ID: &str = "Irminsul";

#[derive(Clone, Copy, Debug)]
pub enum ConfirmationType {
    Initial,
    Update,
}

#[derive(Clone, Debug)]
pub enum State {
    Starting,
    CheckingForUpdate,
    WaitingForUpdateConfirmation(String),
    Updating,
    /// `needs_caps` is set when the update dropped the packet capture
    /// permissions the user had granted, and they need to be re-granted.
    Updated {
        needs_caps: bool,
    },
    CheckingForData,
    WaitingForDownloadConfirmation(ConfirmationType),
    Downloading,
    Main,
}

#[derive(Debug)]
pub enum Message {
    UpdateAcknowledged,
    UpdateCanceled,
    DownloadAcknowledged,
    StartCapture,
    StopCapture,
    ExportGenshinOptimizer(AccountId, ExportSettings, oneshot::Sender<Result<String>>),
    ForgetAccount(AccountId),
}

/// How far along decoding the game's connection is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkStatus {
    Stopped,
    WaitingForGame,
    /// Genshin was already connected when capture started.
    MissedLogin,
    Connecting,
    LoggingIn,
    Decrypting,
    DecryptFailed,
    CaptureFailed,
}

#[derive(Clone, Debug)]
pub struct AppState {
    state: State,
    capturing: bool,
    link: LinkStatus,
    accounts: Vec<AccountSummary>,
    /// The account that most recently received captured data, and when.
    last_capture: Option<(AccountId, DateTime<Local>)>,
}

impl AppState {
    fn new() -> Self {
        AppState {
            state: State::Starting,
            capturing: false,
            link: LinkStatus::Stopped,
            accounts: Vec::new(),
            last_capture: None,
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = false)]
    no_admin: bool,

    #[arg(
        long = "capture-backend",
        value_enum,
        default_value_t = capture::DEFAULT_CAPTURE_BACKEND_TYPE
    )]
    capture_backend: capture::BackendType,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, Default)]
pub enum TracingLevel {
    #[default]
    Default,
    VerboseInfo,
    VerboseDebug,
    VerboseTrace,
}

impl Display for TracingLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TracingLevel::Default => write!(f, "Default"),
            TracingLevel::VerboseInfo => write!(f, "Verbose Info"),
            TracingLevel::VerboseDebug => write!(f, "Verbose Debug"),
            TracingLevel::VerboseTrace => write!(f, "Verbose Trace"),
        }
    }
}

impl TracingLevel {
    fn get_filter(&self) -> &'static str {
        match self {
            TracingLevel::Default => {
                if cfg!(debug_assertions) {
                    "info"
                } else {
                    "warn,irminsul=info"
                }
            }
            TracingLevel::VerboseInfo => "info",
            TracingLevel::VerboseDebug => "debug",
            TracingLevel::VerboseTrace => "trace",
        }
    }
}

struct ReloadHandle(reload::Handle<EnvFilter, tracing_subscriber::Registry>);

impl ReloadHandle {
    pub fn set_filter(&mut self, filter: &str) {
        if let Err(e) = self.0.reload(filter) {
            tracing::warn!("Failed to set tracing filter to \"{filter}\": {e}");
        }
        tracing::info!("Set tracing filter to \"{filter}\"");
    }
}

fn main() -> eframe::Result {
    let (_guard, reload_handle) = tracing_init().unwrap();

    let args = Args::parse();

    if !args.no_admin {
        #[cfg(any(windows, unix))]
        admin::ensure_admin();
    }

    let capture_backend = args.capture_backend;

    let background_image_size = [1600., 1000.];

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size(background_image_size.map(|v| v * 0.5))
            .with_resizable(false)
            .with_decorations(false)
            .with_icon(
                // NOTE: Adding an icon is optional
                eframe::icon_data::from_png_bytes(&include_bytes!("../assets/icon-256.png")[..])
                    .expect("Failed to load icon"),
            ),
        persist_window: false,
        ..Default::default()
    };
    eframe::run_native(
        "Irminsul",
        native_options,
        Box::new(move |cc| {
            Ok(Box::new(app::IrminsulApp::new(
                cc,
                reload_handle,
                capture_backend,
            )))
        }),
    )
}

fn log_dir() -> Result<PathBuf> {
    let mut dir = eframe::storage_dir(APP_ID).context("Storage dir not found")?;
    dir.push("log");
    println!("Log folder: {}", dir.display());
    Ok(dir)
}

fn open_log_dir() -> Result<()> {
    let dir = log_dir()?;
    open::that(dir)?;
    Ok(())
}

fn tracing_init() -> Result<(tracing_appender::non_blocking::WorkerGuard, ReloadHandle)> {
    let appender = tracing_appender::rolling::Builder::new()
        .filename_prefix("log")
        .rotation(Rotation::DAILY)
        .max_log_files(7)
        .build(log_dir()?)?;
    let (non_blocking_appender, guard) = tracing_appender::non_blocking(appender);

    let filter = EnvFilter::new(TracingLevel::default().get_filter());
    let (filter, reload_handle) = reload::Layer::new(filter);
    // The level filter applies to the log file only, so the decoder events
    // the activity panel relies on are seen whatever the log level is.
    let writer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking_appender)
        .with_ansi(false)
        .with_filter(filter);
    tracing_subscriber::registry()
        .with(writer)
        .with(decoder_events::layer())
        .init();
    tracing::info!("Tracing initialized and logging to file.");

    Ok((guard, ReloadHandle(reload_handle)))
}
