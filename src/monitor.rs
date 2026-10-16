use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anime_game_data::AnimeGameData;
use anyhow::{Context, Result};
use auto_artifactarium::{
    ConnectionPacket, GameCommand, GamePacket, GameSniffer, matches_achievement_packet,
    matches_avatar_packet, matches_item_packet,
};
use base64::prelude::*;
use chrono::prelude::*;
use flate2::read::GzDecoder;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::accounts::{self, AccountId, AccountSummary, Accounts, Captured, DataKind};
use crate::activity::{self, Level, format_count};
use crate::capture::{BackendType, CaptureError, create_capture};
use crate::decoder_events::{self, DecoderEvent};
use crate::player_data::Achievement;
use crate::{APP_ID, AppState, LinkStatus, Message, State};

/// How long game traffic may flow without a connection handshake before
/// concluding that Genshin logged in before capture started.
const MISSED_LOGIN_GRACE: Duration = Duration::from_secs(5);

const RELAUNCH_HINT: &str =
    "Keep capture running, close Genshin completely, then launch it and log in again.";

struct AppStateManager {
    app_state: AppState,
    state_tx: watch::Sender<AppState>,
}

impl AppStateManager {
    fn new(app_state: AppState, state_tx: watch::Sender<AppState>) -> Self {
        Self {
            app_state,
            state_tx,
        }
    }

    pub fn update_app_state(&mut self, state: State) {
        self.app_state.state = state;
        let _ = self.state_tx.send(self.app_state.clone());
    }

    pub fn update_capturing_state(&mut self, capturing: bool) {
        self.app_state.capturing = capturing;
        let _ = self.state_tx.send(self.app_state.clone());
    }

    pub fn update_link(&mut self, link: LinkStatus) {
        if self.app_state.link != link {
            self.app_state.link = link;
            let _ = self.state_tx.send(self.app_state.clone());
        }
    }

    pub fn update_accounts(
        &mut self,
        accounts: Vec<AccountSummary>,
        last_capture: Option<(AccountId, DateTime<Local>)>,
    ) {
        self.app_state.accounts = accounts;
        if last_capture.is_some() {
            self.app_state.last_capture = last_capture;
        }
        let _ = self.state_tx.send(self.app_state.clone());
    }
}

struct CaptureEvent {
    /// Which capture run this event belongs to, so that events from a
    /// stopped run are ignored.
    run: u32,
    kind: CaptureEventKind,
}

enum CaptureEventKind {
    Started,
    Failed(String),
}

/// What has been seen of the current packet capture run.
#[derive(Default)]
struct CaptureStats {
    first_packet_at: Option<Instant>,
    saw_handshake: bool,
    warned_missed_login: bool,
}

/// What has been seen of the game's current connection to the server.
#[derive(Default)]
struct Session {
    id: u32,
    connected: bool,
    initial_key_reported: bool,
    login_token_seen: bool,
    decrypting: bool,
    decrypt_failed: bool,
    reported_errors: HashSet<String>,
    account: Option<AccountId>,
    /// Achievements don't identify their account, so they wait here when
    /// they arrive before data that does.
    pending_achievements: Option<(Vec<Achievement>, Vec<u8>)>,
    captured: HashSet<DataKind>,
    announced_ready: bool,
}

pub struct Monitor {
    app_state: AppStateManager,
    ui_message_rx: mpsc::UnboundedReceiver<Message>,
    log_packet_rx: watch::Receiver<bool>,
    accounts: Accounts,
    sniffer: GameSniffer,
    capture_cancel_token: Option<CancellationToken>,
    capture_run: u32,
    capture_stats: CaptureStats,
    packet_tx: mpsc::UnboundedSender<Vec<u8>>,
    packet_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    capture_event_tx: mpsc::UnboundedSender<CaptureEvent>,
    capture_event_rx: mpsc::UnboundedReceiver<CaptureEvent>,
    capture_backend: BackendType,
    session: Session,
    sessions_started: u32,
}

impl Monitor {
    pub async fn new(
        state_tx: watch::Sender<AppState>,
        mut ui_message_rx: mpsc::UnboundedReceiver<Message>,
        log_packet_rx: watch::Receiver<bool>,
        capture_backend: BackendType,
    ) -> Result<Self> {
        let mut app_state = AppStateManager::new(state_tx.borrow().clone(), state_tx.clone());
        let game_data = get_database(&mut app_state, &mut ui_message_rx).await?;

        let mut accounts = Accounts::new(Arc::new(game_data), accounts_dir());
        let (loaded, errors) = accounts.load_saved();
        for error in errors {
            activity::warning(error);
        }
        for summary in loaded.into_iter().filter_map(|id| accounts.summary(id)) {
            let captured_at = summary
                .last_captured()
                .map(|time| format!(" (captured {})", time.format("%b %-d, %H:%M")))
                .unwrap_or_default();
            activity::info(format!("Loaded saved data for {}{captured_at}", summary.id));
        }
        app_state.update_accounts(accounts.summaries(), None);

        let keys = load_keys()?;
        let sniffer = GameSniffer::new().set_initial_keys(keys);
        let (packet_tx, packet_rx) = mpsc::unbounded_channel();
        let (capture_event_tx, capture_event_rx) = mpsc::unbounded_channel();

        Ok(Self {
            app_state,
            accounts,
            ui_message_rx,
            log_packet_rx,
            sniffer,
            capture_cancel_token: None,
            capture_run: 0,
            capture_stats: CaptureStats::default(),
            packet_tx,
            packet_rx,
            capture_event_tx,
            capture_event_rx,
            capture_backend,
            session: Session::default(),
            sessions_started: 0,
        })
    }

    pub async fn run(mut self) {
        self.app_state.update_app_state(State::Main);
        activity::info("Ready. Start packet capture, then launch Genshin and log in.");

        loop {
            #[rustfmt::skip]
                tokio::select! {
                    Some(packet) = self.packet_rx.recv() => self.handle_packet(packet),
                    Some(msg) = self.ui_message_rx.recv() => self.handle_ui_msg(msg),
                    Some(event) = self.capture_event_rx.recv() => self.handle_capture_event(event),
                }
        }
    }

    fn handle_ui_msg(&mut self, msg: Message) {
        match msg {
            Message::StartCapture => self.start_capture(),
            Message::StopCapture => self.stop_capture(),
            Message::ExportGenshinOptimizer(id, settings, reply_tx) => {
                let _ = reply_tx.send(self.accounts.export_genshin_optimizer(id, &settings));
            }
            Message::ForgetAccount(id) => {
                match self.accounts.forget(id) {
                    Ok(()) => activity::info(format!("Forgot the captured data for {id}")),
                    Err(e) => activity::error(format!("Couldn't forget {id}: {e:#}")),
                }
                self.app_state
                    .update_accounts(self.accounts.summaries(), None);
            }
            _ => (),
        }
    }

    fn start_capture(&mut self) {
        if self.capture_cancel_token.is_some() {
            tracing::warn!("Capture start request while already capturing");
            return;
        }

        self.capture_run += 1;
        self.capture_stats = CaptureStats::default();

        let cancel_token = CancellationToken::new();
        tokio::spawn(capture_task(
            self.capture_run,
            cancel_token.clone(),
            self.packet_tx.clone(),
            self.capture_event_tx.clone(),
            self.capture_backend,
        ));
        self.capture_cancel_token = Some(cancel_token);
        self.app_state.update_capturing_state(true);
        self.set_link(LinkStatus::WaitingForGame);
    }

    fn stop_capture(&mut self) {
        let Some(cancel_token) = self.capture_cancel_token.take() else {
            tracing::warn!("Capture stop request with no current cancel token");
            return;
        };
        cancel_token.cancel();
        self.app_state.update_capturing_state(false);
        self.set_link(LinkStatus::Stopped);
        activity::info("Packet capture stopped");
    }

    fn handle_capture_event(&mut self, event: CaptureEvent) {
        if event.run != self.capture_run || self.capture_cancel_token.is_none() {
            return;
        }

        match event.kind {
            CaptureEventKind::Started => {
                activity::info("Packet capture is running. Launch Genshin and log in.")
            }
            CaptureEventKind::Failed(error) => {
                self.capture_cancel_token = None;
                self.app_state.update_capturing_state(false);
                self.set_link(LinkStatus::CaptureFailed);
                activity::log(
                    Level::Error,
                    format!("Packet capture failed: {error}"),
                    Some(capture_failed_hint(self.capture_backend)),
                );
            }
        }
    }

    /// The link status only changes while capturing, so that packets still
    /// queued when capture stops don't make it look like it's running.
    fn set_link(&mut self, link: LinkStatus) {
        let capturing = self.capture_cancel_token.is_some();
        if capturing || matches!(link, LinkStatus::Stopped | LinkStatus::CaptureFailed) {
            self.app_state.update_link(link);
        }
    }

    fn handle_packet(&mut self, packet: Vec<u8>) {
        self.note_traffic();

        let result = self.sniffer.receive_packet(packet);

        let events = decoder_events::drain();
        let login_token_in_packet = events.contains(&DecoderEvent::LoginTokenSeen);
        for event in events {
            self.handle_decoder_event(event);
        }

        match result {
            Some(GamePacket::Connection(ConnectionPacket::HandshakeRequested)) => {
                self.on_handshake()
            }
            Some(GamePacket::Connection(ConnectionPacket::Disconnected)) => self.on_disconnected(),
            Some(GamePacket::Commands(commands)) if !commands.is_empty() => {
                // Commands decoded after the login token can only have been
                // decrypted with the session key.
                if self.session.login_token_seen && !login_token_in_packet {
                    if self.session.decrypting {
                        self.set_link(LinkStatus::Decrypting);
                    } else {
                        self.on_decrypting();
                    }
                }
                self.handle_commands(commands);
            }
            _ => (),
        }
    }

    fn note_traffic(&mut self) {
        if self.capture_cancel_token.is_none() {
            return;
        }

        let now = Instant::now();
        let first_packet_at = *self.capture_stats.first_packet_at.get_or_insert_with(|| {
            activity::info("Seeing Genshin network traffic");
            now
        });

        if !self.capture_stats.saw_handshake
            && !self.capture_stats.warned_missed_login
            && !self.session.decrypting
            && now.duration_since(first_packet_at) >= MISSED_LOGIN_GRACE
        {
            self.capture_stats.warned_missed_login = true;
            activity::log(
                Level::Warning,
                "Genshin was already connected when capture started, so Irminsul missed the login",
                Some(RELAUNCH_HINT),
            );
            self.set_link(LinkStatus::MissedLogin);
        }
    }

    fn on_handshake(&mut self) {
        self.capture_stats.saw_handshake = true;

        // Handshakes are retried until the server answers, and those retries
        // are part of the same login.
        if self.session.connected
            && !self.session.initial_key_reported
            && !self.session.login_token_seen
        {
            return;
        }

        self.sessions_started += 1;
        self.session = Session {
            id: self.sessions_started,
            connected: true,
            ..Default::default()
        };
        activity::info("Genshin is connecting to the server");
        self.set_link(LinkStatus::Connecting);
    }

    fn on_disconnected(&mut self) {
        // The disconnect notice is sent several times.
        if !self.session.connected {
            return;
        }
        self.session.connected = false;
        activity::info("Genshin disconnected from the server (logged out or closed)");
        self.set_link(LinkStatus::WaitingForGame);
    }

    fn on_decrypting(&mut self) {
        self.session.decrypting = true;
        self.session.decrypt_failed = false;
        activity::success("Decrypting game traffic. Waiting for account data…");
        self.set_link(LinkStatus::Decrypting);
    }

    fn handle_decoder_event(&mut self, event: DecoderEvent) {
        match event {
            DecoderEvent::InitialKeyFound { version } => {
                if !self.session.initial_key_reported {
                    self.session.initial_key_reported = true;
                    activity::info(format!(
                        "Recognized the game's connection key (protocol {version})"
                    ));
                }
            }
            DecoderEvent::InitialKeyMissing { version } => {
                // Once decryption has failed, the decoder looks up keys with
                // packets that aren't part of the connection setup, so a miss
                // tells us nothing new.
                if self.session.initial_key_reported || self.session.decrypt_failed {
                    return;
                }
                self.session.initial_key_reported = true;
                self.session.decrypt_failed = true;
                if self.session.connected {
                    activity::log(
                        Level::Error,
                        format!("No decryption key for this game version (protocol {version})"),
                        Some(
                            "This Genshin version may not be supported yet. Check for an Irminsul update.",
                        ),
                    );
                } else {
                    activity::log(
                        Level::Error,
                        "Couldn't decrypt a connection that started before capture",
                        Some(RELAUNCH_HINT),
                    );
                }
                self.set_link(LinkStatus::DecryptFailed);
            }
            DecoderEvent::LoginTokenSeen => {
                if !self.session.login_token_seen {
                    self.session.login_token_seen = true;
                    activity::info("Login detected, working out the session key…");
                    self.set_link(LinkStatus::LoggingIn);
                }
            }
            DecoderEvent::SessionKeyNotFound => {
                if self.session.decrypt_failed {
                    return;
                }
                self.session.decrypt_failed = true;
                let hint = if self.session.login_token_seen {
                    "Genshin reuses key material from its first login after launch, and this \
                     Irminsul session didn't see that login (for example after switching \
                     accounts, or restarting Irminsul while Genshin was running). Keep capture \
                     running, close Genshin completely, then launch it and log in again."
                } else {
                    "Irminsul didn't see this login's key exchange. Keep capture running, close \
                     Genshin completely, then launch it and log in again."
                };
                activity::log(Level::Error, "Couldn't decrypt this login", Some(hint));
                self.set_link(LinkStatus::DecryptFailed);
            }
            DecoderEvent::SessionKeyLost => {
                if self.session.decrypt_failed {
                    return;
                }
                self.session.decrypting = false;
                self.session.decrypt_failed = true;
                activity::log(
                    Level::Error,
                    "Lost the session key partway through the connection",
                    Some(RELAUNCH_HINT),
                );
                self.set_link(LinkStatus::DecryptFailed);
            }
            DecoderEvent::OtherError(message) => {
                if self.session.reported_errors.insert(message.clone()) {
                    activity::warning(format!("Decoder error: {message}"));
                }
            }
        }
    }

    fn handle_commands(&mut self, commands: Vec<GameCommand>) {
        let log_packets = *self.log_packet_rx.borrow_and_update();
        let now = Local::now();

        for command in commands {
            let _span = tracing::info_span!("packet id {}", command.command_id);
            if log_packets {
                if let Err(e) = log_command(&command) {
                    tracing::info!("error logging command {e}");
                }
            }

            if let Some(items) = matches_item_packet(&command) {
                tracing::info!("Found item packet with {} items", items.len());
                let id = self.identify_account(items.iter().map(|item| item.guid));
                self.record(id, Captured::Items(items), command.proto_data, now);
            } else if let Some(avatars) = matches_avatar_packet(&command) {
                tracing::info!("Found avatar packet with {} avatars", avatars.len());
                let id = self.identify_account(avatars.iter().map(|avatar| avatar.guid));
                self.record(id, Captured::Characters(avatars), command.proto_data, now);
            } else if let Some(achievements) = matches_achievement_packet(&command) {
                tracing::info!(
                    "Found achievement packet with {} achievements",
                    achievements.len()
                );
                match self.session.account {
                    Some(id) => self.record(
                        id,
                        Captured::Achievements(achievements),
                        command.proto_data,
                        now,
                    ),
                    None => {
                        self.session.pending_achievements = Some((achievements, command.proto_data))
                    }
                }
            }
        }
    }

    /// Works out which account the current login belongs to from the GUIDs
    /// of its items or characters.
    fn identify_account(&mut self, guids: impl IntoIterator<Item = u64>) -> AccountId {
        if let Some(id) = self.session.account {
            return id;
        }

        let id = match accounts::uid_from_guids(guids) {
            Some(uid) => {
                activity::info(format!("Logged in as UID {uid}"));
                AccountId::Uid(uid)
            }
            None => {
                let id = AccountId::Session(self.session.id);
                activity::log(
                    Level::Warning,
                    format!("Couldn't detect the account's UID, so this login is kept as \"{id}\""),
                    Some(
                        "It can still be exported, but won't be saved for the next time Irminsul starts.",
                    ),
                );
                id
            }
        };
        self.session.account = Some(id);

        if let Some((achievements, proto_data)) = self.session.pending_achievements.take() {
            self.record(
                id,
                Captured::Achievements(achievements),
                proto_data,
                Local::now(),
            );
        }

        id
    }

    fn record(
        &mut self,
        id: AccountId,
        captured: Captured,
        proto_data: Vec<u8>,
        captured_at: DateTime<Local>,
    ) {
        let kind = captured.kind();
        let count = captured.count();

        if let Err(e) = self.accounts.record(id, captured, proto_data, captured_at) {
            activity::warning(format!("Couldn't save {id} to disk: {e:#}"));
        }
        activity::success(format!(
            "{id}: captured {} {}",
            format_count(count),
            kind.noun()
        ));
        self.app_state
            .update_accounts(self.accounts.summaries(), Some((id, captured_at)));

        self.session.captured.insert(kind);
        if !self.session.announced_ready
            && self.session.captured.contains(&DataKind::Items)
            && self.session.captured.contains(&DataKind::Characters)
        {
            self.session.announced_ready = true;
            activity::log(
                Level::Success,
                format!("{id} is ready to export to Genshin Optimizer"),
                Some(
                    "To capture another account, log out and switch accounts in Genshin while Irminsul stays open.",
                ),
            );
        }
    }
}

fn capture_failed_hint(backend: BackendType) -> &'static str {
    match backend {
        BackendType::Pktmon => {
            "Packet capture needs administrator rights. Start Irminsul without --no-admin and \
             accept the Windows prompt."
        }
        BackendType::Pcap => {
            "Make sure Npcap (Windows) or libpcap is installed and Irminsul is allowed to \
             capture packets."
        }
    }
}

fn accounts_dir() -> Option<PathBuf> {
    let dir = eframe::storage_dir(APP_ID).map(|dir| dir.join("accounts"));
    if dir.is_none() {
        activity::warning(
            "Couldn't find a folder to save captured accounts in. They'll be kept until Irminsul closes.",
        );
    }
    dir
}

async fn get_database(
    app_state: &mut AppStateManager,
    _ui_message_rx: &mut mpsc::UnboundedReceiver<Message>,
) -> Result<AnimeGameData> {
    app_state.update_app_state(State::CheckingForData);

    static DATABASE: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/game_data.gz"));
    let reader = GzDecoder::new(DATABASE);
    let db = anime_game_data::AnimeGameData::new_from_reader(reader)?;

    Ok(db)
}

async fn capture_task(
    run: u32,
    cancel_token: CancellationToken,
    packet_tx: mpsc::UnboundedSender<Vec<u8>>,
    event_tx: mpsc::UnboundedSender<CaptureEvent>,
    backend: BackendType,
) {
    let send_event = |kind| {
        let _ = event_tx.send(CaptureEvent { run, kind });
    };

    let mut capture = match create_capture(backend) {
        Ok(capture) => capture,
        Err(e) => {
            tracing::error!("Error creating packet capture using {backend:?}: {e}");
            send_event(CaptureEventKind::Failed(e.to_string()));
            return;
        }
    };
    tracing::info!("starting capture");
    send_event(CaptureEventKind::Started);

    loop {
        let packet = tokio::select!(
            packet = capture.next_packet() => packet,
            _ = cancel_token.cancelled() => break,
        );
        match packet {
            Ok(packet) => {
                if let Err(e) = packet_tx.send(packet) {
                    tracing::error!("Error sending captured packet to monitor: {e}");
                }
            }
            // The capture can't produce any more packets.
            Err(e @ (CaptureError::CaptureClosed | CaptureError::ChannelClosed)) => {
                tracing::error!("Packet capture ended: {e}");
                send_event(CaptureEventKind::Failed(e.to_string()));
                break;
            }
            Err(e) => tracing::error!("Error receiving packet: {e}"),
        }
    }
    tracing::info!("ending capture");
}

fn log_command(command: &GameCommand) -> Result<()> {
    let mut packet_log_path = eframe::storage_dir(APP_ID).context("Storage dir not found")?;
    packet_log_path.push("packet_log");
    fs::create_dir_all(&packet_log_path)?;

    let now = Local::now();
    packet_log_path.push(format!(
        "{}-{}.bin",
        now.format("%Y-%m-%d_%H-%M-%S%.f"),
        command.command_id
    ));

    let file = fs::File::create(&packet_log_path)
        .with_context(|| format!("can't create file {packet_log_path:?}"))?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&command.proto_data)?;

    Ok(())
}

fn load_keys() -> Result<HashMap<u16, Vec<u8>>> {
    let keys: HashMap<u16, String> = serde_json::from_slice(include_bytes!("../keys/gi.json"))?;

    keys.iter()
        .map(|(key, value)| -> Result<_, _> { Ok((*key, BASE64_STANDARD.decode(value)?)) })
        .collect::<Result<HashMap<_, _>>>()
}
