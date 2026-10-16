//! Captured player data, kept separately for each game account.
//!
//! The account is identified by the UID in the upper 32 bits of item and
//! character GUIDs.  When that can't be detected, the data is kept per login
//! session instead so that two accounts are never mixed together.
//!
//! Accounts with a known UID are saved to disk as the raw packet payloads they
//! were decoded from, and decoded again when Irminsul starts.

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anime_game_data::AnimeGameData;
use anyhow::{Context, Result, anyhow, bail};
use auto_artifactarium::{
    matches_achievement_all_data_notify, matches_avatars_all_data_notify,
    matches_items_all_data_notify,
};
use base64::prelude::*;
use chrono::{DateTime, Local, TimeZone};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::player_data::{Achievement, AvatarInfo, ExportSettings, Item, PlayerData};

/// Genshin UIDs have at least 9 digits.
const MIN_UID: u32 = 100_000_000;
/// Share of GUIDs that must agree on the owning UID for it to be trusted.
const MIN_UID_AGREEMENT: f64 = 0.9;

const SAVE_FORMAT: u32 = 1;
const SAVE_PREFIX: &str = "uid-";
const SAVE_SUFFIX: &str = ".json.gz";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AccountId {
    Uid(u32),
    /// The UID couldn't be detected, so the data belongs to the numbered
    /// login session it was captured in.
    Session(u32),
}

impl AccountId {
    /// Distinguishes the export files of different accounts.
    pub fn file_tag(&self) -> String {
        match self {
            AccountId::Uid(uid) => format!("uid{uid}"),
            AccountId::Session(session) => format!("login{session}"),
        }
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccountId::Uid(uid) => write!(f, "UID {uid}"),
            AccountId::Session(session) => write!(f, "Unknown account (login #{session})"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DataKind {
    Items,
    Characters,
    Achievements,
}

impl DataKind {
    pub fn noun(&self) -> &'static str {
        match self {
            DataKind::Items => "items",
            DataKind::Characters => "characters",
            DataKind::Achievements => "achievements",
        }
    }
}

/// Player data decoded from a captured packet.
pub enum Captured {
    Items(Vec<Item>),
    Characters(Vec<AvatarInfo>),
    Achievements(Vec<Achievement>),
}

impl Captured {
    fn parse(kind: DataKind, proto_data: &[u8]) -> Option<Self> {
        match kind {
            DataKind::Items => matches_items_all_data_notify(proto_data).map(Captured::Items),
            DataKind::Characters => {
                matches_avatars_all_data_notify(proto_data).map(Captured::Characters)
            }
            DataKind::Achievements => {
                matches_achievement_all_data_notify(proto_data.to_vec()).map(Captured::Achievements)
            }
        }
    }

    pub fn kind(&self) -> DataKind {
        match self {
            Captured::Items(_) => DataKind::Items,
            Captured::Characters(_) => DataKind::Characters,
            Captured::Achievements(_) => DataKind::Achievements,
        }
    }

    pub fn count(&self) -> usize {
        match self {
            Captured::Items(items) => items.len(),
            Captured::Characters(characters) => characters.len(),
            Captured::Achievements(achievements) => achievements.len(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DataSummary {
    pub count: usize,
    pub captured_at: DateTime<Local>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AccountSummary {
    pub id: AccountId,
    pub items: Option<DataSummary>,
    pub characters: Option<DataSummary>,
    pub achievements: Option<DataSummary>,
}

impl AccountSummary {
    pub fn get(&self, kind: DataKind) -> Option<DataSummary> {
        match kind {
            DataKind::Items => self.items,
            DataKind::Characters => self.characters,
            DataKind::Achievements => self.achievements,
        }
    }

    pub fn can_export_optimizer(&self) -> bool {
        self.items.is_some() && self.characters.is_some()
    }

    pub fn last_captured(&self) -> Option<DateTime<Local>> {
        [self.items, self.characters, self.achievements]
            .into_iter()
            .flatten()
            .map(|summary| summary.captured_at)
            .max()
    }
}

/// The packet payload data was decoded from, kept so it can be saved.
struct RawCapture {
    captured_at: DateTime<Local>,
    count: usize,
    proto_data: Vec<u8>,
}

impl RawCapture {
    fn summary(&self) -> DataSummary {
        DataSummary {
            count: self.count,
            captured_at: self.captured_at,
        }
    }
}

struct Account {
    data: PlayerData,
    items: Option<RawCapture>,
    characters: Option<RawCapture>,
    achievements: Option<RawCapture>,
}

impl Account {
    fn new(game_data: Arc<AnimeGameData>) -> Self {
        Self {
            data: PlayerData::new(game_data),
            items: None,
            characters: None,
            achievements: None,
        }
    }

    fn store(&mut self, captured: Captured, proto_data: Vec<u8>, captured_at: DateTime<Local>) {
        let raw = RawCapture {
            captured_at,
            count: captured.count(),
            proto_data,
        };
        match captured {
            Captured::Items(items) => {
                self.data.process_items(&items);
                self.items = Some(raw);
            }
            Captured::Characters(characters) => {
                self.data.process_characters(&characters);
                self.characters = Some(raw);
            }
            Captured::Achievements(achievements) => {
                self.data.process_achievements(&achievements);
                self.achievements = Some(raw);
            }
        }
    }

    fn summary(&self, id: AccountId) -> AccountSummary {
        AccountSummary {
            id,
            items: self.items.as_ref().map(RawCapture::summary),
            characters: self.characters.as_ref().map(RawCapture::summary),
            achievements: self.achievements.as_ref().map(RawCapture::summary),
        }
    }
}

#[derive(Deserialize, Serialize)]
struct SavedAccount {
    format: u32,
    uid: u32,
    items: Option<SavedCapture>,
    characters: Option<SavedCapture>,
    achievements: Option<SavedCapture>,
}

#[derive(Deserialize, Serialize)]
struct SavedCapture {
    /// Unix timestamp in seconds.
    captured_at: i64,
    /// Base64 encoded packet payload.
    proto_data: String,
}

impl From<&RawCapture> for SavedCapture {
    fn from(raw: &RawCapture) -> Self {
        Self {
            captured_at: raw.captured_at.timestamp(),
            proto_data: BASE64_STANDARD.encode(&raw.proto_data),
        }
    }
}

pub struct Accounts {
    game_data: Arc<AnimeGameData>,
    accounts: IndexMap<AccountId, Account>,
    save_dir: Option<PathBuf>,
}

impl Accounts {
    pub fn new(game_data: Arc<AnimeGameData>, save_dir: Option<PathBuf>) -> Self {
        Self {
            game_data,
            accounts: IndexMap::new(),
            save_dir,
        }
    }

    /// Loads the accounts saved by earlier runs, returning the ones that
    /// loaded and a description of each that didn't.
    pub fn load_saved(&mut self) -> (Vec<AccountId>, Vec<String>) {
        let mut loaded = Vec::new();
        let mut errors = Vec::new();

        let Some(dir) = self.save_dir.clone() else {
            return (loaded, errors);
        };
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return (loaded, errors),
            Err(e) => {
                errors.push(format!("Couldn't read saved accounts in {dir:?}: {e}"));
                return (loaded, errors);
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.starts_with(SAVE_PREFIX) || !name.ends_with(SAVE_SUFFIX) {
                continue;
            }
            match self.load_file(&path) {
                Ok(id) => loaded.push(id),
                Err(e) => errors.push(format!("Couldn't load saved account {name}: {e:#}")),
            }
        }

        (loaded, errors)
    }

    fn load_file(&mut self, path: &Path) -> Result<AccountId> {
        let reader = GzDecoder::new(BufReader::new(File::open(path)?));
        let saved: SavedAccount = serde_json::from_reader(reader)?;
        if saved.format != SAVE_FORMAT {
            bail!("unsupported save format {}", saved.format);
        }

        let mut account = Account::new(self.game_data.clone());
        for (kind, capture) in [
            (DataKind::Items, saved.items),
            (DataKind::Characters, saved.characters),
            (DataKind::Achievements, saved.achievements),
        ] {
            let Some(capture) = capture else {
                continue;
            };
            let proto_data = BASE64_STANDARD.decode(&capture.proto_data)?;
            let captured = Captured::parse(kind, &proto_data)
                .ok_or_else(|| anyhow!("saved {} couldn't be decoded", kind.noun()))?;
            let captured_at = Local
                .timestamp_opt(capture.captured_at, 0)
                .single()
                .ok_or_else(|| anyhow!("invalid capture time {}", capture.captured_at))?;
            account.store(captured, proto_data, captured_at);
        }

        let id = AccountId::Uid(saved.uid);
        self.accounts.insert(id, account);
        Ok(id)
    }

    /// Stores freshly captured data for an account and saves it.
    pub fn record(
        &mut self,
        id: AccountId,
        captured: Captured,
        proto_data: Vec<u8>,
        captured_at: DateTime<Local>,
    ) -> Result<()> {
        let game_data = self.game_data.clone();
        self.accounts
            .entry(id)
            .or_insert_with(|| Account::new(game_data))
            .store(captured, proto_data, captured_at);
        self.save(id)
    }

    fn save_path(&self, uid: u32) -> Option<PathBuf> {
        let dir = self.save_dir.as_ref()?;
        Some(dir.join(format!("{SAVE_PREFIX}{uid}{SAVE_SUFFIX}")))
    }

    fn save(&self, id: AccountId) -> Result<()> {
        // Without a UID there is nothing to match the data up with after a
        // restart.
        let AccountId::Uid(uid) = id else {
            return Ok(());
        };
        let (Some(path), Some(account)) = (self.save_path(uid), self.accounts.get(&id)) else {
            return Ok(());
        };
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).with_context(|| format!("couldn't create {dir:?}"))?;
        }

        let saved = SavedAccount {
            format: SAVE_FORMAT,
            uid,
            items: account.items.as_ref().map(SavedCapture::from),
            characters: account.characters.as_ref().map(SavedCapture::from),
            achievements: account.achievements.as_ref().map(SavedCapture::from),
        };

        // Write to a temporary file first so a crash can't leave a truncated
        // save behind.
        let tmp_path = path.with_extension("tmp");
        {
            let file =
                File::create(&tmp_path).with_context(|| format!("couldn't create {tmp_path:?}"))?;
            let mut writer = GzEncoder::new(BufWriter::new(file), Compression::default());
            serde_json::to_writer(&mut writer, &saved)?;
            writer.finish()?.flush()?;
        }
        fs::rename(&tmp_path, &path).with_context(|| format!("couldn't write {path:?}"))?;

        Ok(())
    }

    /// Discards an account's data, including its save file.
    pub fn forget(&mut self, id: AccountId) -> Result<()> {
        self.accounts.shift_remove(&id);

        if let AccountId::Uid(uid) = id
            && let Some(path) = self.save_path(uid)
        {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).with_context(|| format!("couldn't delete {path:?}")),
            }
        }

        Ok(())
    }

    pub fn summary(&self, id: AccountId) -> Option<AccountSummary> {
        self.accounts.get(&id).map(|account| account.summary(id))
    }

    pub fn summaries(&self) -> Vec<AccountSummary> {
        self.accounts
            .iter()
            .map(|(id, account)| account.summary(*id))
            .collect()
    }

    pub fn export_genshin_optimizer(
        &self,
        id: AccountId,
        settings: &ExportSettings,
    ) -> Result<String> {
        self.accounts
            .get(&id)
            .ok_or_else(|| anyhow!("No captured data for {id}"))?
            .data
            .export_genshin_optimizer(settings)
    }
}

/// Detects the account that owns a set of item or character GUIDs.
///
/// GUIDs carry the owner's UID in their upper 32 bits.  The UID is only
/// trusted when nearly all GUIDs agree on it and it looks like a real UID.
pub fn uid_from_guids(guids: impl IntoIterator<Item = u64>) -> Option<u32> {
    let mut counts: HashMap<u32, usize> = HashMap::new();
    let mut total = 0;
    for guid in guids {
        *counts.entry((guid >> 32) as u32).or_default() += 1;
        total += 1;
    }

    let (&uid, &count) = counts.iter().max_by_key(|(_, count)| **count)?;
    let agreement = count as f64 / total as f64;
    (uid >= MIN_UID && agreement >= MIN_UID_AGREEMENT).then_some(uid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guid(uid: u32, sequence: u32) -> u64 {
        ((uid as u64) << 32) | sequence as u64
    }

    #[test]
    fn uid_is_read_from_guids() {
        let guids = (1..50).map(|sequence| guid(812_345_678, sequence));
        assert_eq!(uid_from_guids(guids), Some(812_345_678));
    }

    #[test]
    fn ten_digit_uids_are_accepted() {
        let guids = (1..50).map(|sequence| guid(1_812_345_678, sequence));
        assert_eq!(uid_from_guids(guids), Some(1_812_345_678));
    }

    #[test]
    fn a_few_stray_guids_are_tolerated() {
        let guids = (1..50)
            .map(|sequence| guid(812_345_678, sequence))
            .chain([guid(0, 1), guid(123, 2)]);
        assert_eq!(uid_from_guids(guids), Some(812_345_678));
    }

    #[test]
    fn disagreeing_guids_are_rejected() {
        let guids = (1..50)
            .map(|sequence| guid(812_345_678, sequence))
            .chain((1..50).map(|sequence| guid(612_345_678, sequence)));
        assert_eq!(uid_from_guids(guids), None);
    }

    #[test]
    fn implausible_uids_are_rejected() {
        assert_eq!(
            uid_from_guids((1..50).map(|sequence| guid(0, sequence))),
            None
        );
        assert_eq!(
            uid_from_guids((1..50).map(|sequence| guid(42, sequence))),
            None
        );
        assert_eq!(uid_from_guids([]), None);
    }

    fn varint(mut value: u64, out: &mut Vec<u8>) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }

    fn varint_field(field: u64, value: u64, out: &mut Vec<u8>) {
        varint(field << 3, out);
        varint(value, out);
    }

    fn bytes_field(field: u64, data: &[u8], out: &mut Vec<u8>) {
        varint((field << 3) | 2, out);
        varint(data.len() as u64, out);
        out.extend_from_slice(data);
    }

    /// Encodes a `PacketWithItems` holding materials.
    fn items_packet(uid: u32, count: u32) -> Vec<u8> {
        let mut packet = Vec::new();
        for sequence in 1..=count {
            let mut material = Vec::new();
            varint_field(1, 5, &mut material);

            let mut item = Vec::new();
            varint_field(1, 104_000 + sequence as u64, &mut item);
            varint_field(2, guid(uid, sequence), &mut item);
            bytes_field(5, &material, &mut item);
            bytes_field(6, &item, &mut packet);
        }
        packet
    }

    /// Encodes an `AvatarDataNotify`.
    fn characters_packet(uid: u32, count: u32) -> Vec<u8> {
        let mut packet = Vec::new();
        for sequence in 1..=count {
            let mut avatar = Vec::new();
            varint_field(1, 10_000_001 + sequence as u64, &mut avatar);
            varint_field(2, guid(uid, 1000 + sequence), &mut avatar);
            bytes_field(7, &avatar, &mut packet);
        }
        packet
    }

    fn record_packet(accounts: &mut Accounts, id: AccountId, kind: DataKind, proto_data: Vec<u8>) {
        let captured = Captured::parse(kind, &proto_data).expect("test packet must decode");
        let captured_at = Local.timestamp_opt(1_760_000_000, 0).unwrap();
        accounts
            .record(id, captured, proto_data, captured_at)
            .unwrap();
    }

    #[test]
    fn saved_accounts_are_loaded_again() {
        let dir = tempfile::tempdir().unwrap();
        let game_data = Arc::new(AnimeGameData::new());
        let id = AccountId::Uid(812_345_678);

        let mut accounts = Accounts::new(game_data.clone(), Some(dir.path().to_path_buf()));
        record_packet(
            &mut accounts,
            id,
            DataKind::Items,
            items_packet(812_345_678, 12),
        );
        record_packet(
            &mut accounts,
            id,
            DataKind::Characters,
            characters_packet(812_345_678, 3),
        );

        let mut reloaded = Accounts::new(game_data, Some(dir.path().to_path_buf()));
        let (loaded, errors) = reloaded.load_saved();
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(loaded, [id]);
        assert_eq!(reloaded.summaries(), accounts.summaries());

        let summary = reloaded.summary(id).unwrap();
        assert_eq!(summary.items.unwrap().count, 12);
        assert_eq!(summary.characters.unwrap().count, 3);
        assert!(summary.achievements.is_none());
        assert!(summary.can_export_optimizer());
    }

    #[test]
    fn forgotten_accounts_stay_forgotten() {
        let dir = tempfile::tempdir().unwrap();
        let game_data = Arc::new(AnimeGameData::new());
        let id = AccountId::Uid(812_345_678);

        let mut accounts = Accounts::new(game_data.clone(), Some(dir.path().to_path_buf()));
        record_packet(
            &mut accounts,
            id,
            DataKind::Items,
            items_packet(812_345_678, 12),
        );
        accounts.forget(id).unwrap();
        assert!(accounts.summaries().is_empty());

        let mut reloaded = Accounts::new(game_data, Some(dir.path().to_path_buf()));
        assert!(reloaded.load_saved().0.is_empty());
    }

    #[test]
    fn accounts_are_kept_apart() {
        let game_data = Arc::new(AnimeGameData::new());
        let mut accounts = Accounts::new(game_data, None);
        let first = AccountId::Uid(812_345_678);
        let second = AccountId::Uid(612_345_678);

        record_packet(
            &mut accounts,
            first,
            DataKind::Items,
            items_packet(812_345_678, 12),
        );
        record_packet(
            &mut accounts,
            second,
            DataKind::Items,
            items_packet(612_345_678, 20),
        );

        assert_eq!(accounts.summary(first).unwrap().items.unwrap().count, 12);
        assert_eq!(accounts.summary(second).unwrap().items.unwrap().count, 20);
    }

    #[test]
    fn accounts_without_a_uid_are_not_saved() {
        let dir = tempfile::tempdir().unwrap();
        let mut accounts = Accounts::new(
            Arc::new(AnimeGameData::new()),
            Some(dir.path().to_path_buf()),
        );
        record_packet(
            &mut accounts,
            AccountId::Session(1),
            DataKind::Items,
            items_packet(812_345_678, 12),
        );

        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
