![Screenshot](docs/src/images/main-window.webp)

# Irminsul

Irminsul exports your Genshin Impact characters, weapons, artifacts and materials to [Genshin Optimizer](https://frzyc.github.io/genshin-optimizer/) in the [GOOD](https://frzyc.github.io/genshin-optimizer/#/doc) format. Instead of scanning the screen with OCR, it reads the game's network traffic, which makes it much faster.

This is a fork of [konkers/irminsul](https://github.com/konkers/irminsul). For help, see the original [docs](https://konkers.github.io/irminsul) and [Discord](https://discord.gg/aQqdZPHEpP).

## Download

Download `irminsul-windows-x64.exe` from the [latest release](https://github.com/ujjukumar/irminsul/releases/latest). Every push to `main` publishes a new build, and the app offers to update itself.

The release exe needs a CPU with AVX2 (Intel Haswell, AMD Zen or newer).

## Usage

1. Start Irminsul and allow admin access. It needs admin rights to capture packets.
2. Click ▶ to start capturing.
3. Launch Genshin and log in. Capture must already be running when you log in.
4. Export to Genshin Optimizer by copying to the clipboard or saving to a file.

The Activity panel shows what Irminsul is doing. Each account (UID) is saved separately, so you can log in to another account and capture it too.

## Command line options

- `--capture-backend <pktmon|pcap>`: `pktmon` is the Windows default. `pcap` needs Npcap on Windows or libpcap on Linux.
- `--no-admin`: don't ask for admin rights at startup.

## Building

You need Rust. The nightly toolchain is picked up automatically from `rust-toolchain.toml`. On Windows you also need the Visual Studio C++ build tools. The build downloads game data, so you need an internet connection.

```sh
cargo build --release
```

To get the same optimized AVX2 build as the releases:

```powershell
$env:RUSTFLAGS = "-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
cargo build --profile dist --target x86_64-pc-windows-msvc
```

## Credits

[konkers](https://github.com/konkers) wrote the original Irminsul. It builds on work by [PJK136](https://github.com/PJK136/stardb-exporter), [juliuskreutz](https://github.com/juliuskreutz/stardb-exporter), [hashblen](https://github.com/hashblen/auto-artifactarium) (packet decoding), [IceDynamix](https://github.com/IceDynamix/), [emmachase](https://github.com/emmachase/pktmon) (pktmon capture), [Genshin Optimizer](https://frzyc.github.io/genshin-optimizer/) and [Inventory Kamera](https://github.com/Andrewthe13th/Inventory_Kamera).
