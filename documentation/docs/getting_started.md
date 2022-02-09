---
image: /img/chronicle_icon.png 
description:  Get started with Chronicle. Prerequisites, installation and build instructions to run your own Chronicle permanode. 
keywords:
- quick start
- how to
- scylla
- cargo
- rust
- install
- build
- run chronicle
---

# Getting Started

## Prerequisites

Before you start the installation process, please make sure you meet the following requirements:

- A Linux LTS operating system such as [Ubuntu](https://ubuntu.com/download#download).

- 4 GB RAM.

- At least 32 GB of disk space.

- 64-bit processor.

- Preferred a 10 Gbps network connection.

- At least 2 CPU cores (recommended).

- [Rust](https://www.rust-lang.org/tools/install).

- At least one Scylla node (version 4 or greater) running on a different device in the same private network that you
  want to install Chronicle. 
 
    You can find instructions on how to set up the Scylla node in
    the [official Scylla documentation](https://docs.scylladb.com/getting-started/) as well as information
    about [securing your Scylla nodes](https://docs.scylladb.com/operating-scylla/security/).

- The `build-essentials` packages.

  In Debian based distros, you can install these packages, using the following command:

    ```bash
    sudo apt-get install build-essential gcc make cmake cmake-gui cmake-curses-gui pkg-config openssl libssl-dev
    ```
  
  For other Linux distros, please refer to your package manager to install the `build-essential` packages.

- (Optional) An IDE that supports Rust autocompletion. For example, [Visual Studio Code](https://code.visualstudio.com/Download) with
  the [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=matklad.rust-analyzer) extension.

- If you want to load historical transactions into your permanode, you can download the files from
  the [IOTA Foundation's archive](https://dbfiles.iota.org/?prefix=mainnet/history/).

We recommend that you update Rust to the latest stable version by running the following command:

```bash
rustup update stable
```

## Installation

If you do not wish to use the filtering functionality, you can download the Chronicle executable.
Alternatively, you can [build Chronicle](#build-chronicle) yourself. 

### Build Chronicle

You can follow these steps to build Chronicle from source:

1. Clone this repository:

```bash
git clone https://github.com/iotaledger/chronicle.rs
```
2. Run the `cargo build` command:
 
```bash
cargo build --release
```

3. (optional) If you wish to use the filter functionality, you should enable the `filter` feature in [chronicle](https://github.com/iotaledger/chronicle.rs/blob/main/chronicle/Cargo.toml) by running the following command:

```bash
cargo build --release --features filter
```

### Configure Chronicle

Chronicle uses a [RON](https://github.com/ron-rs/ron) file to store configuration parameters, called `config.ron`. An
example is provided as [config.example.ron](https://github.com/iotaledger/chronicle.rs/blob/main/config.example.ron) with default values. 

You can find more information about this file and configuration options in the [Configuration Reference section](config_reference.md).

### Run Chronicle

Once you have [built chronicle](#build-chronicle), you can run the following commands to run Chronicle:

1. Change directory into the release folder:

```bash
cd target/release
```

2. Copy your RON configuration file into the release folder:

```bash
cp /path/to/your/config.ron ./
```

3. You can now run your Chronicle permanode with the following command:  

```bash
cargo run --release
```