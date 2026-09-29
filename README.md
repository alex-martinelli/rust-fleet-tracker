# Asynchronous Fleet Tracking System

![Rust](https://img.shields.io/badge/Rust-System_Programming-black?logo=rust)
![Tokio](https://img.shields.io/badge/Tokio-Concurrency-blue?logo=rust)

Full-stack asynchronous fleet tracking application engineered in Rust to process real-time truck coordinate data and visualize vehicle positions dynamically.

## 📌 Project Overview
The system provides real-time monitoring of a vehicle fleet through a highly concurrent backend and an interactive frontend. The codebase is structured using a modular workspace architecture, ensuring strict separation of concerns between the client, server, and shared data libraries.

## ⚙️ Key Features & Architecture
* **Concurrent Backend (Tokio):** Handles high-frequency, real-time coordinate updates and asynchronous database interactions without blocking the main execution thread.
* **Interactive Frontend (Walkers):** Implements a graphical client featuring an interactive geographical map to render vehicle positions.
* **Workspace Modularization:** Divides the application into independent crates (`client`, `server`, `shared`) to manage dependencies efficiently and facilitate independent compilation.

## 🚀 How to Run
1. Clone the repository: `git clone https://github.com/alex-martinelli/rust-fleet-tracker.git`
2. Navigate to the project directory: `cd rust-fleet-tracker`
3. Start the backend server: `cargo run --bin server`
4. In a separate terminal, launch the interactive client: `cargo run --bin client`
