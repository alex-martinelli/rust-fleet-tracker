// # Georuggine - Client
//
// Emula il terminale di bordo di un veicolo della flotta:
//
// * registrazione e login verso il server;
// * emulazione del movimento
// * mappa con la posizione del veicolo aggiornata in tempo reale
// * richiesta al server dell'analisi del movimento (giorno/settimana/mese);
// * invio di messaggi al server e ricezione dei messaggi (broadcast o diretti) inviati dal server.
//
// L'architettura:
// * il **thread della GUI**, sincrono.
// * un **thread di rete** con un runtime Tokio a thread singolo.
//
// I due comunicano con due canali: `tokio::sync::mpsc` GUI -> rete e
// `std::sync::mpsc` rete -> GUI.

use eframe::egui;
use shared::{
    formatta_durata, hash_password, normalizza_username, valida_username, ClientMessage,
    Intervallo, ServerMessage, Stato, INTERVALLO_INVIO_SEC, TIMEOUT_FERMO_SEC,
};
use std::fs::File;
use std::io::{BufRead, BufReader as StdBufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex as TokioMutex;
use walkers::{
    lat_lon, sources::OpenStreetMap, HttpTiles, Map, MapMemory, Plugin, Position, Projector,
};

/// Indirizzo del server usato di default.
const SERVER_DEFAULT: &str = "127.0.0.1:8080";
/// Nome del file con il tracciato emulato.
const FILE_PERCORSO: &str = "percorso.csv";
/// Intervallo minimo fra due richieste di analisi automatiche.
const THROTTLE_ANALISI: Duration = Duration::from_secs(2);

// ===========================================================================
// Canali GUI <-> rete
// ===========================================================================

/// Comandi che la GUI invia al thread di rete.
enum ComandoRete {
    Registra { user: String, password: String },
    Accedi { user: String, password: String },
    AvviaPercorso { fattore: f64 },
    ImpostaPausa(bool),
    FermaPercorso,
    RichiediAnalisi(Intervallo),
    InviaMessaggio(String),
    /// Disconnessione esplicita richiesta dall'utente.
    Disconnetti,
}

/// Eventi che il thread di rete notifica alla GUI.
enum EventoRete {
    Connesso,
    Disconnesso(String),
    Avviso(String),
    PercorsoCaricato { file: String, punti: usize, durata_sec: u64 },
    DalServer(ServerMessage),
}

fn notifica(tx: &Sender<EventoRete>, ctx: &egui::Context, evento: EventoRete) {
    if tx.send(evento).is_ok() {
        ctx.request_repaint();
    }
}

// ===========================================================================
// Lettura del tracciato emulato
// ===========================================================================

/// Un campione del tracciato.
#[derive(Clone, Copy)]
struct PuntoPercorso {
    offset_sec: u64,
    lat: f64,
    lon: f64,
}

fn percorsi_candidati() -> Vec<PathBuf> {
    let mut candidati = vec![
        PathBuf::from(FILE_PERCORSO),
        PathBuf::from("client").join(FILE_PERCORSO),
    ];
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidati.push(dir.join(FILE_PERCORSO));
        }
    }
    candidati
}

fn separa_campi(riga: &str) -> Vec<String> {
    for separatore in [';', '\t'] {
        let campi: Vec<&str> = riga
            .split(separatore)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        if campi.len() == 2 || campi.len() == 3 {
            return campi.into_iter().map(|s| s.replace(',', ".")).collect();
        }
    }
    let campi: Vec<&str> = riga
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if campi.len() == 2 || campi.len() == 3 {
        return campi.into_iter().map(str::to_string).collect();
    }
    riga.split_whitespace()
        .map(|s| s.replace(',', "."))
        .collect()
}

/// Accetta `secondi`, `mm:ss` oppure `hh:mm:ss`.
fn parse_tempo(testo: &str) -> Option<u64> {
    let testo = testo.trim();
    if !testo.contains(':') {
        return testo.parse::<u64>().ok();
    }
    let numeri: Option<Vec<u64>> = testo
        .split(':')
        .map(|p| p.trim().parse::<u64>().ok())
        .collect();
    let numeri = numeri?;
    match numeri.len() {
        2 => Some(numeri[0] * 60 + numeri[1]),
        3 => Some(numeri[0] * 3600 + numeri[1] * 60 + numeri[2]),
        _ => None,
    }
}

/// Carica il tracciato dal primo file trovato fra quelli candidati.
fn carica_percorso() -> Result<(PathBuf, Vec<PuntoPercorso>), String> {
    let candidati = percorsi_candidati();
    let file_scelto = candidati
        .iter()
        .find(|p| p.is_file())
        .cloned()
        .ok_or_else(|| {
            format!(
                "File '{}' non trovato. Cercato in: {}",
                FILE_PERCORSO,
                candidati
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;

    let file = File::open(&file_scelto).map_err(|e| format!("{}: {e}", file_scelto.display()))?;
    let mut punti: Vec<PuntoPercorso> = Vec::new();

    for (n, riga) in StdBufReader::new(file).lines().enumerate() {
        let riga = riga.map_err(|e| format!("errore di lettura alla riga {}: {e}", n + 1))?;
        // Un file salvato da Excel o da certi editor inizia con il BOM UTF-8:
        // va rimosso, altrimenti la prima riga non viene riconosciuta.
        let riga = riga.trim_start_matches('\u{feff}').trim();
        if riga.is_empty() || riga.starts_with('#') || riga.starts_with("//") {
            continue;
        }

        let campi = separa_campi(riga);
        let (tempo, lat_txt, lon_txt) = match campi.len() {
            3 => (Some(campi[0].clone()), campi[1].clone(), campi[2].clone()),
            2 => (None, campi[0].clone(), campi[1].clone()),
            _ => {
                return Err(format!(
                    "riga {}: formato non riconosciuto -> \"{}\"",
                    n + 1,
                    riga
                ))
            }
        };

        if lon_txt.parse::<f64>().is_err() && punti.is_empty() {
            continue;
        }

        let lat: f64 = lat_txt
            .parse()
            .map_err(|_| format!("riga {}: latitudine non valida '{lat_txt}'", n + 1))?;
        let lon: f64 = lon_txt
            .parse()
            .map_err(|_| format!("riga {}: longitudine non valida '{lon_txt}'", n + 1))?;
        if !shared::coordinate_valide(lat, lon) {
            return Err(format!("riga {}: coordinate fuori range", n + 1));
        }

        // Se manca la colonna dei tempi si assume il passo standard di 30 s.
        let offset_sec = match tempo {
            Some(t) => parse_tempo(&t)
                .ok_or_else(|| format!("riga {}: tempo non valido '{t}'", n + 1))?,
            None => punti.len() as u64 * INTERVALLO_INVIO_SEC,
        };

        punti.push(PuntoPercorso { offset_sec, lat, lon });
    }

    if punti.is_empty() {
        return Err(format!("{}: nessun punto valido", file_scelto.display()));
    }
    // I tempi devono essere crescenti perche' le attese abbiano senso.
    punti.sort_by_key(|p| p.offset_sec);

    Ok((file_scelto, punti))
}

// ===========================================================================
// Thread di rete
// ===========================================================================

type Scrittore = Arc<TokioMutex<WriteHalf<TcpStream>>>;

async fn invia(scrittore: &Scrittore, messaggio: &ClientMessage) -> bool {
    let Ok(mut testo) = serde_json::to_string(messaggio) else {
        return false;
    };
    testo.push('\n');
    let mut guardia = scrittore.lock().await;
    guardia.write_all(testo.as_bytes()).await.is_ok()
}

/// Emula il movimento del veicolo.
async fn simula_percorso(
    scrittore: Scrittore,
    punti: Vec<PuntoPercorso>,
    fattore: f64,
    in_pausa: Arc<AtomicBool>,
) {
    if punti.is_empty() {
        return;
    }
    let fattore = if fattore.is_finite() && fattore > 0.0 { fattore } else { 1.0 };
    let mut i = 0usize;

    loop {
        let punto = punti[i];
        if !invia(
            &scrittore,
            &ClientMessage::PositionUpdate { lat: punto.lat, lon: punto.lon },
        )
        .await
        {
            return; // socket chiuso: il task termina da solo
        }

        let attesa_simulata = if i + 1 < punti.len() {
            punti[i + 1].offset_sec.saturating_sub(punto.offset_sec).max(1)
        } else {
            INTERVALLO_INVIO_SEC
        };
        let attesa = (attesa_simulata as f64 / fattore).max(0.05);
        tokio::time::sleep(Duration::from_secs_f64(attesa)).await;

        if !in_pausa.load(Ordering::Relaxed) && i + 1 < punti.len() {
            i += 1;
        }
    }
}

async fn network_loop(
    mut rx_comandi: tokio::sync::mpsc::Receiver<ComandoRete>,
    tx_gui: Sender<EventoRete>,
    ctx: egui::Context,
    indirizzo: String,
) {
    let socket = match TcpStream::connect(&indirizzo).await {
        Ok(s) => s,
        Err(e) => {
            notifica(
                &tx_gui,
                &ctx,
                EventoRete::Disconnesso(format!(
                    "Impossibile connettersi al server {indirizzo}: {e}\nAvvia prima il server con `cargo run --release --bin server`."
                )),
            );
            return;
        }
    };
    let _ = socket.set_nodelay(true);
    notifica(&tx_gui, &ctx, EventoRete::Connesso);

    let (lettore, scrittore) = tokio::io::split(socket);
    let scrittore: Scrittore = Arc::new(TokioMutex::new(scrittore));

    // Inoltra alla GUI tutto cio' che arriva dal server.
    {
        let tx_gui = tx_gui.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let mut righe = BufReader::new(lettore).lines();
            loop {
                match righe.next_line().await {
                    Ok(Some(riga)) => {
                        let riga = riga.trim();
                        if riga.is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<ServerMessage>(riga) {
                            Ok(msg) => notifica(&tx_gui, &ctx, EventoRete::DalServer(msg)),
                            Err(e) => eprintln!("[rete] messaggio illeggibile dal server: {e}"),
                        }
                    }
                    _ => {
                        notifica(
                            &tx_gui,
                            &ctx,
                            EventoRete::Disconnesso(
                                "Connessione con il server interrotta.".to_string(),
                            ),
                        );
                        break;
                    }
                }
            }
        });
    }

    let mut simulazione: Option<(tokio::task::JoinHandle<()>, Arc<AtomicBool>)> = None;

    while let Some(comando) = rx_comandi.recv().await {
        match comando {
            ComandoRete::Registra { user, password } => {
                let messaggio = ClientMessage::Register {
                    user: normalizza_username(&user),
                    password_hash: hash_password(&user, &password),
                };
                invia(&scrittore, &messaggio).await;
            }

            ComandoRete::Accedi { user, password } => {
                let messaggio = ClientMessage::Login {
                    user: normalizza_username(&user),
                    password_hash: hash_password(&user, &password),
                };
                invia(&scrittore, &messaggio).await;
            }

            ComandoRete::RichiediAnalisi(intervallo) => {
                invia(&scrittore, &ClientMessage::RichiestaAnalisi(intervallo)).await;
            }

            ComandoRete::InviaMessaggio(testo) => {
                invia(&scrittore, &ClientMessage::ChatMessage { testo }).await;
            }

            ComandoRete::AvviaPercorso { fattore } => {
                if simulazione.is_some() {
                    continue;
                }
                match carica_percorso() {
                    Ok((file, punti)) => {
                        let durata_sec = punti
                            .last()
                            .map(|p| p.offset_sec)
                            .unwrap_or(0)
                            .saturating_sub(punti[0].offset_sec);
                        notifica(
                            &tx_gui,
                            &ctx,
                            EventoRete::PercorsoCaricato {
                                file: file.display().to_string(),
                                punti: punti.len(),
                                durata_sec,
                            },
                        );
                        // Segnala al server che sta iniziando (o ricominciando) un
                        // nuovo tragitto.
                        invia(&scrittore, &ClientMessage::PercorsoAvviato).await;
                        let in_pausa = Arc::new(AtomicBool::new(false));
                        let handle = tokio::spawn(simula_percorso(
                            Arc::clone(&scrittore),
                            punti,
                            fattore,
                            Arc::clone(&in_pausa),
                        ));
                        simulazione = Some((handle, in_pausa));
                    }
                    Err(e) => notifica(&tx_gui, &ctx, EventoRete::Avviso(e)),
                }
            }

            ComandoRete::ImpostaPausa(pausa) => {
                if let Some((_, flag)) = &simulazione {
                    flag.store(pausa, Ordering::Relaxed);
                }
            }

            ComandoRete::FermaPercorso => {
                if let Some((handle, _)) = simulazione.take() {
                    handle.abort();
                }
            }

            ComandoRete::Disconnetti => {
                
                if let Some((handle, _)) = simulazione.take() {
                    handle.abort();
                }
                invia(&scrittore, &ClientMessage::Logout).await;
            }
        }
    }
}

// ===========================================================================
// Overlay della mappa
// ===========================================================================

struct TracciaVeicolo {
    traccia: Vec<Position>,
    attuale: Option<Position>,
    in_movimento: bool,
}

impl Plugin for TracciaVeicolo {
    fn run(
        self: Box<Self>,
        ui: &mut egui::Ui,
        response: &egui::Response,
        projector: &Projector,
        _map_memory: &MapMemory,
    ) {
        let painter = ui.painter_at(response.rect);

        let punti: Vec<egui::Pos2> = self
            .traccia
            .iter()
            .map(|p| projector.project(*p).to_pos2())
            .collect();

        if punti.len() >= 2 {
            let tratto = egui::Stroke::new(3.0, egui::Color32::from_rgb(30, 120, 220));
            for coppia in punti.windows(2) {
                painter.line_segment([coppia[0], coppia[1]], tratto);
            }
        }

        // Marcatore verde sulla partenza.
        if let Some(partenza) = punti.first() {
            painter.circle_filled(*partenza, 6.0, egui::Color32::from_rgb(40, 160, 80));
            painter.circle_stroke(
                *partenza,
                6.0,
                egui::Stroke::new(1.5, egui::Color32::WHITE),
            );
        }

        // Marcatore della posizione attuale: verde se in movimento, giallo se fermo.
        if let Some(pos) = self.attuale {
            let centro = projector.project(pos).to_pos2();
            let colore = if self.in_movimento {
                egui::Color32::from_rgb(40, 200, 60)
            } else {
                egui::Color32::from_rgb(235, 175, 30)
            };
            painter.circle_filled(centro, 10.0, colore);
            painter.circle_stroke(centro, 10.0, egui::Stroke::new(2.0, egui::Color32::WHITE));
        }
    }
}

// ===========================================================================
// Applicazione
// ===========================================================================

#[derive(PartialEq)]
enum StatoConnessione {
    InCorso,
    Connesso,
    Caduta(String),
}

/// Copia locale dell'ultimo risultato di analisi ricevuto dal server.
struct AnalisiVista {
    intervallo: Intervallo,
    tragitto_km: f64,
    velocita_media_kmh: f64,
    movimento_sec: u64,
    pause_sec: u64,
    campioni: u64,
}

struct GeoruggineApp {
    tx_net: tokio::sync::mpsc::Sender<ComandoRete>,
    rx_net: Receiver<EventoRete>,
    indirizzo_server: String,

    connessione: StatoConnessione,

    // --- autenticazione ---
    autenticato: bool,
    utente: String,
    modo_registrazione: bool,
    campo_user: String,
    campo_pass: String,
    campo_pass_conferma: String,
    /// (esito positivo?, testo) dell'ultima risposta di login/registrazione.
    esito_auth: Option<(bool, String)>,

    // --- telemetria ---
    stato_veicolo: Stato,
    ultima_lat: Option<f64>,
    ultima_lon: Option<f64>,
    campioni_ricevuti: u64,

    // --- simulazione ---
    simulazione_attiva: bool,
    in_pausa: bool,
    fattore_velocita: f64,
    info_percorso: Option<String>,
    avviso: Option<String>,

    // --- mappa ---
    tiles: HttpTiles,
    map_memory: MapMemory,
    traccia: Vec<(f64, f64)>,

    // --- analisi ---
    intervallo: Intervallo,
    analisi: Option<AnalisiVista>,
    ultima_richiesta_analisi: Instant,

    // --- messaggi ---
    messaggi: Vec<String>,
    campo_messaggio: String,
}

impl GeoruggineApp {
    fn new(
        tx_net: tokio::sync::mpsc::Sender<ComandoRete>,
        rx_net: Receiver<EventoRete>,
        ctx: egui::Context,
        indirizzo_server: String,
    ) -> Self {
        let mut map_memory = MapMemory::default();
        let _ = map_memory.set_zoom(10.0);

        Self {
            tx_net,
            rx_net,
            indirizzo_server,
            connessione: StatoConnessione::InCorso,
            autenticato: false,
            utente: String::new(),
            modo_registrazione: false,
            campo_user: String::new(),
            campo_pass: String::new(),
            campo_pass_conferma: String::new(),
            esito_auth: None,
            stato_veicolo: Stato::Sconnesso,
            ultima_lat: None,
            ultima_lon: None,
            campioni_ricevuti: 0,
            simulazione_attiva: false,
            in_pausa: false,
            fattore_velocita: 1.0,
            info_percorso: None,
            avviso: None,
            tiles: HttpTiles::new(OpenStreetMap, ctx),
            map_memory,
            traccia: Vec::new(),
            intervallo: Intervallo::Giorno,
            analisi: None,
            ultima_richiesta_analisi: Instant::now()
                .checked_sub(THROTTLE_ANALISI)
                .unwrap_or_else(Instant::now),
            messaggi: Vec::new(),
            campo_messaggio: String::new(),
        }
    }

    fn invia_comando(&mut self, comando: ComandoRete) {
        if self.tx_net.try_send(comando).is_err() {
            self.avviso = Some("Il canale verso il server non risponde.".to_string());
        }
    }

    /// Chiede l'analisi al server.
    fn richiedi_analisi(&mut self, forza: bool) {
        if forza || self.ultima_richiesta_analisi.elapsed() >= THROTTLE_ANALISI {
            self.ultima_richiesta_analisi = Instant::now();
            let intervallo = self.intervallo;
            self.invia_comando(ComandoRete::RichiediAnalisi(intervallo));
        }
    }

    /// Riporta la GUI alla schermata di login.
    fn esegui_logout(&mut self) {
        self.invia_comando(ComandoRete::Disconnetti);

        self.autenticato = false;
        self.utente.clear();
        self.esito_auth = None;
        self.campo_pass.clear();
        self.campo_pass_conferma.clear();

        self.stato_veicolo = Stato::Sconnesso;
        self.ultima_lat = None;
        self.ultima_lon = None;
        self.campioni_ricevuti = 0;

        self.simulazione_attiva = false;
        self.in_pausa = false;
        self.info_percorso = None;

        self.traccia.clear();
        self.analisi = None;

        self.messaggi.clear();
        self.campo_messaggio.clear();
    }

    // -----------------------------------------------------------------------
    // Eventi dalla rete
    // -----------------------------------------------------------------------

    fn processa_eventi(&mut self) {
        while let Ok(evento) = self.rx_net.try_recv() {
            match evento {
                EventoRete::Connesso => {
                    self.connessione = StatoConnessione::Connesso;
                }

                EventoRete::Disconnesso(motivo) => {
                    self.connessione = StatoConnessione::Caduta(motivo);
                    self.autenticato = false;
                    self.simulazione_attiva = false;
                    self.in_pausa = false;
                    self.stato_veicolo = Stato::Sconnesso;
                }

                EventoRete::Avviso(testo) => {
                    self.avviso = Some(testo);
                    self.simulazione_attiva = false;
                }

                EventoRete::PercorsoCaricato { file, punti, durata_sec } => {
                    self.avviso = None;
                    self.info_percorso = Some(format!(
                        "{punti} campioni da '{file}' ({} di tragitto emulato)",
                        formatta_durata(durata_sec)
                    ));
                }

                EventoRete::DalServer(msg) => self.processa_messaggio_server(msg),
            }
        }
    }

    fn processa_messaggio_server(&mut self, msg: ServerMessage) {
        match msg {
            ServerMessage::AuthResult { success, messaggio } => {
                if success {
                    self.autenticato = true;
                    self.utente = normalizza_username(&self.campo_user);
                    self.esito_auth = None;
                    self.campo_pass.clear();
                    self.campo_pass_conferma.clear();
                    self.richiedi_analisi(true);
                } else {
                    self.esito_auth = Some((false, messaggio));
                }
            }

            ServerMessage::RegisterResult { success, messaggio } => {
                self.esito_auth = Some((success, messaggio));
                if success {
                    self.modo_registrazione = false;
                    self.campo_pass.clear();
                    self.campo_pass_conferma.clear();
                }
            }

            ServerMessage::StatoAggiornato { stato, lat, lon } => {
                self.stato_veicolo = stato;
                if let (Some(lat), Some(lon)) = (lat, lon) {
                    self.ultima_lat = Some(lat);
                    self.ultima_lon = Some(lon);
                    self.campioni_ricevuti += 1;
                    if self.traccia.last() != Some(&(lat, lon)) {
                        self.traccia.push((lat, lon));
                    }
                    // Le statistiche seguono il veicolo mentre si muove.
                    self.richiedi_analisi(false);
                }
            }

            ServerMessage::AnalisiResult {
                intervallo,
                tragitto_km,
                velocita_media_kmh,
                movimento_sec,
                pause_sec,
                campioni,
            } => {
                self.analisi = Some(AnalisiVista {
                    intervallo,
                    tragitto_km,
                    velocita_media_kmh,
                    movimento_sec,
                    pause_sec,
                    campioni,
                });
            }

            ServerMessage::MessaggioDalServer { testo, broadcast } => {
                let etichetta = if broadcast { "[SERVER - broadcast]" } else { "[SERVER - diretto]" };
                self.messaggi.push(format!("{etichetta} {testo}"));
            }

            // Conferma del logout.
            ServerMessage::LogoutOk => {}
        }
    }

    // -----------------------------------------------------------------------
    // Schermata di login / registrazione
    // -----------------------------------------------------------------------

    /// Controlla i campi lato client.
    fn valida_campi(&self) -> Result<(), String> {
        valida_username(&self.campo_user)?;
        if self.campo_pass.len() < 4 {
            return Err("La password deve essere lunga almeno 4 caratteri.".to_string());
        }
        if self.modo_registrazione && self.campo_pass != self.campo_pass_conferma {
            return Err("Le due password non coincidono.".to_string());
        }
        Ok(())
    }

    fn vista_login(&mut self, ui: &mut egui::Ui) {
        let invio_premuto = ui.ctx().input(|i| i.key_pressed(egui::Key::Enter));
        let connesso = self.connessione == StatoConnessione::Connesso;

        ui.vertical_centered(|ui| {
            ui.add_space(50.0);
            ui.heading("Georuggine - Terminale di bordo");
            ui.label(
                egui::RichText::new(if self.modo_registrazione {
                    "Registrazione di un nuovo autista"
                } else {
                    "Autenticazione veicolo"
                })
                .size(15.0),
            );
            ui.add_space(6.0);

            // Stato della connessione al server.
            match &self.connessione {
                StatoConnessione::InCorso => {
                    ui.colored_label(
                        egui::Color32::from_rgb(200, 160, 40),
                        format!("Connessione a {} in corso...", self.indirizzo_server),
                    );
                }
                StatoConnessione::Connesso => {
                    ui.colored_label(
                        egui::Color32::from_rgb(60, 170, 90),
                        format!("Connesso a {}", self.indirizzo_server),
                    );
                }
                StatoConnessione::Caduta(motivo) => {
                    ui.colored_label(egui::Color32::from_rgb(210, 70, 60), motivo);
                    ui.small("Riavvia il client dopo aver avviato il server.");
                }
            }

            ui.add_space(16.0);

            ui.horizontal(|ui| {
                ui.label("Nome utente:");
                ui.add(
                    egui::TextEdit::singleline(&mut self.campo_user)
                        .desired_width(220.0)
                        .hint_text("da 3 a 24 caratteri"),
                );
            });
            ui.horizontal(|ui| {
                ui.label("Password:");
                ui.add(
                    egui::TextEdit::singleline(&mut self.campo_pass)
                        .password(true)
                        .desired_width(220.0)
                        .hint_text("almeno 4 caratteri"),
                );
            });
            if self.modo_registrazione {
                ui.horizontal(|ui| {
                    ui.label("Conferma:");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.campo_pass_conferma)
                            .password(true)
                            .desired_width(220.0)
                            .hint_text("ripeti la password"),
                    );
                });
            }

            ui.add_space(12.0);

            let etichetta = if self.modo_registrazione { "Registrati" } else { "Accedi" };
            let conferma = ui
                .add_enabled(connesso, egui::Button::new(etichetta).min_size(egui::vec2(140.0, 26.0)))
                .clicked()
                || (connesso && invio_premuto);

            if conferma {
                match self.valida_campi() {
                    Ok(()) => {
                        let user = self.campo_user.clone();
                        let password = self.campo_pass.clone();
                        self.esito_auth = None;
                        let comando = if self.modo_registrazione {
                            ComandoRete::Registra { user, password }
                        } else {
                            ComandoRete::Accedi { user, password }
                        };
                        self.invia_comando(comando);
                    }
                    Err(e) => self.esito_auth = Some((false, e)),
                }
            }

            ui.add_space(4.0);
            let alternativa = if self.modo_registrazione {
                "Hai gia' un account? Accedi"
            } else {
                "Non hai un account? Registrati"
            };
            if ui.button(alternativa).clicked() {
                self.modo_registrazione = !self.modo_registrazione;
                self.esito_auth = None;
                self.campo_pass_conferma.clear();
            }

            if let Some((ok, testo)) = &self.esito_auth {
                ui.add_space(8.0);
                let colore = if *ok {
                    egui::Color32::from_rgb(60, 170, 90)
                } else {
                    egui::Color32::from_rgb(210, 70, 60)
                };
                ui.colored_label(colore, testo);
            }

            ui.add_space(20.0);
            ui.small("La password non viene mai trasmessa in chiaro: il client invia solo la sua impronta SHA-256.");
        });
    }

    // -----------------------------------------------------------------------
    // Schermata operativa
    // -----------------------------------------------------------------------

    fn vista_principale(&mut self, ui: &mut egui::Ui) {
        if let Some(avviso) = self.avviso.clone() {
            ui.horizontal(|ui| {
                ui.colored_label(egui::Color32::from_rgb(210, 70, 60), avviso);
                if ui.small_button("chiudi").clicked() {
                    self.avviso = None;
                }
            });
            ui.separator();
        }

        let larghezza_totale = ui.available_width();
        let altezza_totale = ui.available_height();
        let larghezza_lato = (larghezza_totale * 0.26).clamp(250.0, 340.0);
        let larghezza_mappa = (larghezza_totale - 2.0 * larghezza_lato - 42.0).max(260.0);
        let altezza_mappa = (altezza_totale - 64.0).max(240.0);

        ui.horizontal_top(|ui| {
            ui.vertical(|ui| {
                ui.set_width(larghezza_lato);
                egui::ScrollArea::vertical()
                    .id_salt("colonna_telemetria")
                    .max_height(altezza_mappa + 24.0)
                    .auto_shrink(false)
                    .show(ui, |ui| self.pannello_telemetria(ui));
            });

            ui.separator();

            ui.vertical(|ui| {
                ui.set_width(larghezza_mappa);
                self.pannello_mappa(ui, larghezza_mappa, altezza_mappa);
            });

            ui.separator();

            ui.vertical(|ui| {
                ui.set_width(larghezza_lato);
                self.pannello_messaggi(ui, altezza_mappa);
            });
        });
    }

    fn pannello_telemetria(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading(format!("Autista: {}", self.utente));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Esci").clicked() {
                    self.esegui_logout();
                }
            });
        });
        ui.separator();

        ui.label(egui::RichText::new("Telemetria").strong().size(15.0));
        ui.horizontal(|ui| {
            ui.label("Stato veicolo:");
            let colore = match self.stato_veicolo {
                Stato::InMovimento => egui::Color32::from_rgb(40, 200, 60),
                Stato::Fermo => egui::Color32::from_rgb(220, 170, 40),
                Stato::Sconnesso => egui::Color32::GRAY,
            };
            ui.colored_label(
                colore,
                egui::RichText::new(self.stato_veicolo.etichetta()).strong(),
            );
        });

        match (self.ultima_lat, self.ultima_lon) {
            (Some(lat), Some(lon)) => {
                ui.label(format!("Posizione: {lat:.5}, {lon:.5}"));
            }
            _ => {
                ui.label("Posizione: nessuna coordinata inviata");
            }
        }
        ui.label(format!("Campioni trasmessi: {}", self.campioni_ricevuti));
        ui.small(format!(
            "Invio ogni {INTERVALLO_INVIO_SEC} s; passaggio a \"fermo\" dopo {} senza variazioni.",
            formatta_durata(TIMEOUT_FERMO_SEC)
        ));

        ui.add_space(12.0);
        ui.label(egui::RichText::new("Emulazione del movimento").strong().size(15.0));

        if !self.simulazione_attiva {
            ui.horizontal(|ui| {
                ui.label("Velocita':");
                ui.selectable_value(&mut self.fattore_velocita, 1.0, "x1");
                ui.selectable_value(&mut self.fattore_velocita, 10.0, "x10");
                ui.selectable_value(&mut self.fattore_velocita, 60.0, "x60");
            });
            ui.small("x1 rispetta la specifica (un campione ogni 30 s reali); i fattori piu' alti servono solo per le dimostrazioni.");
            ui.add_space(4.0);
            if ui
                .add(egui::Button::new("Avvia percorso (Torino - Asti)").min_size(egui::vec2(240.0, 26.0)))
                .clicked()
            {
                // Ripulisce la mappa dal tragitto precedente.
                self.traccia.clear();
                self.ultima_lat = None;
                self.ultima_lon = None;

                self.simulazione_attiva = true;
                self.in_pausa = false;
                let fattore = self.fattore_velocita;
                self.invia_comando(ComandoRete::AvviaPercorso { fattore });
            }
        } else {
            ui.colored_label(
                egui::Color32::from_rgb(60, 170, 90),
                format!("Trasmissione attiva (fattore x{:.0})", self.fattore_velocita),
            );
            ui.horizontal(|ui| {
                let etichetta = if self.in_pausa { "Riprendi" } else { "Sosta" };
                if ui.button(etichetta).clicked() {
                    self.in_pausa = !self.in_pausa;
                    let pausa = self.in_pausa;
                    self.invia_comando(ComandoRete::ImpostaPausa(pausa));
                }
                if ui.button("Ferma").clicked() {
                    self.simulazione_attiva = false;
                    self.in_pausa = false;
                    self.invia_comando(ComandoRete::FermaPercorso);
                }
            });
            if self.in_pausa {
                ui.small("In sosta: il veicolo continua a trasmettere la stessa posizione.");
            }
        }

        if let Some(info) = &self.info_percorso {
            ui.small(info);
        }

        ui.add_space(12.0);
        ui.label(egui::RichText::new("Analisi del movimento").strong().size(15.0));
        ui.horizontal(|ui| {
            let mut cambiato = false;
            cambiato |= ui
                .selectable_value(&mut self.intervallo, Intervallo::Giorno, "Giorno")
                .clicked();
            cambiato |= ui
                .selectable_value(&mut self.intervallo, Intervallo::Settimana, "Settimana")
                .clicked();
            cambiato |= ui
                .selectable_value(&mut self.intervallo, Intervallo::Mese, "Mese")
                .clicked();
            if cambiato {
                self.richiedi_analisi(true);
            }
        });
        ui.add_space(4.0);

        let larghezza_gruppo = ui.available_width();
        ui.group(|ui| {
            ui.set_width(larghezza_gruppo);
            match &self.analisi {
                None => {
                    ui.label("Nessuna analisi disponibile.");
                    ui.small("Avvia il percorso per ricevere i dati dal server.");
                }
                Some(a) => {
                    ui.label(egui::RichText::new(a.intervallo.etichetta()).italics());
                    ui.label(format!("Tragitto percorso: {:.2} km", a.tragitto_km));
                    ui.label(format!("Velocita' media: {:.2} km/h", a.velocita_media_kmh));
                    ui.label(format!("Durata movimento: {}", formatta_durata(a.movimento_sec)));
                    ui.label(format!("Durata pause: {}", formatta_durata(a.pause_sec)));
                    ui.small(format!("Calcolata su {} campioni.", a.campioni));
                }
            }
        });
    }

    fn pannello_mappa(&mut self, ui: &mut egui::Ui, larghezza: f32, altezza: f32) {
        ui.horizontal(|ui| {
            ui.heading("Mappa in tempo reale");
            if ui.small_button("Segui veicolo").clicked() {
                self.map_memory.follow_my_position();
            }
            if ui.small_button("+").clicked() {
                let _ = self.map_memory.zoom_in();
            }
            if ui.small_button("-").clicked() {
                let _ = self.map_memory.zoom_out();
            }
            ui.small(format!("zoom {:.0}", self.map_memory.zoom()));
        });
        ui.separator();

        // Se non e' ancora arrivata nessuna coordinata si centra su Torino,
        // primo punto del tracciato di esempio.
        let posizione = match (self.ultima_lat, self.ultima_lon) {
            (Some(lat), Some(lon)) => lat_lon(lat, lon),
            _ => lat_lon(45.0618513, 7.6606506),
        };

        let overlay = TracciaVeicolo {
            traccia: self
                .traccia
                .iter()
                .map(|(lat, lon)| lat_lon(*lat, *lon))
                .collect(),
            attuale: self.ultima_lat.map(|_| posizione),
            in_movimento: self.stato_veicolo == Stato::InMovimento,
        };

        let mappa = Map::new(Some(&mut self.tiles), &mut self.map_memory, posizione)
            .with_plugin(overlay);

        ui.add_sized(egui::vec2(larghezza, altezza), mappa);
        ui.small("Tile della mappa: (c) OpenStreetMap contributors");
    }

    fn pannello_messaggi(&mut self, ui: &mut egui::Ui, altezza: f32) {
        ui.heading("Messaggi");
        ui.small("Comunicazione testuale con il centro di controllo.");
        ui.separator();

        egui::ScrollArea::vertical()
            .id_salt("elenco_messaggi")
            .max_height((altezza - 80.0).max(120.0))
            .auto_shrink(false)
            .stick_to_bottom(true)
            .show(ui, |ui| {
                if self.messaggi.is_empty() {
                    ui.small("Nessun messaggio.");
                }
                for messaggio in &self.messaggi {
                    ui.label(messaggio);
                }
            });

        ui.separator();
        ui.horizontal(|ui| {
            let larghezza_campo = (ui.available_width() - 72.0).max(80.0);
            let campo = ui.add(
                egui::TextEdit::singleline(&mut self.campo_messaggio)
                    .desired_width(larghezza_campo)
                    .hint_text("messaggio per il server"),
            );
            let invio = campo.lost_focus()
                && ui.ctx().input(|i| i.key_pressed(egui::Key::Enter));

            if (ui.button("Invia").clicked() || invio)
                && !self.campo_messaggio.trim().is_empty()
            {
                let testo = self.campo_messaggio.trim().to_string();
                self.messaggi.push(format!("[tu -> server] {testo}"));
                self.campo_messaggio.clear();
                self.invia_comando(ComandoRete::InviaMessaggio(testo));
            }
        });
    }
}

impl eframe::App for GeoruggineApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.processa_eventi();

        ui.ctx().request_repaint_after(Duration::from_millis(500));

        egui::CentralPanel::default().show(ui, |ui| {
            if self.autenticato {
                self.vista_principale(ui);
            } else {
                self.vista_login(ui);
            }
        });
    }
}

// ===========================================================================
// main
// ===========================================================================

fn main() -> eframe::Result<()> {
    let indirizzo = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("GEORUGGINE_SERVER").ok())
        .unwrap_or_else(|| SERVER_DEFAULT.to_string());

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1240.0, 700.0])
            .with_min_inner_size([980.0, 560.0])
            .with_title("Georuggine - Dashboard veicolo"),
        ..Default::default()
    };

    eframe::run_native(
        "Georuggine",
        options,
        Box::new(move |cc| {
            let ctx = cc.egui_ctx.clone();

            let (tx_comandi, rx_comandi) = tokio::sync::mpsc::channel::<ComandoRete>(64);
            let (tx_eventi, rx_eventi) = channel::<EventoRete>();

            // Thread di rete con runtime Tokio a thread singolo.
            let ctx_rete = ctx.clone();
            let indirizzo_rete = indirizzo.clone();
            std::thread::Builder::new()
                .name("georuggine-net".to_string())
                .spawn(move || {
                    match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt.block_on(network_loop(
                            rx_comandi,
                            tx_eventi,
                            ctx_rete,
                            indirizzo_rete,
                        )),
                        Err(e) => eprintln!("[rete] impossibile creare il runtime: {e}"),
                    }
                })
                .expect("impossibile avviare il thread di rete");

            Ok(Box::new(GeoruggineApp::new(
                tx_comandi, rx_eventi, ctx, indirizzo,
            )) as Box<dyn eframe::App>)
        }),
    )
}
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_tempo_formati_validi() {
        assert_eq!(parse_tempo("90"), Some(90));
        assert_eq!(parse_tempo("01:30"), Some(90));
        assert_eq!(parse_tempo("00:01:30"), Some(90));
        assert_eq!(parse_tempo("01:00:00"), Some(3600));
        assert_eq!(parse_tempo("  45  "), Some(45)); // spazi ai margini tollerati
    }

    #[test]
    fn parse_tempo_formati_non_validi() {
        assert_eq!(parse_tempo("abc"), None);
        assert_eq!(parse_tempo(""), None);
        assert_eq!(parse_tempo("1:2:3:4"), None); // troppi segmenti
        assert_eq!(parse_tempo("1:ab"), None);
    }

    #[test]
    fn separa_campi_con_punto_e_virgola() {
        let campi = separa_campi("30;45.123;9.456");
        assert_eq!(campi, vec!["30", "45.123", "9.456"]);
    }

    #[test]
    fn separa_campi_con_virgola_decimale_italiana() {
     
        let campi = separa_campi("30\t45,123\t9,456");
        assert_eq!(campi, vec!["30", "45.123", "9.456"]);
    }

    #[test]
    fn separa_campi_senza_colonna_tempo() {
        let campi = separa_campi("45.0,9.0");
        assert_eq!(campi, vec!["45.0", "9.0"]);
    }

    #[test]
    fn separa_campi_con_spazi() {
        let campi = separa_campi("30 45.0 9.0");
        assert_eq!(campi, vec!["30", "45.0", "9.0"]);
    }

    #[test]
    fn separa_campi_formato_non_riconosciuto() {
        let campi = separa_campi("valorenonvalido");
        assert_eq!(campi, vec!["valorenonvalido"]);
    }
    fn app_di_test() -> GeoruggineApp {
    let (tx_net, _rx_comandi) = tokio::sync::mpsc::channel::<ComandoRete>(8);
    let (_tx_eventi, rx_net) = std::sync::mpsc::channel::<EventoRete>();
    GeoruggineApp::new(tx_net, rx_net, egui::Context::default(), "127.0.0.1:8080".to_string())
}

#[test]
fn valida_campi_password_troppo_corta() {
    let mut app = app_di_test();
    app.campo_user = "utente".to_string();
    app.campo_pass = "123".to_string(); // meno di 4 caratteri
    assert!(app.valida_campi().is_err());
}

#[test]
fn valida_campi_conferma_non_coincide_in_registrazione() {
    let mut app = app_di_test();
    app.campo_user = "utente".to_string();
    app.campo_pass = "1234".to_string();
    app.campo_pass_conferma = "5678".to_string();
    app.modo_registrazione = true;
    assert!(app.valida_campi().is_err());
}

#[test]
fn valida_campi_ok_in_login_senza_conferma() {
    let mut app = app_di_test();
    app.campo_user = "utente".to_string();
    app.campo_pass = "1234".to_string();
    app.modo_registrazione = false; // in login la conferma non viene controllata
    assert!(app.valida_campi().is_ok());
}
}