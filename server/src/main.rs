// # Georuggine - Server
//
// Server TCP asincrono che gestisce una flotta di veicoli, con una GUI per l'operatore:
//
// * registrazione e autenticazione degli utenti;
// * ricezione delle posizioni, una ogni 30 s;
// * monitoraggio utente;
// * analisi del movimento utente (velocità, tragitto ecc.) su giorno / settimana / mese corrente, selezionabile dal pannello;
// * comunicazione con gli utenti (broadcast o privata);
// * log periodico del tempo di CPU consumato dal processo.
//
// ## Architettura
//
// Come nel client, la GUI vive sul thread principale mentre tutta la rete/DB/logica asincrona gira su un
// thread separato con un proprio runtime Tokio. I due mondi comunicano con due canali:
// * `tokio::sync::mpsc` (GUI -> rete): comandi amministrativi e richieste di aggiornamento dello stato;
// * `std::sync::mpsc` (rete -> GUI): eventi da mostrare (log, utenti connessi, risultati di un'analisi, messaggi privati).

use rusqlite::{params, Connection, OptionalExtension};
use shared::{
    coordinate_valide, formatta_durata, haversine_km, normalizza_username, valida_username,
    ClientMessage, Intervallo, ServerMessage, Stato, TIMEOUT_FERMO_SEC,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};

use eframe::egui;
use walkers::{
    lat_lon, sources::OpenStreetMap, HttpTiles, Map, MapMemory, Plugin, Position, Projector,
};

// ---------------------------------------------------------------------------
// Costanti di configurazione
// ---------------------------------------------------------------------------

/// In ascolto su tutte le interfacce: il client puo' girare su un'altra macchina/piattaforma rispetto al server.
const INDIRIZZO_DEFAULT: &str = "0.0.0.0:8080";
const DB_PATH: &str = "georuggine.db";
const LOG_CPU_PATH: &str = "cpu_log.txt";
const PERIODO_LOG_SEC: u64 = 120;
/// Ogni quanto rivalutare la transizione "in movimento" -> "fermo" .
const PERIODO_CONTROLLO_STATO_SEC: u64 = 5;
const TOLLERANZA_COORD_GRADI: f64 = 1e-9;
const MAX_LEN_RIGA: usize = 8 * 1024;
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Ogni quanto la GUI aggiorna in automatico l'analisi dell'utente selezionato.
const POLL_INTERVAL_ANALISI_LIVE: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------------------
// Registro delle sessioni attive
// ---------------------------------------------------------------------------

/// Informazioni su un utente attualmente connesso.
struct Sessione {
    tx: mpsc::Sender<ServerMessage>,
    stato: Stato,
    ultima_pos: Option<(f64, f64)>,
    connesso_da: Instant,
    inizio_tragitto: i64,
}

type Registro = Arc<Mutex<HashMap<String, Sessione>>>;

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

/// Apre una connessione a SQLite.
fn apri_db() -> rusqlite::Result<Connection> {
    let conn = Connection::open(DB_PATH)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    // PRAGMA journal_mode restituisce una riga, quindi va letto con query_row.
    let _: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(conn)
}

/// Crea lo schema se non esiste.
fn init_db() -> rusqlite::Result<()> {
    let conn = apri_db()?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS Utenti (
             id            INTEGER PRIMARY KEY,
             username      TEXT NOT NULL UNIQUE,
             password_hash TEXT NOT NULL,
             creato_il     DATETIME DEFAULT CURRENT_TIMESTAMP
         );
         CREATE TABLE IF NOT EXISTS Posizioni (
             id        INTEGER PRIMARY KEY,
             id_utente INTEGER NOT NULL,
             lat       REAL NOT NULL,
             lon       REAL NOT NULL,
             timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
             FOREIGN KEY(id_utente) REFERENCES Utenti(id)
         );
         CREATE INDEX IF NOT EXISTS idx_posizioni_utente_ts
             ON Posizioni(id_utente, timestamp);",
    )?;
    Ok(())
}

fn id_utente(conn: &Connection, username: &str) -> rusqlite::Result<Option<i64>> {
    conn.query_row(
        "SELECT id FROM Utenti WHERE username = ?1",
        params![username],
        |r| r.get(0),
    )
    .optional()
}

/// Elenco di tutti gli username registrati, in ordine alfabetico.
fn elenco_utenti_registrati(conn: &Connection) -> Vec<String> {
    let mut stmt = match conn.prepare("SELECT username FROM Utenti ORDER BY username") {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[gui] impossibile leggere l'elenco utenti: {e}");
            return Vec::new();
        }
    };
    let risultato = match stmt.query_map([], |r| r.get::<_, String>(0)) {
        Ok(righe) => righe.filter_map(Result::ok).collect(),
        Err(e) => {
            eprintln!("[gui] impossibile leggere l'elenco utenti: {e}");
            Vec::new()
        }
    };
    risultato
}

/// Carica dal database la traccia di un utente a partire da un certo istante.
fn carica_traccia(conn: &Connection, uid: i64, soglia_unix: i64) -> Vec<(f64, f64)> {
    let mut stmt = match conn.prepare(
        "SELECT lat, lon FROM Posizioni
          WHERE id_utente = ?1 AND CAST(strftime('%s', timestamp) AS INTEGER) >= ?2
          ORDER BY timestamp ASC",
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[gui] impossibile leggere la traccia: {e}");
            return Vec::new();
        }
    };
    let risultato = stmt.query_map(params![uid, soglia_unix], |r| {
        Ok((r.get::<_, f64>(0)?, r.get::<_, f64>(1)?))
    });
    match risultato {
        Ok(righe) => righe.filter_map(Result::ok).collect(),
        Err(e) => {
            eprintln!("[gui] impossibile leggere la traccia: {e}");
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// Analisi del movimento
// ---------------------------------------------------------------------------

/// Risultato dell'analisi del tragitto di un utente.
struct Analisi {
    tragitto_km: f64,
    velocita_media_kmh: f64,
    movimento_sec: u64,
    pause_sec: u64,
    campioni: u64,
}

/// Espressione SQL che individua l'inizio dell'intervallo richiesto.
///
/// Nota: i timestamp sono salvati in UTC (`CURRENT_TIMESTAMP`), quindi anche i
/// confronti avvengono in UTC. `date('now','weekday 0','-6 days')` restituisce
/// il lunedi' della settimana ISO corrente.
fn inizio_intervallo_sql(intervallo: Intervallo) -> &'static str {
    match intervallo {
        Intervallo::Giorno => "date('now','start of day')",
        Intervallo::Settimana => "date('now','weekday 0','-6 days')",
        Intervallo::Mese => "date('now','start of month')",
    }
}

/// Calcola tragitto percorso, velocita' media, durata del movimento e delle pause per un utente nell'intervallo richiesto.
fn analizza(conn: &Connection, uid: i64, intervallo: Intervallo) -> rusqlite::Result<Analisi> {
    const SOGLIA_MOVIMENTO_KM: f64 = 0.010;
    const GAP_MAX_SEC: i64 = 300;

    let sql = format!(
        "SELECT lat, lon, CAST(strftime('%s', timestamp) AS INTEGER)
           FROM Posizioni
          WHERE id_utente = ?1 AND timestamp >= {}
          ORDER BY timestamp ASC",
        inizio_intervallo_sql(intervallo)
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut righe = stmt.query(params![uid])?;

    let mut tragitto_km = 0.0_f64;
    let mut movimento_sec = 0_i64;
    let mut pause_sec = 0_i64;
    let mut campioni = 0_u64;
    let mut precedente: Option<(f64, f64, i64)> = None;

    while let Some(riga) = righe.next()? {
        let lat: f64 = riga.get(0)?;
        let lon: f64 = riga.get(1)?;
        let ts: i64 = riga.get(2)?;
        campioni += 1;

        if let Some((p_lat, p_lon, p_ts)) = precedente {
            let dt = ts - p_ts;
            if dt > 0 && dt <= GAP_MAX_SEC {
                let dist = haversine_km(p_lat, p_lon, lat, lon);
                if dist >= SOGLIA_MOVIMENTO_KM {
                    tragitto_km += dist;
                    movimento_sec += dt;
                } else {
                    pause_sec += dt;
                }
            }
        }
        precedente = Some((lat, lon, ts));
    }

    let velocita_media_kmh = if movimento_sec > 0 {
        tragitto_km / (movimento_sec as f64 / 3600.0)
    } else {
        0.0
    };

    Ok(Analisi {
        tragitto_km,
        velocita_media_kmh,
        movimento_sec: movimento_sec as u64,
        pause_sec: pause_sec as u64,
        campioni,
    })
}

// ---------------------------------------------------------------------------
// Log delle prestazioni
// ---------------------------------------------------------------------------

/// Converte un contatore di giorni nella data
fn data_civile(giorni: i64) -> (i64, u32, u32) {
    let z = giorni + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let anno = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let giorno = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let mese = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if mese <= 2 { anno + 1 } else { anno }, mese, giorno)
}

/// Data e ora correnti.
fn timestamp_utc() -> String {
    let secondi = ora_unix() as u64;
    let (anno, mese, giorno) = data_civile((secondi / 86_400) as i64);
    let resto = secondi % 86_400;
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        anno,
        mese,
        giorno,
        resto / 3600,
        (resto % 3600) / 60,
        resto % 60
    )
}

/// Istante attuale come numero di secondi dall'epoch Unix. Usato per marcare l'inizio di un tragitto.
fn ora_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Scrive su `cpu_log.txt`, ogni 2 minuti, i dettagli sul tempo di CPU consumato dal processo server.
async fn logger_cpu(registro: Registro) {
    use sysinfo::{Pid, System};

    let pid = Pid::from_u32(std::process::id());
    let mut sys = System::new();
    sys.refresh_process(pid); // primo campione: azzera il contatore

    let avvio = Instant::now();
    let mut ultimo_campione = Instant::now();
    let mut cpu_time_totale_sec = 0.0_f64;

    scrivi_log(&format!(
        "\n=== avvio server {} | pid {} ===\n",
        timestamp_utc(),
        std::process::id()
    ))
    .await;

    let mut ticker = tokio::time::interval(Duration::from_secs(PERIODO_LOG_SEC));
    ticker.tick().await; // il primo tick di `interval` scatta subito: lo scarto

    loop {
        ticker.tick().await;

        let dt = ultimo_campione.elapsed().as_secs_f64();
        ultimo_campione = Instant::now();

        sys.refresh_process(pid);
        let (uso_pct, ram_bytes) = match sys.process(pid) {
            Some(p) => (p.cpu_usage() as f64, p.memory()),
            None => (0.0, 0),
        };
        cpu_time_totale_sec += uso_pct / 100.0 * dt;

        let connessi = registro.lock().await.len();

        let riga = format!(
            "[{}] uptime={} | CPU intervallo={:>6.2}% | CPU time totale={:>8.3}s | RAM={:>7.2} MB | client connessi={}\n",
            timestamp_utc(),
            formatta_durata(avvio.elapsed().as_secs()),
            uso_pct,
            cpu_time_totale_sec,
            ram_bytes as f64 / (1024.0 * 1024.0),
            connessi
        );

        scrivi_log(&riga).await;
    }
}

/// Accoda una riga al file di log delle prestazioni.
async fn scrivi_log(testo: &str) {
    match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(LOG_CPU_PATH)
        .await
    {
        Ok(mut file) => {
            if let Err(e) = file.write_all(testo.as_bytes()).await {
                eprintln!("[log] impossibile scrivere su {LOG_CPU_PATH}: {e}");
            }
        }
        Err(e) => eprintln!("[log] impossibile aprire {LOG_CPU_PATH}: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Invio messaggi agli utenti connessi
// ---------------------------------------------------------------------------

async fn invia_a_utente(registro: &Registro, utente: &str, msg: ServerMessage) -> bool {
    let reg = registro.lock().await;
    match reg.get(utente) {
        Some(s) => s.tx.send(msg).await.is_ok(),
        None => false,
    }
}

async fn invia_broadcast(registro: &Registro, msg: ServerMessage) -> usize {
    let reg = registro.lock().await;
    let mut inviati = 0;
    for sessione in reg.values() {
        if sessione.tx.send(msg.clone()).await.is_ok() {
            inviati += 1;
        }
    }
    inviati
}

// ---------------------------------------------------------------------------
// Comandi amministrativi (barra comandi della GUI)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ComandoAmministrazione {
    Msg { destinatario: String, testo: String },
    All { testo: String },
    Aiuto,
    Stop,
    Vuoto,
    Sconosciuto { testo: String },
}

const TESTO_AIUTO: &str = "Comandi disponibili:\n\
  /all <testo>            messaggio in broadcast a tutti i connessi\n\
  /stop                   arresta il server\n\
  /aiuto                  mostra questo elenco\n\
\n\
";

/// Interpreta una riga digitata nella barra comandi della GUI.
fn interpreta_comando(riga: &str) -> ComandoAmministrazione {
    let riga = riga.trim();
    if riga.is_empty() {
        return ComandoAmministrazione::Vuoto;
    }

    let (comando, resto) = match riga.split_once(char::is_whitespace) {
        Some((c, r)) => (c.to_lowercase(), r.trim().to_string()),
        None => (riga.to_lowercase(), String::new()),
    };

    match comando.as_str() {
        "/aiuto" | "/help" | "/?" => ComandoAmministrazione::Aiuto,
        "/stop" | "/exit" | "/quit" => ComandoAmministrazione::Stop,
        "/all" | "/broadcast" => ComandoAmministrazione::All { testo: resto },

        _ => ComandoAmministrazione::Sconosciuto {
            testo: riga.to_string(),
        },
    }
}

/// Esegue un comando amministrativo.
async fn esegui_comando_admin(
    registro: &Registro,
    comando: ComandoAmministrazione,
) -> Vec<String> {
    match comando {
        ComandoAmministrazione::Vuoto => Vec::new(),

        ComandoAmministrazione::Aiuto => vec![TESTO_AIUTO.to_string()],

        ComandoAmministrazione::Stop => {
            println!("Arresto del server richiesto dalla GUI. Chiusura in corso...");
            std::process::exit(0);
        }

        ComandoAmministrazione::All { testo } => {
            if testo.is_empty() {
                return vec!["Uso: /all <testo>".to_string()];
            }
            let n = invia_broadcast(
                registro,
                ServerMessage::MessaggioDalServer {
                    testo,
                    broadcast: true,
                },
            )
            .await;
            vec![format!("Broadcast inviato a {n} utente/i.")]
        }

        ComandoAmministrazione::Msg {
            destinatario,
            testo,
        } => {
            let ok = invia_a_utente(
                registro,
                &destinatario,
                ServerMessage::MessaggioDalServer {
                    testo,
                    broadcast: false,
                },
            )
            .await;
            let riga = if ok {
                format!("Messaggio privato consegnato a '{destinatario}'.")
            } else {
                format!("Utente '{destinatario}' non connesso.")
            };
            vec![riga]
        }

        ComandoAmministrazione::Sconosciuto { testo } => {
            vec![format!(
                "Comando non riconosciuto: '{testo}'. Digita /aiuto per l'elenco dei comandi."
            )]
        }
    }
}

// ---------------------------------------------------------------------------
// Comunicazione GUI <-> thread di rete
// ---------------------------------------------------------------------------

/// Comandi che la GUI invia al thread di rete.
enum ComandoAdminGui {
    /// Richiede una fotografia aggiornata degli utenti attualmente connessi.
    RichiediSnapshot,
    /// Esegue un comando amministrativo (barra comandi o chat privata).
    Esegui { comando: ComandoAmministrazione },
    /// Richiede l'analisi di un utente per l'intervallo scelto nel selettore
    /// del pannello di destra.
    RichiediAnalisi { utente: String, intervallo: Intervallo },
}

/// Serve alla GUI per disegnare lista e mappa.
#[derive(Clone, Copy)]
struct SessioneSnapshot {
    stato: Stato,
    ultima_pos: Option<(f64, f64)>,
    inizio_tragitto: i64,
}

/// Eventi che il thread di rete notifica alla GUI.
enum EventoServerGui {
    /// Stato di tutti gli utenti attualmente connessi.
    Snapshot(HashMap<String, SessioneSnapshot>),
    /// Righe di testo da accodare al terminale della GUI.
    Output(Vec<String>),
    /// Risultato di una richiesta di analisi.
    AnalisiPronta {
        utente: String,
        intervallo: Intervallo,
        dati: Analisi,
    },
    /// Messaggio privato ricevuto da un utente.
    MessaggioPrivato { utente: String, testo: String },
}

// ---------------------------------------------------------------------------
// Gestione di una singola connessione
// ---------------------------------------------------------------------------

async fn gestisci_connessione(
    socket: tokio::net::TcpStream,
    registro: Registro,
    tx_eventi: std::sync::mpsc::Sender<EventoServerGui>,
) {
    let _ = socket.set_nodelay(true);

    let conn = match apri_db() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[conn] impossibile aprire il database: {e}");
            return;
        }
    };

    let (reader, mut writer) = tokio::io::split(socket);
    let mut righe = BufReader::new(reader).lines();
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(64);

    // Task dedicato alla scrittura.
    let task_scrittura = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let Ok(mut testo) = serde_json::to_string(&msg) else {
                continue;
            };
            testo.push('\n');
            if writer.write_all(testo.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    // Stato della sessione, locale al task: non serve alcun lock per gestirlo.
    let mut utente: Option<String> = None;
    let mut uid: Option<i64> = None;
    let mut stato = Stato::Sconnesso;
    let mut ultima_pos: Option<(f64, f64)> = None;
    let mut ultimo_cambio_pos = Instant::now();

    let mut controllo_stato =
        tokio::time::interval(Duration::from_secs(PERIODO_CONTROLLO_STATO_SEC));

    loop {
        tokio::select! {
            // Messaggio in arrivo dal client
            letto = righe.next_line() => {
                let riga = match letto {
                    Ok(Some(r)) => r,
                    _ => break,
                };
                if riga.trim().is_empty() {
                    continue;
                }
                if riga.len() > MAX_LEN_RIGA {
                    eprintln!("[conn] riga troppo lunga, connessione chiusa");
                    break;
                }

                let Ok(msg) = serde_json::from_str::<ClientMessage>(riga.trim()) else {
                    eprintln!("[conn] messaggio non riconosciuto, ignorato");
                    continue;
                };

                match msg {
                    // REGISTRAZIONE
                    ClientMessage::Register { user, password_hash } => {
                        let esito = match valida_username(&user) {
                            Err(e) => Err(e),
                            Ok(_) if password_hash.trim().is_empty() => {
                                Err("La password non puo' essere vuota.".to_string())
                            }
                            Ok(nome) => match id_utente(&conn, &nome) {
                                Err(e) => Err(format!("Errore del database: {e}")),
                                Ok(Some(_)) => Err("Nome utente gia' in uso.".to_string()),
                                Ok(None) => {
                                    
                                    let inserimento = conn.execute(
                                        "INSERT INTO Utenti (username, password_hash) VALUES (?1, ?2)",
                                        params![nome, password_hash],
                                    );
                                    match inserimento {
                                        Ok(_) => Ok(nome),
                                        Err(e) => Err(format!("Errore del database: {e}")),
                                    }
                                }
                            },
                        };

                        let msg = match &esito {
                            Ok(nome) => {
                                println!("[+] Nuovo utente registrato: '{nome}'");
                                ServerMessage::RegisterResult {
                                    success: true,
                                    messaggio: "Registrazione completata: ora puoi accedere.".to_string(),
                                }
                            }
                            Err(e) => ServerMessage::RegisterResult {
                                success: false,
                                messaggio: e.clone(),
                            },
                        };
                        let _ = tx.send(msg).await;
                    }

                    // LOGIN
                    ClientMessage::Login { user, password_hash } => {
                        if utente.is_some() {
                            continue; 
                        }
                        let nome = normalizza_username(&user);

                        let credenziali: Option<(i64, String)> = conn
                            .query_row(
                                "SELECT id, password_hash FROM Utenti WHERE username = ?1",
                                params![nome],
                                |r| Ok((r.get(0)?, r.get(1)?)),
                            )
                            .optional()
                            .unwrap_or(None);

                        let gia_connesso = registro.lock().await.contains_key(&nome);

                        match credenziali {
                            Some((id, hash)) if hash == password_hash && !gia_connesso => {
                                utente = Some(nome.clone());
                                uid = Some(id);
                                stato = Stato::Fermo; // connesso ma ancora senza spostamenti
                                ultimo_cambio_pos = Instant::now();
                                ultima_pos = None;

                                registro.lock().await.insert(
                                    nome.clone(),
                                    Sessione {
                                        tx: tx.clone(),
                                        stato,
                                        ultima_pos: None,
                                        connesso_da: Instant::now(),
                                        inizio_tragitto: ora_unix(),
                                    },
                                );

                                println!("[+] '{nome}' connesso (stato iniziale: {stato})");
                                let _ = tx
                                    .send(ServerMessage::AuthResult {
                                        success: true,
                                        messaggio: format!("Benvenuto, {nome}."),
                                    })
                                    .await;
                                let _ = tx
                                    .send(ServerMessage::StatoAggiornato {
                                        stato,
                                        lat: None,
                                        lon: None,
                                    })
                                    .await;
                            }
                            Some((_, hash)) if hash == password_hash => {
                                let _ = tx
                                    .send(ServerMessage::AuthResult {
                                        success: false,
                                        messaggio: "Utente gia' connesso da un altro dispositivo."
                                            .to_string(),
                                    })
                                    .await;
                            }
                            _ => {
                                let _ = tx
                                    .send(ServerMessage::AuthResult {
                                        success: false,
                                        messaggio: "Credenziali non valide.".to_string(),
                                    })
                                    .await;
                            }
                        }
                    }

                    // UPDATE POSIZIONE
                    ClientMessage::PositionUpdate { lat, lon } => {
                        let (Some(id), Some(nome)) = (uid, utente.clone()) else {
                            continue; // posizioni accettate solo da utenti autenticati
                        };
                        if !coordinate_valide(lat, lon) {
                            eprintln!("[conn] coordinate non valide da '{nome}', scartate");
                            continue;
                        }

                        if let Err(e) = conn.execute(
                            "INSERT INTO Posizioni (id_utente, lat, lon) VALUES (?1, ?2, ?3)",
                            params![id, lat, lon],
                        ) {
                            eprintln!("[conn] errore di scrittura della posizione: {e}");
                        }

                        
                        //fermo -> in movimento al PRIMO cambio di coordinata;
                        //in movimento -> fermo dopo 3 minuti senza variazioni.
                        let cambiata = match ultima_pos {
                            None => false,
                            Some((p_lat, p_lon)) => {
                                (p_lat - lat).abs() > TOLLERANZA_COORD_GRADI
                                    || (p_lon - lon).abs() > TOLLERANZA_COORD_GRADI
                            }
                        };

                        if cambiata {
                            ultimo_cambio_pos = Instant::now();
                            if stato != Stato::InMovimento {
                                stato = Stato::InMovimento;
                                println!("[>] '{nome}': ora IN MOVIMENTO");
                            }
                        } else if ultima_pos.is_none() {
                            ultimo_cambio_pos = Instant::now();
                        } else if stato == Stato::InMovimento
                            && ultimo_cambio_pos.elapsed() >= Duration::from_secs(TIMEOUT_FERMO_SEC)
                        {
                            stato = Stato::Fermo;
                            println!("[.] '{nome}': ora FERMO (3 minuti senza variazioni)");
                        }

                        ultima_pos = Some((lat, lon));

                        if let Some(s) = registro.lock().await.get_mut(&nome) {
                            s.stato = stato;
                            s.ultima_pos = ultima_pos;
                        }

                        let _ = tx
                            .send(ServerMessage::StatoAggiornato {
                                stato,
                                lat: Some(lat),
                                lon: Some(lon),
                            })
                            .await;
                    }

                    // RICHIESTA ANALISI
                    ClientMessage::RichiestaAnalisi(intervallo) => {
                        let Some(id) = uid else { continue };
                        match analizza(&conn, id, intervallo) {
                            Ok(a) => {
                                let _ = tx
                                    .send(ServerMessage::AnalisiResult {
                                        intervallo,
                                        tragitto_km: a.tragitto_km,
                                        velocita_media_kmh: a.velocita_media_kmh,
                                        movimento_sec: a.movimento_sec,
                                        pause_sec: a.pause_sec,
                                        campioni: a.campioni,
                                    })
                                    .await;
                            }
                            Err(e) => eprintln!("[conn] errore durante l'analisi: {e}"),
                        }
                    }

                    // CHAT UTENTE
                    ClientMessage::ChatMessage { testo } => {
                        let Some(nome) = utente.as_deref() else { continue };
                        let testo = testo.trim();
                        if !testo.is_empty() {
                            println!("[msg] {nome}: {testo}");

                            let _ = tx_eventi.send(EventoServerGui::MessaggioPrivato {
                                utente: nome.to_string(),
                                testo: testo.to_string(),
                            });
                        }
                    }

                    // PERCORSO AVVIATO
                    ClientMessage::PercorsoAvviato => {
                        let Some(nome) = utente.clone() else { continue };
                        if let Some(s) = registro.lock().await.get_mut(&nome) {
                            s.inizio_tragitto = ora_unix();
                        }
                        println!("[>] '{nome}': nuovo tragitto avviato (traccia sulla mappa riazzerata)");
                    }

                    // LOGOUT
                    ClientMessage::Logout => {
                        
                        let Some(nome) = utente.take() else { continue };
                        registro.lock().await.remove(&nome);
                        println!("[-] '{nome}' disconnesso (logout esplicito)");

                        uid = None;
                        stato = Stato::Sconnesso;
                        ultima_pos = None;
                        ultimo_cambio_pos = Instant::now();

                        let _ = tx.send(ServerMessage::LogoutOk).await;
                    }
                }
            }

            // Controllo della transizione verso "fermo"
            _ = controllo_stato.tick() => {
                if stato == Stato::InMovimento
                    && ultimo_cambio_pos.elapsed() >= Duration::from_secs(TIMEOUT_FERMO_SEC)
                {
                    stato = Stato::Fermo;
                    if let Some(nome) = utente.clone() {
                        println!("[.] '{nome}': ora FERMO (3 minuti senza variazioni)");
                        if let Some(s) = registro.lock().await.get_mut(&nome) {
                            s.stato = stato;
                        }
                    }
                    let (lat, lon) = match ultima_pos {
                        Some((la, lo)) => (Some(la), Some(lo)),
                        None => (None, None),
                    };
                    let _ = tx.send(ServerMessage::StatoAggiornato { stato, lat, lon }).await;
                }
            }
        }
    }

    // Chiusura ordinata.
    if let Some(nome) = utente {
        registro.lock().await.remove(&nome);
        println!("[-] '{nome}' disconnesso (stato: {})", Stato::Sconnesso);
    }
    drop(tx);
    task_scrittura.abort();
}

// ---------------------------------------------------------------------------
// Thread di rete: accept loop + gestione dei comandi dalla GUI
// ---------------------------------------------------------------------------

async fn avvia_rete(
    indirizzo: String,
    mut rx_admin: mpsc::Receiver<ComandoAdminGui>,
    tx_eventi: std::sync::mpsc::Sender<EventoServerGui>,
) {
    if let Err(e) = init_db() {
        eprintln!("[main] impossibile inizializzare il database: {e}");
        let _ = tx_eventi.send(EventoServerGui::Output(vec![format!(
            "Impossibile inizializzare il database: {e}"
        )]));
        return;
    }
    println!("Database '{DB_PATH}' pronto.");

    let conn_comandi = match apri_db() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[main] impossibile aprire il database per i comandi: {e}");
            return;
        }
    };

    let registro: Registro = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(logger_cpu(Arc::clone(&registro)));

    let listener = match TcpListener::bind(&indirizzo).await {
        Ok(l) => l,
        Err(e) => {
            let _ = tx_eventi.send(EventoServerGui::Output(vec![format!(
                "Impossibile aprire la porta '{indirizzo}': {e}"
            )]));
            return;
        }
    };
    println!("Server Georuggine in ascolto su {indirizzo}");
    println!("Log delle prestazioni: '{LOG_CPU_PATH}' (aggiornato ogni {PERIODO_LOG_SEC} s)");

    loop {
        tokio::select! {
            accettata = listener.accept() => {
                match accettata {
                    Ok((socket, addr)) => {
                        println!("[~] Nuova connessione da {addr}");
                        tokio::spawn(gestisci_connessione(socket, Arc::clone(&registro), tx_eventi.clone()));
                    }
                    Err(e) => eprintln!("[main] accept fallita: {e}"),
                }
            }

            comando = rx_admin.recv() => {
                let Some(comando) = comando else { break };
                match comando {
                    ComandoAdminGui::RichiediSnapshot => {
                        let reg = registro.lock().await;
                        let snapshot: HashMap<String, SessioneSnapshot> = reg
                            .iter()
                            .map(|(nome, s)| {
                                (
                                    nome.clone(),
                                    SessioneSnapshot {
                                        stato: s.stato,
                                        ultima_pos: s.ultima_pos,
                                        inizio_tragitto: s.inizio_tragitto,
                                    },
                                )
                            })
                            .collect();
                        drop(reg);
                        let _ = tx_eventi.send(EventoServerGui::Snapshot(snapshot));
                    }

                    ComandoAdminGui::Esegui { comando } => {
                        let righe = esegui_comando_admin(&registro, comando).await;
                        if !righe.is_empty() {
                            let _ = tx_eventi.send(EventoServerGui::Output(righe));
                        }
                    }

                    ComandoAdminGui::RichiediAnalisi { utente, intervallo } => {
                        match id_utente(&conn_comandi, &utente) {
                            Ok(Some(uid)) => match analizza(&conn_comandi, uid, intervallo) {
                                Ok(dati) => {
                                    let _ = tx_eventi.send(EventoServerGui::AnalisiPronta {
                                        utente,
                                        intervallo,
                                        dati,
                                    });
                                }
                                Err(e) => eprintln!("[gui] errore durante l'analisi: {e}"),
                            },
                            Ok(None) => { // Utente non più registrato.
                            }
                            Err(e) => eprintln!("[gui] errore di lettura dal database: {e}"),
                        }
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                println!("\nArresto del server richiesto (Ctrl+C). Chiusura in corso...");
                std::process::exit(0);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Overlay della mappa 
// ---------------------------------------------------------------------------

struct TracciaServer {
    traccia: Vec<Position>,
    attuale: Position,
    in_movimento: bool,
}

impl Plugin for TracciaServer {
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

        if let Some(partenza) = punti.first() {
            painter.circle_filled(*partenza, 6.0, egui::Color32::from_rgb(40, 160, 80));
            painter.circle_stroke(*partenza, 6.0, egui::Stroke::new(1.5, egui::Color32::WHITE));
        }

        let centro = projector.project(self.attuale).to_pos2();
        let colore = if self.in_movimento {
            egui::Color32::from_rgb(40, 200, 60)
        } else {
            egui::Color32::from_rgb(235, 175, 30)
        };
        painter.circle_filled(centro, 10.0, colore);
        painter.circle_stroke(centro, 10.0, egui::Stroke::new(2.0, egui::Color32::WHITE));
    }
}

// ---------------------------------------------------------------------------
// GUI dell'operatore
// ---------------------------------------------------------------------------

struct ServerApp {
    tx_admin: mpsc::Sender<ComandoAdminGui>,
    rx_eventi: std::sync::mpsc::Receiver<EventoServerGui>,

    conn_lettura: Connection,
    indirizzo: String,

    utenti_registrati: Vec<String>,
    stato_online: HashMap<String, SessioneSnapshot>,
    ultimo_poll: Instant,

    utente_selezionato: Option<String>,
    /// Traccia dell'utente selezionato, ricostruita dal database
    traccia_live: Vec<(f64, f64)>,

    /// Intervallo attualmente scelto nel selettore del pannello di analisi (giorno / settimana / mese).
    intervallo_selezionato: Intervallo,
    /// Ultimo risultato di analisi ricevuto
    analisi_corrente: Option<(String, Intervallo, Analisi)>,
    ultimo_poll_analisi: Instant,

    /// Cronologia dei messaggi privati scambiati con ciascun utente
    chat_privata: HashMap<String, Vec<String>>,
    campo_chat: String,

    log: Vec<String>,
    campo_comando: String,

    tiles: HttpTiles,
    map_memory: MapMemory,
}

impl ServerApp {
    fn new(
        tx_admin: mpsc::Sender<ComandoAdminGui>,
        rx_eventi: std::sync::mpsc::Receiver<EventoServerGui>,
        conn_lettura: Connection,
        indirizzo: String,
        ctx: egui::Context,
    ) -> Self {
        let mut map_memory = MapMemory::default();
        let _ = map_memory.set_zoom(9.0);

        let messaggio_iniziale = format!("Server in ascolto su {indirizzo}.");

        let mut app = Self {
            tx_admin,
            rx_eventi,
            conn_lettura,
            indirizzo,
            utenti_registrati: Vec::new(),
            stato_online: HashMap::new(),
            ultimo_poll: Instant::now()
                .checked_sub(POLL_INTERVAL)
                .unwrap_or_else(Instant::now),
            utente_selezionato: None,
            traccia_live: Vec::new(),
            intervallo_selezionato: Intervallo::Giorno,
            analisi_corrente: None,
            ultimo_poll_analisi: Instant::now()
                .checked_sub(POLL_INTERVAL_ANALISI_LIVE)
                .unwrap_or_else(Instant::now),
            chat_privata: HashMap::new(),
            campo_chat: String::new(),
            log: vec![messaggio_iniziale, TESTO_AIUTO.to_string()],
            campo_comando: String::new(),
            tiles: HttpTiles::new(OpenStreetMap, ctx),
            map_memory,
        };
        app.aggiorna_utenti_registrati();
        app
    }

    fn aggiorna_utenti_registrati(&mut self) {
        self.utenti_registrati = elenco_utenti_registrati(&self.conn_lettura);
    }

    fn invia_comando_rete(&mut self, comando: ComandoAdminGui) {
        if self.tx_admin.try_send(comando).is_err() {
            self.log
                .push("Il canale verso il thread di rete non risponde.".to_string());
        }
    }

    /// Ricostruisce dal database la traccia di un utente a partire da un certo istante Unix (l'inizio del suo tragitto in corso).
    fn carica_traccia_utente(&self, nome: &str, soglia_unix: i64) -> Vec<(f64, f64)> {
        match id_utente(&self.conn_lettura, nome) {
            Ok(Some(uid)) => carica_traccia(&self.conn_lettura, uid, soglia_unix),
            _ => Vec::new(),
        }
    }

    /// Cambia l'utente selezionato nella lista.
    fn seleziona_utente(&mut self, nome: String) {
        if self.utente_selezionato.as_deref() != Some(nome.as_str()) {
            let soglia = self
                .stato_online
                .get(&nome)
                .map(|s| s.inizio_tragitto)
                .unwrap_or(0);
            self.traccia_live = self.carica_traccia_utente(&nome, soglia);
            self.utente_selezionato = Some(nome);
            self.analisi_corrente = None;
            self.ultimo_poll_analisi = Instant::now()
                .checked_sub(POLL_INTERVAL_ANALISI_LIVE)
                .unwrap_or_else(Instant::now);
        }
    }

    /// Cambia l'intervallo del selettore di analisi.
    fn seleziona_intervallo(&mut self, intervallo: Intervallo) {
        if self.intervallo_selezionato != intervallo {
            self.intervallo_selezionato = intervallo;
            self.analisi_corrente = None;
            self.ultimo_poll_analisi = Instant::now()
                .checked_sub(POLL_INTERVAL_ANALISI_LIVE)
                .unwrap_or_else(Instant::now);
        }
    }

    fn invia_comando_testo(&mut self) {
        let testo = self.campo_comando.trim().to_string();
        if testo.is_empty() {
            return;
        }
        self.log.push(format!("> {testo}"));
        self.campo_comando.clear();

        let comando = interpreta_comando(&testo);
        self.invia_comando_rete(ComandoAdminGui::Esegui { comando });
    }

    fn processa_eventi(&mut self) {
        while let Ok(evento) = self.rx_eventi.try_recv() {
            match evento {
                EventoServerGui::Snapshot(mappa) => {
                    if let Some(nome) = self.utente_selezionato.clone() {
                        let vecchia_soglia =
                            self.stato_online.get(&nome).map(|s| s.inizio_tragitto);
                        if let Some(s) = mappa.get(&nome) {
                            if vecchia_soglia != Some(s.inizio_tragitto) {
                                // Il veicolo ha avviato un nuovo tragitto. 
                                self.traccia_live =
                                    self.carica_traccia_utente(&nome, s.inizio_tragitto);
                            } else if let Some(pos) = s.ultima_pos {
                                if self.traccia_live.last() != Some(&pos) {
                                    self.traccia_live.push(pos);
                                }
                            }
                        }
                    }
                    self.stato_online = mappa;
                }
                EventoServerGui::Output(righe) => self.log.extend(righe),
                EventoServerGui::AnalisiPronta {
                    utente,
                    intervallo,
                    dati,
                } => {
                    self.analisi_corrente = Some((utente, intervallo, dati));
                }
                EventoServerGui::MessaggioPrivato { utente, testo } => {
                    self.chat_privata
                        .entry(utente.clone())
                        .or_default()
                        .push(format!("[{utente} -> tu] {testo}"));
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Pannelli
    // -----------------------------------------------------------------------

    fn pannello_utenti(&mut self, ui: &mut egui::Ui) {
        ui.heading("Utenti registrati");
        ui.small(format!("In ascolto su {}", self.indirizzo));
        ui.separator();

        egui::ScrollArea::vertical()
            .id_salt("elenco_utenti_registrati")
            .auto_shrink(false)
            .show(ui, |ui| {
                if self.utenti_registrati.is_empty() {
                    ui.weak("Nessun utente registrato.");
                }
                for nome in self.utenti_registrati.clone() {
                    let sessione = self.stato_online.get(&nome).copied();
                    let selezionato = self.utente_selezionato.as_deref() == Some(nome.as_str());

                    ui.horizontal(|ui| {
                        if ui.selectable_label(selezionato, &nome).clicked() {
                            self.seleziona_utente(nome.clone());
                        }
                        match sessione {
                            Some(s) => {
                                ui.colored_label(egui::Color32::from_rgb(60, 170, 90), "online");
                                // Accanto a "online" si vede anche se il veicolo è in movimento o fermo
                                let (testo, colore) = match s.stato {
                                    Stato::InMovimento => {
                                        ("in movimento", egui::Color32::from_rgb(40, 200, 60))
                                    }
                                    Stato::Fermo => {
                                        ("fermo", egui::Color32::from_rgb(220, 170, 40))
                                    }
                                    Stato::Sconnesso => ("-", egui::Color32::GRAY),
                                };
                                ui.colored_label(colore, testo);
                            }
                            None => {
                                ui.colored_label(egui::Color32::GRAY, "offline");
                            }
                        }
                    });
                }
            });
    }

    fn pannello_analisi(&mut self, ui: &mut egui::Ui) {
        ui.heading("Analisi");
        ui.separator();

        let Some(nome_selezionato) = self.utente_selezionato.clone() else {
            ui.weak("Seleziona un utente dalla lista a sinistra.");
            return;
        };

        ui.label(format!("Utente: {nome_selezionato}"));
        ui.add_space(6.0);

        ui.horizontal(|ui| {
            let mut scelto = self.intervallo_selezionato;
            let cambiato_g = ui
                .selectable_value(&mut scelto, Intervallo::Giorno, "Giorno corrente")
                .clicked();
            let cambiato_s = ui
                .selectable_value(&mut scelto, Intervallo::Settimana, "Settimana")
                .clicked();
            let cambiato_m = ui
                .selectable_value(&mut scelto, Intervallo::Mese, "Mese")
                .clicked();
            if cambiato_g || cambiato_s || cambiato_m {
                self.seleziona_intervallo(scelto);
            }
        });
        ui.add_space(8.0);

        match &self.analisi_corrente {
            Some((utente, intervallo, dati))
                if utente == &nome_selezionato && *intervallo == self.intervallo_selezionato =>
            {
                ui.label(egui::RichText::new(intervallo.etichetta()).italics());
                ui.label(format!("Tragitto percorso: {:.2} km", dati.tragitto_km));
                ui.label(format!(
                    "Velocita' media: {:.2} km/h",
                    dati.velocita_media_kmh
                ));
                ui.label(format!(
                    "Durata movimento: {}",
                    formatta_durata(dati.movimento_sec)
                ));
                ui.label(format!("Durata pause: {}", formatta_durata(dati.pause_sec)));
                ui.small(format!("Calcolata su {} campioni.", dati.campioni));
            }
            _ => {
                ui.weak("In attesa del primo aggiornamento...");
            }
        }
    }

    /// Chat privata con l'utente attualmente selezionato.
    fn pannello_chat_privata(&mut self, ui: &mut egui::Ui) {
        ui.heading("Chat privata");

        let Some(nome) = self.utente_selezionato.clone() else {
            ui.weak("Seleziona un utente per scrivergli in privato.");
            return;
        };

        let online = self.stato_online.contains_key(&nome);

        ui.small(format!("Con: {nome}"));
        if !online {
            ui.colored_label(
                egui::Color32::from_rgb(210, 70, 60),
                "Utente sconnesso: impossibile inviare messaggi.",
            );
        }
        ui.separator();

        let altezza_lista = (ui.available_height() - 46.0).max(60.0);
        egui::ScrollArea::vertical()
            .id_salt("chat_privata_scroll")
            .max_height(altezza_lista)
            .stick_to_bottom(true)
            .auto_shrink(false)
            .show(ui, |ui| match self.chat_privata.get(&nome) {
                Some(righe) if !righe.is_empty() => {
                    for riga in righe {
                        ui.label(riga);
                    }
                }
                _ => {
                    ui.weak("Nessun messaggio privato con questo utente.");
                }
            });

        ui.horizontal(|ui| {
            let larghezza_campo = (ui.available_width() - 60.0).max(80.0);
            let campo = ui.add(
                egui::TextEdit::singleline(&mut self.campo_chat)
                    .desired_width(larghezza_campo)
                    .hint_text("messaggio privato..."),
            );
            let invio = campo.lost_focus() && ui.ctx().input(|i| i.key_pressed(egui::Key::Enter));

            if (ui.button("Invia").clicked() || invio) && !self.campo_chat.trim().is_empty() {
                let testo = self.campo_chat.trim().to_string();
                self.campo_chat.clear();

                if !online {
                    // Utente non online: il messaggio non viene inviato, si
                    // avvisa l'operatore direttamente nella chat.
                    self.chat_privata.entry(nome.clone()).or_default().push(
                        "Utente sconnesso, impossibile inviare messaggi.".to_string(),
                    );
                } else {
                    // Mostrato subito in locale.
                    self.chat_privata
                        .entry(nome.clone())
                        .or_default()
                        .push(format!("[tu -> {nome}] {testo}"));

                    self.invia_comando_rete(ComandoAdminGui::Esegui {
                        comando: ComandoAmministrazione::Msg {
                            destinatario: nome.clone(),
                            testo,
                        },
                    });
                }
            }
        });
    }

    fn pannello_mappa(&mut self, ui: &mut egui::Ui) {
        let Some(nome) = self.utente_selezionato.clone() else {
            ui.centered_and_justified(|ui| {
                ui.weak("Seleziona un utente dalla lista a sinistra.");
            });
            return;
        };

        let Some(sessione) = self.stato_online.get(&nome).copied() else {
            // Utente offline: pannello centrale lasciato vuoto.
            return;
        };

        ui.horizontal(|ui| {
            ui.heading(format!("Posizione live: {nome}"));
            let colore = match sessione.stato {
                Stato::InMovimento => egui::Color32::from_rgb(60, 170, 90),
                Stato::Fermo => egui::Color32::from_rgb(220, 170, 40),
                Stato::Sconnesso => egui::Color32::GRAY,
            };
            ui.colored_label(
                colore,
                egui::RichText::new(sessione.stato.etichetta()).strong(),
            );
        });
        ui.separator();

        let Some((lat, lon)) = sessione.ultima_pos else {
            ui.weak("Ancora nessuna posizione ricevuta da questo utente.");
            return;
        };

        let posizione = lat_lon(lat, lon);
        let overlay = TracciaServer {
            traccia: self
                .traccia_live
                .iter()
                .map(|(la, lo)| lat_lon(*la, *lo))
                .collect(),
            attuale: posizione,
            in_movimento: sessione.stato == Stato::InMovimento,
        };

        let mappa =
            Map::new(Some(&mut self.tiles), &mut self.map_memory, posizione).with_plugin(overlay);

        let disponibile = ui.available_size();
        ui.add_sized(disponibile, mappa);
    }

    fn pannello_comandi(&mut self, ui: &mut egui::Ui) {
        ui.heading("Terminale");
        ui.separator();

        egui::ScrollArea::vertical()
            .id_salt("log_terminale")
            .stick_to_bottom(true)
            .max_height((ui.available_height() - 40.0).max(60.0))
            .auto_shrink(false)
            .show(ui, |ui| {
                for riga in &self.log {
                    ui.monospace(riga);
                }
            });

        ui.separator();
        ui.horizontal(|ui| {
            let larghezza_campo = (ui.available_width() - 80.0).max(120.0);
            let campo = ui.add(
                egui::TextEdit::singleline(&mut self.campo_comando)
                    .desired_width(larghezza_campo)
                    .hint_text("/aiuto per l'elenco dei comandi"),
            );
            let invio = campo.lost_focus() && ui.ctx().input(|i| i.key_pressed(egui::Key::Enter));
            if ui.button("Invia").clicked() || invio {
                self.invia_comando_testo();
            }
        });
    }
}

impl eframe::App for ServerApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.processa_eventi();

        if self.ultimo_poll.elapsed() >= POLL_INTERVAL {
            self.ultimo_poll = Instant::now();
            self.aggiorna_utenti_registrati();
            self.invia_comando_rete(ComandoAdminGui::RichiediSnapshot);
        }

        // Aggiornamento analisi utente
        if let Some(nome) = self.utente_selezionato.clone() {
            if self.ultimo_poll_analisi.elapsed() >= POLL_INTERVAL_ANALISI_LIVE {
                self.ultimo_poll_analisi = Instant::now();
                self.invia_comando_rete(ComandoAdminGui::RichiediAnalisi {
                    utente: nome,
                    intervallo: self.intervallo_selezionato,
                });
            }
        }

        ui.ctx().request_repaint_after(Duration::from_millis(300));

        egui::Panel::bottom("pannello_comandi")
            .resizable(true)
            .default_size(220.0)
            .size_range(140.0..=520.0)
            .show(ui, |ui| self.pannello_comandi(ui));

        egui::Panel::left("pannello_utenti")
            .resizable(true)
            .default_size(220.0)
            .show(ui, |ui| self.pannello_utenti(ui));

        egui::Panel::right("pannello_analisi")
            .resizable(true)
            .default_size(280.0)
            .size_range(240.0..=460.0)
            .show(ui, |ui| {
                
                let altezza_disponibile = ui.available_height();
                egui::ScrollArea::vertical()
                    .id_salt("analisi_scroll")
                    .max_height((altezza_disponibile * 0.55).max(140.0))
                    .auto_shrink(false)
                    .show(ui, |ui| self.pannello_analisi(ui));

                ui.separator();

                // Chat privata
                self.pannello_chat_privata(ui);
            });

        egui::CentralPanel::default().show(ui, |ui| self.pannello_mappa(ui));
    }
}

// --------------------------------
// Main
// --------------------------------
fn main() -> eframe::Result<()> {
    let indirizzo = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("GEORUGGINE_BIND").ok())
        .unwrap_or_else(|| INDIRIZZO_DEFAULT.to_string());

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 760.0])
            .with_min_inner_size([1000.0, 600.0])
            .with_title("Georuggine - Console operatore"),
        ..Default::default()
    };

    eframe::run_native(
        "Georuggine Server",
        options,
        Box::new(move |cc| {
            let ctx = cc.egui_ctx.clone();

            let (tx_comandi, rx_comandi) = mpsc::channel::<ComandoAdminGui>(64);
            let (tx_eventi, rx_eventi) = std::sync::mpsc::channel::<EventoServerGui>();

            // Thread di rete con il proprio runtime Tokio, cosi' la GUI resta
            // sul thread principale (richiesto da eframe su diverse piattaforme).
            let indirizzo_rete = indirizzo.clone();
            std::thread::Builder::new()
                .name("georuggine-server-net".to_string())
                .spawn(move || {
                    match tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt.block_on(avvia_rete(indirizzo_rete, rx_comandi, tx_eventi)),
                        Err(e) => eprintln!("[main] impossibile creare il runtime: {e}"),
                    }
                })
                .expect("impossibile avviare il thread di rete");

            let conn_lettura = apri_db().expect("impossibile aprire il database per la GUI");

            Ok(Box::new(ServerApp::new(
                tx_comandi,
                rx_eventi,
                conn_lettura,
                indirizzo,
                ctx,
            )) as Box<dyn eframe::App>)
        }),
    )
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn data_civile_nota() {
        assert_eq!(data_civile(0), (1970, 1, 1));
        assert_eq!(data_civile(19_000), (2022, 1, 8));
    }

    #[test]
    fn interpreta_comandi_base() {
        assert!(matches!(
            interpreta_comando(""),
            ComandoAmministrazione::Vuoto
        ));
        assert!(matches!(
            interpreta_comando("/aiuto"),
            ComandoAmministrazione::Aiuto
        ));
        assert!(matches!(
            interpreta_comando("/stop"),
            ComandoAmministrazione::Stop
        ));
        assert!(matches!(
            interpreta_comando("/all ciao a tutti"),
            ComandoAmministrazione::All { testo } if testo == "ciao a tutti"
        ));
        assert!(matches!(
            interpreta_comando("/msg pippo rientra in sede"),
            ComandoAmministrazione::Sconosciuto { .. }
        ));
        assert!(matches!(
            interpreta_comando("/analisi giorno"),
            ComandoAmministrazione::Sconosciuto { .. }
        ));
    }

    fn setup_db_memoria() -> Connection {
        let conn = Connection::open_in_memory().expect("Impossibile creare DB in memoria");
        conn.execute_batch(
            "CREATE TABLE Utenti (
                 id            INTEGER PRIMARY KEY,
                 username      TEXT NOT NULL UNIQUE,
                 password_hash TEXT NOT NULL,
                 creato_il     DATETIME DEFAULT CURRENT_TIMESTAMP
             );
             CREATE TABLE Posizioni (
                 id        INTEGER PRIMARY KEY,
                 id_utente INTEGER NOT NULL,
                 lat       REAL NOT NULL,
                 lon       REAL NOT NULL,
                 timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                 FOREIGN KEY(id_utente) REFERENCES Utenti(id)
             );"
        ).expect("Impossibile creare lo schema di test");
        conn
    }

    #[test]
    fn test_inizio_intervallo_sql() {
        assert_eq!(inizio_intervallo_sql(Intervallo::Giorno), "date('now','start of day')");
        assert_eq!(inizio_intervallo_sql(Intervallo::Settimana), "date('now','weekday 0','-6 days')");
        assert_eq!(inizio_intervallo_sql(Intervallo::Mese), "date('now','start of month')");
    }

    #[test]
    fn test_query_utenti() {
        let conn = setup_db_memoria();
        
        // Inserimento disordinato per verificare l'ordinamento alfabetico
        conn.execute("INSERT INTO Utenti (username, password_hash) VALUES ('mario', 'hash1')", []).unwrap();
        conn.execute("INSERT INTO Utenti (username, password_hash) VALUES ('anna', 'hash2')", []).unwrap();
        conn.execute("INSERT INTO Utenti (username, password_hash) VALUES ('zeta', 'hash3')", []).unwrap();

        // Verifica estrazione ID
        assert_eq!(id_utente(&conn, "mario").unwrap(), Some(1));
        assert_eq!(id_utente(&conn, "anna").unwrap(), Some(2));
        assert_eq!(id_utente(&conn, "inesistente").unwrap(), None);

        // Verifica estrazione e ordinamento elenco (deve essere: anna, mario, zeta)
        let elenco = elenco_utenti_registrati(&conn);
        assert_eq!(elenco, vec!["anna".to_string(), "mario".to_string(), "zeta".to_string()]);
    }

    #[test]
    fn test_carica_traccia_filtraggio_temporale() {
        let conn = setup_db_memoria();
        conn.execute("INSERT INTO Utenti (id, username, password_hash) VALUES (1, 'test', 'pwd')", []).unwrap();
        
        // Inserimento con timestamp UNIX precisi per testare il filtro 'soglia_unix'
        conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 10.0, 20.0, datetime(1000, 'unixepoch'))", []).unwrap();
        conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 11.0, 21.0, datetime(2000, 'unixepoch'))", []).unwrap();
        conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 12.0, 22.0, datetime(3000, 'unixepoch'))", []).unwrap();

        // Soglia a 1500: deve scartare il primo record
        let traccia = carica_traccia(&conn, 1, 1500);
        assert_eq!(traccia.len(), 2);
        assert_eq!(traccia[0], (11.0, 21.0));
        assert_eq!(traccia[1], (12.0, 22.0));
    }

    #[test]
    fn test_analizza_soglie_movimento_e_pause() {
        let conn = setup_db_memoria();
        conn.execute("INSERT INTO Utenti (id, username, password_hash) VALUES (1, 'test_user', 'hash')", []).unwrap();

        // Punto A: Partenza
        conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 45.000, 9.0, datetime('now', '-2 minutes'))", []).unwrap();
        // Punto B: 60 secondi dopo, distanza > 10m (latitudine variata di 0.01 gradi =~ 1.1 km) -> Movimento
        conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 45.010, 9.0, datetime('now', '-1 minute'))", []).unwrap();
        // Punto C: 60 secondi dopo, distanza 0m -> Pausa
        conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 45.010, 9.0, datetime('now'))", []).unwrap();

        let analisi = analizza(&conn, 1, Intervallo::Giorno).unwrap();
        
        assert_eq!(analisi.campioni, 3);
        assert_eq!(analisi.movimento_sec, 60);
        assert_eq!(analisi.pause_sec, 60);
        assert!(analisi.tragitto_km > 1.0); 
        assert!(analisi.velocita_media_kmh > 0.0);
    }
    #[test]
fn test_analizza_gap_troppo_lungo_scartato() {
    let conn = setup_db_memoria();
    conn.execute("INSERT INTO Utenti (id, username, password_hash) VALUES (1, 'test_user', 'hash')", []).unwrap();


    conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 45.000, 9.0, datetime('now', '-400 seconds'))", []).unwrap();
    conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 45.100, 9.0, datetime('now'))", []).unwrap();

    let analisi = analizza(&conn, 1, Intervallo::Giorno).unwrap();

    assert_eq!(analisi.campioni, 2);
    assert_eq!(analisi.movimento_sec, 0);
    assert_eq!(analisi.pause_sec, 0);
    assert_eq!(analisi.tragitto_km, 0.0);
}

#[test]
fn test_analizza_sopra_soglia_movimento() {
    let conn = setup_db_memoria();
    conn.execute("INSERT INTO Utenti (id, username, password_hash) VALUES (1, 'test_user', 'hash')", []).unwrap();

  
    conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 45.00000, 9.0, datetime('now', '-30 seconds'))", []).unwrap();
    conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 45.00020, 9.0, datetime('now'))", []).unwrap();

    let analisi = analizza(&conn, 1, Intervallo::Giorno).unwrap();

    assert_eq!(analisi.movimento_sec, 30);
    assert_eq!(analisi.pause_sec, 0);
}

#[test]
fn test_analizza_sotto_soglia_pausa() {
    let conn = setup_db_memoria();
    conn.execute("INSERT INTO Utenti (id, username, password_hash) VALUES (1, 'test_user', 'hash')", []).unwrap();

    conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 45.000000, 9.0, datetime('now', '-30 seconds'))", []).unwrap();
    conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 45.000050, 9.0, datetime('now'))", []).unwrap();

    let analisi = analizza(&conn, 1, Intervallo::Giorno).unwrap();

    assert_eq!(analisi.movimento_sec, 0);
    assert_eq!(analisi.pause_sec, 30);
}
#[test]
fn test_analizza_nessun_campione() {
    let conn = setup_db_memoria();
    conn.execute("INSERT INTO Utenti (id, username, password_hash) VALUES (1, 'test_user', 'hash')", []).unwrap();

    let analisi = analizza(&conn, 1, Intervallo::Giorno).unwrap();

    assert_eq!(analisi.campioni, 0);
    assert_eq!(analisi.movimento_sec, 0);
    assert_eq!(analisi.pause_sec, 0);
    assert_eq!(analisi.tragitto_km, 0.0);
    // Divisione per zero evitata: la velocita' deve restare 0.0, non NaN.
    assert_eq!(analisi.velocita_media_kmh, 0.0);
}

#[test]
fn test_analizza_un_solo_campione() {
    let conn = setup_db_memoria();
    conn.execute("INSERT INTO Utenti (id, username, password_hash) VALUES (1, 'test_user', 'hash')", []).unwrap();
    conn.execute("INSERT INTO Posizioni (id_utente, lat, lon, timestamp) VALUES (1, 45.0, 9.0, datetime('now'))", []).unwrap();


    let analisi = analizza(&conn, 1, Intervallo::Giorno).unwrap();

    assert_eq!(analisi.campioni, 1);
    assert_eq!(analisi.movimento_sec, 0);
    assert_eq!(analisi.pause_sec, 0);
    assert_eq!(analisi.velocita_media_kmh, 0.0);
}#[tokio::test]
async fn test_esegui_comando_all_vuoto() {
    let registro: Registro = Arc::new(Mutex::new(HashMap::new()));
    let righe = esegui_comando_admin(&registro, ComandoAmministrazione::All { testo: String::new() }).await;

    assert_eq!(righe, vec!["Uso: /all <testo>".to_string()]);
}

#[tokio::test]
async fn test_esegui_comando_msg_utente_non_connesso() {
    let registro: Registro = Arc::new(Mutex::new(HashMap::new()));
    let righe = esegui_comando_admin(
        &registro,
        ComandoAmministrazione::Msg { destinatario: "pippo".to_string(), testo: "ciao".to_string() },
    )
    .await;

    assert_eq!(righe, vec!["Utente 'pippo' non connesso.".to_string()]);
}

#[tokio::test]
async fn test_esegui_comando_vuoto_non_produce_output() {
    let registro: Registro = Arc::new(Mutex::new(HashMap::new()));
    let righe = esegui_comando_admin(&registro, ComandoAmministrazione::Vuoto).await;

    assert!(righe.is_empty());
}
fn sessione_finta() -> (Sessione, mpsc::Receiver<ServerMessage>) {
    let (tx, rx) = mpsc::channel::<ServerMessage>(8);
    let sessione = Sessione {
        tx,
        stato: Stato::Fermo,
        ultima_pos: None,
        connesso_da: Instant::now(),
        inizio_tragitto: 0,
    };
    (sessione, rx)
}

#[tokio::test]
async fn test_invia_a_utente_connesso_e_non_connesso() {
    let registro: Registro = Arc::new(Mutex::new(HashMap::new()));
    let (sessione, mut rx) = sessione_finta();
    registro.lock().await.insert("mario".to_string(), sessione);

    let msg = ServerMessage::MessaggioDalServer { testo: "ciao".to_string(), broadcast: false };

    // Utente presente: invio riuscito e messaggio ricevuto sul canale.
    assert!(invia_a_utente(&registro, "mario", msg.clone()).await);
    assert!(matches!(rx.recv().await, Some(ServerMessage::MessaggioDalServer { testo, .. }) if testo == "ciao"));

    // Utente assente: invio fallito, nessun panico.
    assert!(!invia_a_utente(&registro, "utente_fantasma", msg).await);
}

#[tokio::test]
async fn test_invia_broadcast_conta_solo_i_connessi() {
    let registro: Registro = Arc::new(Mutex::new(HashMap::new()));
    let (s1, mut rx1) = sessione_finta();
    let (s2, mut rx2) = sessione_finta();
    registro.lock().await.insert("anna".to_string(), s1);
    registro.lock().await.insert("mario".to_string(), s2);

    let n = invia_broadcast(
        &registro,
        ServerMessage::MessaggioDalServer { testo: "avviso a tutti".to_string(), broadcast: true },
    )
    .await;

    assert_eq!(n, 2);
    assert!(rx1.recv().await.is_some());
    assert!(rx2.recv().await.is_some());
}
}
