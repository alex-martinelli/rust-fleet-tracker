// Tipi condivisi fra il client e il server.

use serde::{Deserialize, Serialize};

/// Ogni quanti secondi il client trasmette la propria posizione al server.
pub const INTERVALLO_INVIO_SEC: u64 = 30;

/// Dopo quanti secondi di coordinate invariate un utente passa da "in movimento" a "fermo". 
pub const TIMEOUT_FERMO_SEC: u64 = 180;

/// Porta TCP di default del server.
pub const PORTA_DEFAULT: u16 = 8080;

/// Intervallo su cui il server calcola l'analisi del movimento.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intervallo {
    Giorno,
    Settimana,
    Mese,
}

impl Intervallo {
    pub fn etichetta(&self) -> &'static str {
        match self {
            Intervallo::Giorno => "Giorno corrente",
            Intervallo::Settimana => "Settimana corrente",
            Intervallo::Mese => "Mese corrente",
        }
    }

    pub fn da_testo(s: &str) -> Option<Intervallo> {
        match s.trim().to_lowercase().as_str() {
            "g" | "giorno" | "oggi" => Some(Intervallo::Giorno),
            "s" | "settimana" => Some(Intervallo::Settimana),
            "m" | "mese" => Some(Intervallo::Mese),
            _ => None,
        }
    }
}

/// Stato di un utente visto dal server.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stato {
    Sconnesso,
    Fermo,
    InMovimento,
}

impl Stato {
    pub fn etichetta(&self) -> &'static str {
        match self {
            Stato::Sconnesso => "Sconnesso",
            Stato::Fermo => "Fermo",
            Stato::InMovimento => "In movimento",
        }
    }
}

impl std::fmt::Display for Stato {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.etichetta())
    }
}

/// Messaggi che il client invia al server.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientMessage {
    /// Creazione di un nuovo account.
    Register { user: String, password_hash: String },
    /// Autenticazione con un account esistente.
    Login { user: String, password_hash: String },
    /// Posizione geografica emulata
    PositionUpdate { lat: f64, lon: f64 },
    /// Richiesta di analisi del proprio movimento su un intervallo.
    RichiestaAnalisi(Intervallo),
    /// Messaggio di testo indirizzato al server.
    ChatMessage { testo: String },
    /// Segnala l'inizio (o il riavvio) dell'emulazione del tragitto.
    PercorsoAvviato,
    /// Disconnessione richiesta dall'utente.
    Logout,
}

/// Messaggi che il server invia al client.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ServerMessage {
    /// Esito dell'autenticazione.
    AuthResult { success: bool, messaggio: String },
    /// Esito della registrazione.
    RegisterResult { success: bool, messaggio: String },
    /// Risultato dell'analisi del movimento.
    AnalisiResult {
        intervallo: Intervallo,
        tragitto_km: f64,
        velocita_media_kmh: f64,
        movimento_sec: u64,
        pause_sec: u64,
        campioni: u64,
    },
    /// Notifica di stato e ultima posizione nota.
    StatoAggiornato {
        stato: Stato,
        lat: Option<f64>,
        lon: Option<f64>,
    },
    /// Messaggio inviato dal server (in broadcast o diretto).
    MessaggioDalServer { testo: String, broadcast: bool },
    /// Conferma che il server ha chiuso la sessione dopo un logout.
    LogoutOk,
}

/// Funzione per la password
pub fn hash_password(user: &str, password: &str) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(b"georuggine:v1:");
    hasher.update(normalizza_username(user).as_bytes());
    hasher.update(b":");
    hasher.update(password.as_bytes());

    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

pub fn normalizza_username(user: &str) -> String {
    user.trim().to_lowercase()
}

/// Regole di validità per username
pub fn valida_username(user: &str) -> Result<String, String> {
    let u = normalizza_username(user);
    if u.len() < 3 || u.len() > 24 {
        return Err("Lo username deve essere lungo da 3 a 24 caratteri.".to_string());
    }
    if !u.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-') {
        return Err("Lo username puo' contenere solo lettere, cifre, '_', '.' e '-'.".to_string());
    }
    Ok(u)
}

/// Formatta una durata in secondi.
pub fn formatta_durata(secondi: u64) -> String {
    let h = secondi / 3600;
    let m = (secondi % 3600) / 60;
    let s = secondi % 60;
    if h > 0 {
        format!("{}h {:02}m {:02}s", h, m, s)
    } else if m > 0 {
        format!("{}m {:02}s", m, s)
    } else {
        format!("{}s", s)
    }
}

/// Distanza in km fra due coordinate.
pub fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const RAGGIO_TERRA_KM: f64 = 6371.0;
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lon / 2.0).sin().powi(2);
    2.0 * RAGGIO_TERRA_KM * a.sqrt().atan2((1.0 - a).sqrt())
}

/// Controlla che una coppia di coordinate sia valida.
pub fn coordinate_valide(lat: f64, lon: f64) -> bool {
    lat.is_finite() && lon.is_finite() && (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn hash() {
        assert_eq!(hash_password("Mario", "segreta"), hash_password("mario", "segreta"));
        assert_ne!(hash_password("mario", "segreta"), hash_password("luigi", "segreta"));
        assert_eq!(hash_password("mario", "segreta").len(), 64);
    }

    #[test]
    fn distanza_torino_asti() {
        let km = haversine_km(45.0618513, 7.6606506, 44.9084148, 8.1778599);
        assert!((km - 44.0).abs() < 6.0, "distanza inattesa: {km}");
    }

    #[test]
    fn username_validi() {
        assert!(valida_username("  Mario_Rossi ").is_ok());
        assert!(valida_username("ab").is_err());
        assert!(valida_username("mario rossi").is_err());
    }
}