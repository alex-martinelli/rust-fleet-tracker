# Georuggine - Manuale Utente

## 1. Introduzione e Requisiti

Georuggine è un sistema client/server per la geolocalizzazione, l'analisi del movimento e la comunicazione con flotte di veicoli.

L'applicativo si divide rigorosamente in due componenti separati, ognuno con la propria interfaccia grafica dedicata:

* **Interfaccia Admin/Operatore (Server):** Destinata al centro di controllo. Permette di monitorare simultaneamente l'intera flotta, visualizzare le posizioni sulla mappa, osservare le analisi statistiche di ogni veicolo e amministrare le comunicazioni (dirette o broadcast).
* **Interfaccia Utente/Autista (Client):** Destinata al terminale di bordo del singolo veicolo. Consente all'autista la registrazione (o accesso), l'avvio e la gestione dell'emulazione del percorso (movimento e sosta) e la comunicazione testuale con il centro di controllo.

**Requisiti e Piattaforme:**

* **Sistemi supportati:** Windows, Linux, macOS.
* **Requisiti:** Ambiente di compilazione Rust (Cargo).

---

## 2. Avvio dell'Applicazione

L'applicativo richiede l'esecuzione separata dei moduli server e client da due terminali posizionati nella directory radice del progetto.

**Avvio del Server**
Eseguire il comando:
`cargo run --release --bin server`
Il server si avvia in ascolto sull'indirizzo predefinito `0.0.0.0:8080` e crea in automatico il file `georuggine.db`.
Per forzare un indirizzo o una porta differente, utilizzare la variabile d'ambiente:
`GEORUGGINE_BIND=IP:PORTA cargo run --release --bin server`

**Avvio del Client**
Eseguire il comando:
`cargo run --release --bin client`
Per connettere il client a un server remoto, specificare l'indirizzo tramite variabile d'ambiente:
`GEORUGGINE_SERVER=IP:PORTA cargo run --release --bin client`

---

## 3. Guida all'Interfaccia Server (Operatore)

L'interfaccia si divide in quattro aree funzionali:

* **Pannello Utenti (Sinistra):** Elenca tutti gli account registrati nel database. Indica lo stato di connessione (online/offline) e lo stato di movimento (in movimento/fermo). Quando un client si collega, compare immediatamente come online e fermo. Cliccando su un utente lo si seleziona per l'analisi e il tracciamento.
* **Pannello Mappa (Centro):** Mostra la posizione attuale e l'intero tragitto in corso dell'utente selezionato. Il marcatore è verde se il veicolo è in movimento, giallo se è in stato di fermo. Se il client è in movimento, la mappa si aggiorna in tempo reale; l'operatore può monitorare l'avanzamento di un altro client in movimento semplicemente selezionandolo dalla lista a sinistra.
* **Pannello Analisi e Chat (Destra):**

  * *Analisi:* Selezionare l'intervallo (Giorno corrente/Settimana/Mese) per calcolare istantaneamente i chilometri percorsi, la velocità media e i tempi di sosta o movimento dell'utente selezionato.
  * *Chat privata:* Campo testuale per inviare messaggi diretti all'utente selezionato (solo se l'utente è online).
* **Terminale Comandi (Basso):** Riga di comando globale per istruzioni amministrative.

  * `/all <testo>`: Invia un messaggio in broadcast a tutti i connessi.
  * `/stop`: Arresta il server.
  * `/aiuto`: Mostra questo elenco di comandi.

---

## 4. Guida all'Interfaccia Client (Autista)

* **Accesso e Registrazione:** I nuovi utenti devono registrare un account. Lo username deve contenere tra 3 e 24 caratteri (lettere, numeri, `_`, `.`, `-`). La password richiede un minimo di 4 caratteri.
* **Emulazione del Movimento:**

  * Cliccare su "Avvia percorso" per iniziare la simulazione leggendo i dati dal file locale.
  * Il tasto "Sosta" mette in pausa l'avanzamento ma continua a trasmettere la coordinata corrente, forzando la transizione allo stato "Fermo" sul server dopo 3 minuti.
  * Il tasto "Ferma" arresta l'emulazione.
* **Comunicazione:** Il pannello "Messaggi" a destra riceve le notifiche dal server e permette l'invio di testo alla console dell'operatore.

**Pannelli Operativi dell'Interfaccia:**
I pannelli della GUI si aggiornano dinamicamente in base alle operazioni dell'utente e all'interazione con il server.

* **Telemetria:** Ha lo scopo di monitorare i dati istantanei e lo stato della connessione. Indica lo stato logico corrente (Sconnesso, Fermo, In movimento) elaborato e validato dal server, le coordinate dell'ultimo invio (posizione) e il conteggio dei campioni trasmessi dall'inizio del tragitto.
* **Emulazione del movimento:** Serve a configurare e pilotare la simulazione del tracciato. Prima dell'avvio, permette di impostare la frequenza di trasmissione tramite un selettore di velocità: il valore `x1` rispetta la specifica (un campione ogni 30s reali), mentre i fattori `x10` e `x60` accelerano l'invio per facilitare le procedure di test. Ad emulazione attiva, il pannello espone i metadati del tracciato in lettura (totale campioni e durata) e fornisce i controlli diretti di marcia: il tasto "Sosta" sospende l'avanzamento ma continua a trasmettere l'ultima posizione nota, mentre "Ferma" interrompe definitivamente il processo.
* **Analisi del movimento:** Area dedicata alla visualizzazione delle statistiche aggregate. Permette all'utente di selezionare uno specifico intervallo temporale (Giorno corrente, Settimana, Mese) per richiedere al server i dati di sintesi. Il riquadro restituisce i valori oggettivi calcolati: distanza totale percorsa in chilometri, velocità media in km/h e la ripartizione esatta, in secondi, del tempo trascorso in movimento o in sosta durante l'intervallo richiesto.

---

## 5. File di Supporto e Log

* **Tracciati CSV (`percorso.csv`):** Il sistema carica le posizioni da questo file. Il formato accettato è `tempo,latitudine,longitudine` in gradi decimali WGS84. Sono accettati la virgola, il punto e virgola o la tabulazione come separatori, e sia il punto che la virgola come marcatore decimale. L'assenza del campo `tempo` comporta un incremento automatico di 30 secondi a riga.
* **Log Prestazionali:** Il server genera il file `cpu_log.txt` nella radice del progetto. Viene aggiornato ogni 120 secondi documentando l'uptime, l'utilizzo cumulativo della CPU e della RAM consumata dal processo.
