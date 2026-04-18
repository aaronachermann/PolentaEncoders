# Guida rapida: Claude Code + Databricks + MCP (Mac e Windows)

Questa guida riassume i passaggi per usare **Claude Code** da terminale, collegarlo a **Databricks** e sfruttare il server **MCP SQL** nel progetto `PolentaEncoders`.

## 1. Obiettivo

Workflow consigliato:

- **Claude Code in locale** sul tuo computer per scrivere e iterare codice
- **Databricks** per notebook, dati, query, esecuzione e submission
- **MCP** per permettere a Claude Code di vedere tool e dati Databricks dal terminale

---

## 2. Prerequisiti

### Software minimo

- **Git**
- **Claude Code**
- **Databricks CLI**
- **Python**
- **uv** opzionale ma consigliato
- un **ambiente Python locale** per dipendenze del progetto

### Repository

Nel tuo caso:

- repo GitHub: `PolentaEncoders`
- repo locale collegata a GitHub
- `ai-dev-kit` installato nella cartella del progetto
- librerie Python installate in un ambiente virtuale

---

## 3. Installare Claude Code nel terminale

## Mac

### Metodo consigliato

```bash
curl -fsSL https://claude.ai/install.sh | bash
```

### Alternativa con npm

```bash
npm install -g @anthropic-ai/claude-code
```

## Windows

### Metodo consigliato con npm

Apri **PowerShell** o **Windows Terminal** e usa:

```powershell
npm install -g @anthropic-ai/claude-code
```

### Se vuoi usare l'installer script

Se Anthropic lo rende disponibile anche nel tuo ambiente, puoi usarlo in un terminale compatibile. In pratica, su Windows di solito la via più semplice resta **npm**.

### Verifica

Su Mac o Windows:

```bash
claude --version
```

---

## 4. Installare la Databricks CLI

## Mac

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

## Windows

### Metodo consigliato con winget

```powershell
winget install Databricks.DatabricksCLI
```

### Oppure con Chocolatey

```powershell
choco install databricks-cli
```

### Verifica

Su Mac o Windows:

```bash
databricks -v
```

---

## 5. Login a Databricks da terminale

Usa l'host del workspace, cioè **solo il dominio base** del workspace Databricks.

Nel vostro caso:

```bash
databricks auth login --host https://dbc-a5ed240a-fe44.cloud.databricks.com
```

Alla richiesta del profilo puoi dare il nome che vuoi, per esempio il tuo indirizzo email.

### Verifica

```bash
databricks current-user me
```

Oppure:

```bash
databricks workspace list /
```

---

## 6. Clonare o aprire la repo locale

## Mac / Linux

```bash
git clone <URL-DELLA-REPO>
cd PolentaEncoders
```

## Windows PowerShell

```powershell
git clone <URL-DELLA-REPO>
cd PolentaEncoders
```

Se hai già la repo in locale, entra semplicemente nella cartella.

---

## 7. Ambiente Python locale

## Mac / Linux

### Creazione

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Installazione dipendenze

```bash
pip install -r requirements.txt
```

## Windows PowerShell

### Creazione

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### Installazione dipendenze

```powershell
pip install -r requirements.txt
```

## Importante

**Non pushare l'ambiente virtuale nella repo.**

Non vanno pushati:

- `.venv/`
- `env/`
- `venv/`
- `__pycache__/`
- file temporanei locali

Nella repo vanno invece pushati:

- `requirements.txt` oppure `pyproject.toml` / `uv.lock`
- il codice sorgente
- notebook e file utili al team
- documentazione

---

## 8. Installare AI Dev Kit nel progetto

Dentro la cartella del progetto:

```bash
bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh) --profile aaron.achermann@gmail.com --force
```

## Nota Windows

Questo comando è pensato per shell Unix-like. Su Windows hai 3 opzioni pratiche:

### Opzione A — usare Git Bash
Apri **Git Bash** dentro la cartella del progetto e lancia lo stesso comando.

### Opzione B — usare WSL
Apri **WSL** e lancia lo stesso comando.

### Opzione C — installazione manuale
Se non vuoi usare Git Bash o WSL, puoi clonare il repo `ai-dev-kit` e seguire la documentazione del progetto per l'installazione locale.

Questo step crea tipicamente file come:

- `.claude/`
- `.mcp.json`
- altre cartelle di config per tool AI

---

## 9. Aprire Claude Code nel progetto

Da dentro la cartella del progetto:

```bash
claude
```

La prima volta Claude può chiederti di autorizzare gli MCP server trovati nel progetto.

Per questo progetto conviene scegliere:

- **Use this and all future MCP servers in this project**

così l'autorizzazione resta valida per `PolentaEncoders`.

---

## 10. Login dentro Claude Code

Se Claude risponde con un errore tipo:

- `Please run /login`
- `Invalid authentication credentials`

devi autenticarti dentro Claude Code:

```text
/login
```

Completa il login nel browser e poi torna nel terminale.

---

## 11. Trovare il server MCP giusto in Databricks

Nel workspace Databricks:

- vai su **Agents**
- apri la tab **MCPs**
- cerca **DBSQL MCP Server**

Nel vostro caso l'endpoint è:

```text
https://dbc-a5ed240a-fe44.cloud.databricks.com/api/2.0/mcp/sql
```

Questo è il server MCP SQL che permette a Claude Code di interrogare Databricks.

---

## 12. Esempio di file `.mcp.json`

Nel tuo caso, il file locale era di questo tipo:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "/Users/Aaron/.ai-dev-kit/.venv/bin/python",
      "args": [
        "/Users/Aaron/.ai-dev-kit/repo/databricks-mcp-server/run_server.py"
      ],
      "defer_loading": true,
      "env": {
        "DATABRICKS_CONFIG_PROFILE": "aaron.achermann@gmail.com"
      }
    }
  }
}
```

## Importante

Questo file **non va pushato** così com'è, perché contiene:

- path assoluti locali
- configurazione personale
- nome del tuo profilo Databricks

---

## 13. Cosa vuol dire MCP qui

Con MCP, Claude Code può usare tool Databricks dal terminale, ad esempio:

- eseguire SQL
- ispezionare schema e statistiche tabelle
- eseguire codice
- lavorare con warehouse, file workspace, job, dashboard, ecc.

Per l'hackathon, i tool più utili sono in genere:

- `execute_sql`
- `get_table_stats_and_schema`
- `execute_code`
- eventualmente `manage_workspace_files`

---

## 14. Primo test dentro Claude Code

Dopo login e autorizzazione MCP, puoi scrivere:

```text
What MCP tools are available from the Databricks server?
```

Se risponde con la lista dei tool, il collegamento funziona.

---

## 15. Prompt utili per il challenge

### Ispezione tabelle

```text
Use the Databricks MCP server to inspect these tables for the Axpo datathon:
- datathon.shared.client_consumption
- datathon.shared.demand_forecast
- datathon.shared.pv_production_forecast
- datathon.shared.wind_production_forecast

For each table:
1. show schema
2. show row count if possible
3. show min/max timestamp
4. show 5 sample rows
5. explain how it could be useful for day-ahead forecasting
```

### EDA iniziale

```text
Using the Databricks MCP tools, help me do the first EDA for the challenge.

Goals:
- aggregate total active_kw at 15-minute level from datathon.shared.client_consumption
- inspect daily and weekly seasonality
- detect missing intervals and obvious anomalies
- inspect the distribution of clients by community_code
- keep everything efficient and scalable

Please start with SQL queries, then suggest how to translate the logic into PySpark for the exploration notebook.
```

### Baseline

```text
Help me design a simple but strong baseline for the Axpo Iberia retail consumption forecasting challenge.

Requirements:
- day-ahead forecast
- no leakage
- predict total portfolio consumption every 15 minutes
- use only information available before 12:00 on D-1
- start with an aggregate portfolio model
- use calendar features, lag features, rolling features, and available Databricks shared forecasts as optional exogenous features
```

---

## 16. Workflow consigliato per il team

## In locale con Claude Code

Usatelo per:

- capire schema e dati
- scrivere SQL
- proporre feature engineering
- scrivere funzioni Python / PySpark
- ripulire il codice

## In Databricks

Usatelo per:

- eseguire notebook
- fare EDA reale
- testare performance
- preparare `exploration` e `submission`
- fare la submission finale

## Regola pratica

- **Claude Code non sostituisce il notebook Databricks**
- **Claude Code + MCP** serve per aiutarti a lavorare più velocemente sui dati Databricks

---

## 17. Cosa pushare nella repo GitHub

## Sì

- `src/`
- notebook o export utili
- `requirements.txt`, `pyproject.toml`, `uv.lock`
- `.gitignore`
- eventuali file di config condivisi del progetto
- documentazione `README.md`

## No

- `.venv/`, `env/`, `venv/`
- credenziali, token, secret
- file cache e temporanei
- grossi file generati localmente se non servono davvero
- `.mcp.json` se contiene path o profili personali
- `.claude/` se contiene config locale
- `.cursor/`, `.agents/`, `.ai-dev-kit/` se sono solo setup locale

---

## 18. `.gitignore` consigliato

```gitignore
# --- Python envs ---
env/
venv/
.venv/

# --- Python cache / build ---
__pycache__/
*.py[cod]
*.pyo
*.pyd
*.so
build/
dist/
*.egg-info/
.eggs/
.python-version

# --- Jupyter ---
.ipynb_checkpoints/

# --- OS / editor ---
.DS_Store
Thumbs.db
.vscode/
.idea/

# --- Secrets / local env ---
.env
.env.*
*.local
*.log

# --- Claude / MCP local config ---
.mcp.json
.claude/
.cursor/
.agents/

# --- Optional local AI kit cache/config ---
.ai-dev-kit/
```

---

## 19. Checklist finale rapida

## Mac / Linux

```bash
cd PolentaEncoders
source .venv/bin/activate
databricks current-user me
claude
```

## Windows PowerShell

```powershell
cd PolentaEncoders
.\.venv\Scripts\Activate.ps1
databricks current-user me
claude
```

Dentro Claude:

```text
/login
```

poi:

```text
What MCP tools are available from the Databricks server?
```

poi iniziate con i prompt sul catalogo `datathon`.

---

## 20. In una frase

Per il vostro setup:

- **repo GitHub + clone locale + env Python locale + Claude Code + Databricks CLI + MCP SQL Databricks**

è la combinazione giusta.

Le cose da **non** pushare sono l'ambiente virtuale e qualunque credenziale, segreto o configurazione troppo personale.
