# opencall-monitor

Monitorim automatik i thirrjeve/tenderëve për shërbime ligjore (TED/BE, GIZ, UNDP, OSBE etj.) relevante për Shqipërinë.

## Instalim (në VPS/server)

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

Hap dashboard-in: `http://<ip-i-serverit>:8000/`

Për ta mbajtur gjallë vazhdimisht, përdor `systemd` ose `pm2`/`supervisor` (jo thjesht `uvicorn` në terminal).

## Si funksionon

- **`config.yaml`** — fjalët-kyçe (scoring), CPV kodet për TED, dhe on/off për çdo burim.
- **`main.py`** — gjithçka: DB (SQLite), connector-i TED, scheduler (kontrollon çdo N orë, config-urueshëm), API (FastAPI), dhe dashboard-i Kanban (HTML+JS, pa framework).
- Baza e të dhënave (`opencalls.db`) krijohet vetvetiu në të njëjtin folder.
- Butoni **"Kontrollo tani"** në dashboard nis një kontroll manual pa pritur schedulerin.
- Çdo njoftim merr një **score** bazuar në fjalët-kyçe (high=3, medium=2, low=1 pikë) — më i larti në krye.
- Lëviz njoftimet mes kolonave (NEW → REVIEW → GO → BID → WON/LOST/DROPPED) me dropdown-in te çdo kartë.

## Gjendja aktuale e burimeve

| Burim | Status | Shënim |
|---|---|---|
| **TED (BE)** | I lidhur, por **API-ja ktheu 403 Forbidden** në testim | Që nga ndryshimet e TED (eForms), API-ja publike ndoshta kërkon regjistrim/subscription key. Duhet verifikuar në `https://api.ted.europa.eu/swagger` — nëse kërkon `Ocp-Apim-Subscription-Key`, e shtojmë si header në `poll_ted()`. |
| **GIZ** | **I aktivizuar** — via Panorama Online | GIZ Shqipëri s'ka portal publik prokurimi (publikon vetëm në LinkedIn, e paarritshme për scraping pa login). Zbulova se **Panorama Online rimerr çdo njoftim GIZ** te kategoria "Këndi i njoftimeve" (`panorama.com.al/category/njoftime/`) — burim i qëndrueshëm, publik, pa login. Connector-i kontrollon këtë faqe + `max_pages` faqe pagination, filtron artikujt me "GIZ" në titull, dhe përpiqet të nxjerrë afatin (regex mbi tekstin e artikullit — shpesh format "latest by DD.MM.YYYY"). **E testova strukturalisht** (kodi ekzekutohet pa gabime); vetë shkarkimi nga interneti real duhet verifikuar nga VPS-ja jote pasi këtu jam i kufizuar në rrjet gjatë ndërtimit. |
| **UNDP/UNGM** | Placeholder (`rss_sources: []`) | Shto feed RSS nëse UNDP ofron një, ose kalo te "Tender Alert" via email → IMAP (jo ndërtuar ende). |

## Hapat e ardhshëm (kur të duash t'i shtojmë)

1. **Nise në VPS dhe kliko "Kontrollo tani"** — do ta shohësh menjëherë nëse GIZ/Panorama sjell rezultate reale (kontrollo edhe `giz.title_must_contain` nëse titujt ndryshojnë formatin).
2. Zgjidh 403-shin e TED (regjistrim API key) — dytësor pasi GIZ tashmë funksionon.
3. Shto njoftime Slack/email (fushat në `config.yaml` → `notify`, ende jo të lidhura në kod).

## Shënim mbi Panorama Online

Faqja e tyre ka një klauzolë "ndalohet kopjimi/ribotimi pa leje" — ky monitorues **nuk ribotoi asgjë**: ruan vetëm titullin, linkun dhe afatin (jo tekstin e plotë të artikullit) për përdorim të brendshëm, si të ishte një RSS reader personal. Për vetë tekstin e tenderit, dashboard-i të çon te artikulli origjinal.
