#!/usr/bin/env python3
"""
Firecrawl /map en masse : récupère la liste des URLs de chaque site web,
avec rotation de plusieurs clés API et reprise automatique.

    python map_sites.py map     # appelle Firecrawl, écrit output/raw_map.jsonl
    python map_sites.py build   # -> output/urls.csv + output/urls.xlsx (+ résumé par site)

Clés : FIRECRAWL_API_KEYS (séparées par virgule, point-virgule ou retour à la ligne).
Entrée : data/sites.xlsx (ou .csv) avec une colonne `website` (+ `title`, `cid` facultatives).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font

# --------------------------------------------------------------------------- #
# Configuration (surchargeable par variables d'environnement)
# --------------------------------------------------------------------------- #
FIRECRAWL_URL = os.getenv("FIRECRAWL_URL", "https://api.firecrawl.dev/v2/map")
URL_LIMIT = int(os.getenv("URL_LIMIT", "500"))               # URLs max par site (max API : 100000)
SITEMAP = os.getenv("SITEMAP", "include")                    # include | skip | only
INCLUDE_SUBDOMAINS = os.getenv("INCLUDE_SUBDOMAINS", "false").lower() == "true"
MAP_FROM_ORIGIN = os.getenv("MAP_FROM_ORIGIN", "true").lower() == "true"  # mappe la racine du site
COUNTRY = os.getenv("LOCATION_COUNTRY", "FR")
LANGUAGES = [x for x in os.getenv("LOCATION_LANGUAGES", "fr").split(",") if x]
WORKERS = int(os.getenv("WORKERS", "5"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "6"))
KEY_COOLDOWN_S = int(os.getenv("KEY_COOLDOWN_S", "30"))      # pause d'une clé après un 429
REQUEST_TIMEOUT_S = int(os.getenv("REQUEST_TIMEOUT_S", "120"))
MAX_RUNTIME_MIN = float(os.getenv("MAX_RUNTIME_MIN", "340"))  # < limite 6 h de GitHub

OUT_DIR = Path(os.getenv("OUT_DIR", "output"))
RAW_FILE = OUT_DIR / "raw_map.jsonl"
XLSX_ROW_CAP = 1_000_000

_print_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


# --------------------------------------------------------------------------- #
# Rotation des clés
# --------------------------------------------------------------------------- #
class AllKeysExhausted(Exception):
    pass


class KeyPool:
    """Round-robin thread-safe. 429 -> clé en pause ; 401/402 -> clé retirée."""

    def __init__(self, keys: list[str]):
        if not keys:
            raise SystemExit("Aucune clé API : définir FIRECRAWL_API_KEYS.")
        self.keys = keys
        self.lock = threading.Lock()
        self.i = 0
        self.dead: set[int] = set()
        self.cool_until: dict[int, float] = {}
        self.used: Counter = Counter()

    def acquire(self) -> tuple[int, str]:
        while True:
            with self.lock:
                alive = [k for k in range(len(self.keys)) if k not in self.dead]
                if not alive:
                    raise AllKeysExhausted()
                now = time.time()
                n = len(self.keys)
                for _ in range(n):
                    idx = self.i % n
                    self.i += 1
                    if idx in self.dead or self.cool_until.get(idx, 0) > now:
                        continue
                    self.used[idx] += 1
                    return idx, self.keys[idx]
                wait = min(self.cool_until.get(k, 0) for k in alive) - now
            time.sleep(max(0.2, min(wait, 5)))

    def cooldown(self, idx: int, seconds: float) -> None:
        with self.lock:
            self.cool_until[idx] = max(self.cool_until.get(idx, 0), time.time() + seconds)

    def kill(self, idx: int, reason: str) -> None:
        with self.lock:
            if idx not in self.dead:
                self.dead.add(idx)
                log(f"⚠️  Clé #{idx + 1} retirée ({reason}). "
                    f"Restantes : {len(self.keys) - len(self.dead)}/{len(self.keys)}")

    def summary(self) -> str:
        return ", ".join(
            f"#{i + 1}: {self.used[i]} req{' (épuisée)' if i in self.dead else ''}"
            for i in range(len(self.keys))
        )


def load_keys() -> list[str]:
    raw = os.getenv("FIRECRAWL_API_KEYS") or os.getenv("FIRECRAWL_API_KEY") or ""
    keys = [k.strip() for k in re.split(r"[,;\s]+", raw) if k.strip()]
    return list(dict.fromkeys(keys))


# --------------------------------------------------------------------------- #
# Appel Firecrawl /v2/map
# --------------------------------------------------------------------------- #
def firecrawl_call(key: str, url: str) -> tuple[int, str]:
    payload = {
        "url": url, "limit": URL_LIMIT, "sitemap": SITEMAP,
        "includeSubdomains": INCLUDE_SUBDOMAINS,
        "location": {"country": COUNTRY, "languages": LANGUAGES},
    }
    req = urllib.request.Request(
        FIRECRAWL_URL, data=json.dumps(payload).encode(), method="POST",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_S) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def map_site(pool: KeyPool, url: str, stop: threading.Event) -> dict:
    last_err = "erreur_inconnue"
    for attempt in range(MAX_ATTEMPTS):
        if stop.is_set():
            return {"statut": "interrompu", "links": []}
        idx, key = pool.acquire()  # peut lever AllKeysExhausted
        try:
            code, text = firecrawl_call(key, url)
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            last_err = f"erreur_reseau: {e}"
            time.sleep(min(2 ** attempt, 20))
            continue

        if code == 200:
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                last_err = "erreur_json"
                continue
            if data.get("success") is False:
                return {"statut": "erreur_api", "links": [], "erreur": str(data.get("error"))[:200]}
            links = []
            for l in data.get("links", []) or []:
                if isinstance(l, str):
                    links.append({"url": l})
                elif isinstance(l, dict) and l.get("url"):
                    links.append({"url": l["url"], "title": l.get("title"),
                                  "description": l.get("description")})
            return {"statut": "ok" if links else "aucun_resultat", "links": links}
        if code == 429:
            m = re.search(r"retry after (\d+)", text, re.I)
            pool.cooldown(idx, int(m.group(1)) + 1 if m else KEY_COOLDOWN_S)
            last_err = "erreur_http_429"
            continue
        if code in (401, 402):  # clé invalide / crédits épuisés
            pool.kill(idx, f"HTTP {code}: {text[:80]!r}")
            last_err = f"erreur_http_{code}"
            continue
        if code >= 500 or code == 408:
            last_err = f"erreur_http_{code}"
            time.sleep(min(2 ** attempt, 20))
            continue
        return {"statut": f"erreur_http_{code}", "links": [], "erreur": text[:200]}
    return {"statut": last_err, "links": []}


# --------------------------------------------------------------------------- #
# Entrée / checkpoint
# --------------------------------------------------------------------------- #
def normalize(url: str) -> tuple[str, str]:
    """-> (url à mapper, domaine sans www)."""
    url = url.strip()
    if "//" not in url:
        url = "https://" + url
    u = urlparse(url)
    host = u.netloc.lower().split(":")[0].removeprefix("www.")
    target = f"{u.scheme}://{u.netloc}/" if MAP_FROM_ORIGIN else url
    return target, host


def read_input(path: Path) -> list[dict]:
    if path.suffix.lower() in (".xlsx", ".xlsm"):
        ws = load_workbook(path, read_only=True, data_only=True).worksheets[0]
        it = ws.iter_rows(values_only=True)
        header = [str(h).strip() if h is not None else "" for h in next(it)]
        raw = [dict(zip(header, r)) for r in it]
    else:
        with open(path, newline="", encoding="utf-8-sig") as f:
            raw = list(csv.DictReader(f))
    sites, seen = [], set()
    for r in raw:
        w = str(r.get("website") or "").strip()
        if not w:
            continue
        target, host = normalize(w)
        if host in seen:  # un seul appel par domaine
            continue
        seen.add(host)
        sites.append({"domaine": host, "url_source": w, "url_mappee": target,
                      "title": r.get("title"), "cid": str(r["cid"]) if r.get("cid") else None})
    return sites


def load_done() -> set[str]:
    done: set[str] = set()
    if RAW_FILE.exists():
        with open(RAW_FILE, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("statut") in ("ok", "aucun_resultat"):
                    done.add(rec["domaine"])
    return done


# --------------------------------------------------------------------------- #
# Commande : map
# --------------------------------------------------------------------------- #
def cmd_map(args) -> int:
    pool = KeyPool(load_keys())
    log(f"{len(pool.keys)} clé(s) API, {args.workers} threads, limite {URL_LIMIT} URLs/site.")
    sites = read_input(Path(args.input))
    done = load_done()
    todo = [s for s in sites if s["domaine"] not in done]
    if args.limit:
        todo = todo[: args.limit]
    log(f"{len(sites)} sites uniques, {len(done)} déjà faits, {len(todo)} à traiter.")
    if not todo:
        return 0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()
    deadline = time.time() + MAX_RUNTIME_MIN * 60
    counters: Counter = Counter()
    t0 = time.time()

    def work(site: dict):
        if stop.is_set():
            return site, None
        try:
            return site, map_site(pool, site["url_mappee"], stop)
        except AllKeysExhausted:
            stop.set()
            return site, None

    with open(RAW_FILE, "a", encoding="utf-8") as out, \
            ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(work, s) for s in todo]
        for n, fut in enumerate(as_completed(futures), start=1):
            site, res = fut.result()
            if res is None or res["statut"] == "interrompu":
                continue
            out.write(json.dumps({**site, **res}, ensure_ascii=False) + "\n")
            out.flush()
            counters[res["statut"]] += 1
            if n % 50 == 0:
                log(f"[{n}/{len(todo)}] {dict(counters)} — {n / max(time.time() - t0, 1):.2f} sites/s")
            if time.time() > deadline and not stop.is_set():
                log("⏱️  Budget de temps atteint : arrêt propre (reprise au prochain run).")
                stop.set()

    log(f"Terminé : {dict(counters)}")
    log(f"Usage des clés : {pool.summary()}")
    if len(pool.dead) == len(pool.keys):
        log("❌ Toutes les clés sont épuisées : ajouter des clés/crédits puis relancer.")
    remaining = len(todo) - sum(counters.values())
    if remaining:
        log(f"Reste {remaining} site(s) à traiter : relancer le workflow pour continuer.")
    return 0


# --------------------------------------------------------------------------- #
# Commande : build
# --------------------------------------------------------------------------- #
def cmd_build(args) -> int:
    if not RAW_FILE.exists():
        raise SystemExit(f"{RAW_FILE} introuvable : lancer d'abord « map ».")
    latest: dict[str, dict] = {}
    with open(RAW_FILE, encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            latest[rec["domaine"]] = rec  # la dernière tentative l'emporte
    recs = sorted(latest.values(), key=lambda r: r["domaine"])

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    url_cols = ["domaine", "title", "cid", "url", "titre_page", "description_page"]
    n_urls = 0
    sheet_i, rows_in_sheet = 0, XLSX_ROW_CAP  # force la création de la 1re feuille
    wb = Workbook(write_only=True)
    ws = None
    with open(OUT_DIR / "urls.csv", "w", newline="", encoding="utf-8-sig") as fcsv:
        w = csv.writer(fcsv)
        w.writerow(url_cols)
        for r in recs:
            for l in r.get("links", []):
                row = [r["domaine"], r.get("title"), r.get("cid"), l["url"],
                       l.get("title"), l.get("description")]
                w.writerow(row)
                if rows_in_sheet >= XLSX_ROW_CAP:
                    sheet_i += 1
                    ws = wb.create_sheet("URLs" if sheet_i == 1 else f"URLs {sheet_i}")
                    ws.append(url_cols)
                    rows_in_sheet = 1
                ws.append(row)
                rows_in_sheet += 1
                n_urls += 1

    ss = wb.create_sheet("Sites")
    ss.append(["domaine", "title", "cid", "url_mappee", "statut", "nb_urls"])
    stats = Counter()
    for r in recs:
        ss.append([r["domaine"], r.get("title"), r.get("cid"), r["url_mappee"],
                   r["statut"], len(r.get("links", []))])
        stats[r["statut"]] += 1
    sy = wb.create_sheet("Synthèse")
    sy.append(["Indicateur", "Valeur"])
    sy.append(["Sites traités", len(recs)])
    for k, v in sorted(stats.items()):
        sy.append([f"  statut {k}", v])
    sy.append(["URLs récupérées", n_urls])
    wb.save(OUT_DIR / "urls.xlsx")
    log(f"✅ {len(recs)} sites, {n_urls} URLs -> {OUT_DIR / 'urls.csv'} et {OUT_DIR / 'urls.xlsx'}")
    log(f"   {dict(stats)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("map", help="appelle Firecrawl /map (reprend où il s'est arrêté)")
    m.add_argument("--input", default="data/sites.xlsx")
    m.add_argument("--limit", type=int, default=0, help="nb max de sites (0 = tous)")
    m.add_argument("--workers", type=int, default=WORKERS)
    m.set_defaults(func=cmd_map)
    b = sub.add_parser("build", help="génère urls.csv / urls.xlsx")
    b.set_defaults(func=cmd_build)
    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
