import os
import time
import shutil
import requests
import pandas as pd


# =========================
# KONFIGURACJA
# =========================

BASE_URL = "https://api.raporty.pse.pl/api/gen-jw"

CSV_FILE = "PSE_generation_per_unit_since_10_march.csv"

# od kiedy ma zacząć, jeśli pliku jeszcze nie ma
START_DATE = "2026-03-10"

FAILED_CSV = "PSE_generation_per_unit_failed.csv"

REQUEST_TIMEOUT = 90
SLEEP_BETWEEN_PAGES = 3.0

MAX_PAGES = 300
MAX_RETRIES = 8

TIMEZONE = "Europe/Warsaw"


# =========================
# FUNKCJE POMOCNICZE
# =========================

def get_next_link(data):
    return (
        data.get("@odata.nextLink")
        or data.get("odata.nextLink")
        or data.get("nextLink")
    )


def pick(row, *names):
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return None


def format_value(value):
    if pd.isna(value):
        return ""

    value = float(value)

    if value.is_integer():
        return str(int(value))

    return f"{value:.4f}".rstrip("0").rstrip(".")


def timestamp_from_period(row):
    """
    Timestamp bierzemy jako KONIEC okresu.
    Najlepiej używać dtime, bo w PSE oznacza koniec 15-minutowego okresu.

    Czyli:
    period = 23:45 - 00:00
    dtime  = kolejny dzień 00:00
    """

    dtime = pick(
        row,
        "dtime",
        "timestamp",
        "source_datetime"
    )

    if dtime is not None:
        return pd.to_datetime(dtime, errors="coerce")

    business_date = pick(
        row,
        "business_date",
        "businessDate",
        "doba_handlowa"
    )

    period = pick(
        row,
        "period",
        "udtczas_oreb",
        "trading_period"
    )

    if business_date is None or period is None:
        return pd.NaT

    period = str(period)

    if "-" not in period:
        return pd.NaT

    start_time = period.split("-")[0].strip()
    end_time = period.split("-")[1].strip()

    start_ts = pd.to_datetime(
        str(business_date) + " " + start_time,
        errors="coerce"
    )

    end_ts = pd.to_datetime(
        str(business_date) + " " + end_time,
        errors="coerce"
    )

    if pd.isna(start_ts) or pd.isna(end_ts):
        return pd.NaT

    if end_ts <= start_ts:
        end_ts = end_ts + pd.Timedelta(days=1)

    return end_ts


# =========================
# REQUEST Z RETRY
# =========================

def get_with_retry(url, params=None):
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                params=params,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0",
                    "Connection": "close"
                },
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                return response

            print(f"HTTP {response.status_code}, próba {attempt}/{MAX_RETRIES}")
            print(response.text[:500])

            last_error = f"HTTP {response.status_code}: {response.text[:500]}"

            if response.status_code in [429, 500, 502, 503, 504]:
                wait = attempt * 20
                print(f"Czekam {wait} s i próbuję ponownie...")
                time.sleep(wait)
                continue

            return response

        except requests.exceptions.RequestException as e:
            last_error = str(e)

            print(f"Błąd połączenia, próba {attempt}/{MAX_RETRIES}")
            print(last_error[:500])

            wait = attempt * 20
            print(f"Czekam {wait} s i próbuję ponownie...")
            time.sleep(wait)

    raise RuntimeError(
        "Nie udało się pobrać strony po kilku próbach. "
        f"Ostatni błąd: {last_error}"
    )


# =========================
# POBIERANIE JEDNEGO DNIA
# =========================

def fetch_one_day(day):
    url = BASE_URL

    params = {
        "$filter": f"business_date eq '{day}'"
    }

    all_records = []
    failed = []
    success = True

    page = 1
    seen_urls = set()

    while True:
        try:
            response = get_with_retry(url, params)

        except Exception as e:
            print("Nie udało się pobrać strony mimo retry.")
            print(e)

            failed.append({
                "day": day,
                "page": page,
                "url": url,
                "status_code": "connection_error",
                "error": str(e)
            })

            success = False
            break

        print(f"{day} | strona {page} | HTTP {response.status_code}")

        if response.status_code != 200:
            print("Błąd API — dzień nie został pobrany do końca.")
            print(response.text[:1000])

            failed.append({
                "day": day,
                "page": page,
                "url": response.url,
                "status_code": response.status_code,
                "error": response.text[:1000]
            })

            success = False
            break

        current_url = response.url

        if current_url in seen_urls:
            print("API zwróciło ten sam URL drugi raz — przerywam, żeby uniknąć pętli.")

            failed.append({
                "day": day,
                "page": page,
                "url": response.url,
                "status_code": "repeated_url",
                "error": "API returned the same URL twice"
            })

            success = False
            break

        seen_urls.add(current_url)

        data = response.json()
        records = data.get("value", [])

        all_records.extend(records)

        print("Rekordów na stronie:", len(records))
        print("Rekordów surowych razem:", len(all_records))

        next_link = get_next_link(data)

        if not next_link:
            print("Brak nextLink — dzień pobrany do końca.")
            break

        if page >= MAX_PAGES:
            print("Osiągnięto MAX_PAGES — przerywam zabezpieczająco.")

            failed.append({
                "day": day,
                "page": page,
                "url": response.url,
                "status_code": "max_pages",
                "error": "Reached MAX_PAGES"
            })

            success = False
            break

        url = next_link
        params = None
        page += 1

        time.sleep(SLEEP_BETWEEN_PAGES)

    return all_records, failed, success


# =========================
# OBRÓBKA DANYCH Z API
# =========================

def process_records(records):
    rows = []

    for row in records:
        kod_jw = pick(
            row,
            "resource_code",
            "kod_mwe",
            "kod_jw",
            "unit_id"
        )

        wartosc_mw = pick(
            row,
            "value",
            "wartosc",
            "wartość"
        )

        elektrownia = pick(
            row,
            "power_plant",
            "nazwa_mwe",
            "elektrownia"
        )

        tryb_pracy = pick(
            row,
            "operating_mode",
            "tryb_pracy"
        )

        timestamp = timestamp_from_period(row)

        if kod_jw is None or wartosc_mw is None or elektrownia is None or pd.isna(timestamp):
            continue

        wartosc_mw_num = pd.to_numeric(wartosc_mw, errors="coerce")

        if pd.isna(wartosc_mw_num):
            continue

        # Usuwamy 0 MW, zostawiamy wartości dodatnie i ujemne.
        if abs(float(wartosc_mw_num)) < 1e-9:
            continue

        rows.append({
            "kod_jw": str(kod_jw).strip(),
            "timestamp": timestamp,
            "wartosc_mw": float(wartosc_mw_num),
            "elektrownia": str(elektrownia).strip(),
            "tryb_pracy": str(tryb_pracy).strip() if tryb_pracy is not None else "Generacja"
        })

    final = pd.DataFrame(rows)

    if final.empty:
        return final

    final = (
        final
        .drop_duplicates()
        .sort_values(["timestamp", "elektrownia", "kod_jw"])
        .reset_index(drop=True)
    )

    final["timestamp"] = final["timestamp"].dt.strftime("%d.%m.%Y %H:%M")
    final["wartosc_mw"] = final["wartosc_mw"].apply(format_value)

    final = final[
        [
            "kod_jw",
            "timestamp",
            "wartosc_mw",
            "elektrownia",
            "tryb_pracy"
        ]
    ]

    return final


# =========================
# WCZYTANIE ISTNIEJĄCEGO PLIKU
# =========================

def load_existing_file():
    if not os.path.exists(CSV_FILE):
        print("Nie ma jeszcze pliku. Zaczynam od START_DATE.")
        return pd.DataFrame(
            columns=[
                "kod_jw",
                "timestamp",
                "wartosc_mw",
                "elektrownia",
                "tryb_pracy"
            ]
        )

    df = pd.read_csv(
        CSV_FILE,
        header=None,
        names=[
            "kod_jw",
            "timestamp",
            "wartosc_mw",
            "elektrownia",
            "tryb_pracy"
        ],
        encoding="utf-8-sig"
    )

    print("Wczytano istniejący plik:")
    print(CSV_FILE)
    print("Liczba rekordów:", len(df))

    return df


# =========================
# WYBÓR DNIA DO POBRANIA
# =========================

def decide_day_to_fetch(existing_df):
    start_date = pd.to_datetime(START_DATE).date()
    today = pd.Timestamp.now(tz=TIMEZONE).date()

    if existing_df.empty:
        return start_date.strftime("%Y-%m-%d")

    timestamps = pd.to_datetime(
        existing_df["timestamp"],
        format="%d.%m.%Y %H:%M",
        errors="coerce"
    )

    max_ts = timestamps.max()

    if pd.isna(max_ts):
        return start_date.strftime("%Y-%m-%d")

    print("Ostatni timestamp w pliku:", max_ts.strftime("%d.%m.%Y %H:%M"))

    # Logika:
    # Jeżeli ostatni timestamp to np. 11.03.2026 00:00,
    # to znaczy, że skończony został business_date 10.03.2026.
    # Następny dzień do pobrania to data ostatniego timestampu, czyli 2026-03-11.
    candidate_day = max_ts.date()

    if candidate_day < start_date:
        candidate_day = start_date

    # Jeżeli jeszcze nie doszliśmy do dzisiaj, pobieramy kolejny brakujący dzień.
    if candidate_day < today:
        return candidate_day.strftime("%Y-%m-%d")

    # Jeżeli już jesteśmy na dzisiaj, za każdym uruchomieniem odświeżamy dzisiejszy dzień.
    return today.strftime("%Y-%m-%d")


# =========================
# POŁĄCZENIE I CZYSZCZENIE
# =========================

def combine_and_clean(existing_df, new_df):
    combined_df = pd.concat(
        [
            existing_df,
            new_df
        ],
        ignore_index=True
    )

    print("\nLiczba rekordów przed czyszczeniem:")
    print(len(combined_df))

    combined_df["wartosc_mw_num"] = pd.to_numeric(
        combined_df["wartosc_mw"],
        errors="coerce"
    )

    combined_df = combined_df[
        combined_df["wartosc_mw_num"].notna()
        & (combined_df["wartosc_mw_num"].abs() > 1e-9)
    ].copy()

    combined_df = combined_df.drop(columns=["wartosc_mw_num"])

    combined_df = combined_df.drop_duplicates().reset_index(drop=True)

    combined_df["_sort_ts"] = pd.to_datetime(
        combined_df["timestamp"],
        format="%d.%m.%Y %H:%M",
        errors="coerce"
    )

    combined_df = (
        combined_df
        .sort_values(["_sort_ts", "elektrownia", "kod_jw"])
        .drop(columns=["_sort_ts"])
        .reset_index(drop=True)
    )

    print("Liczba rekordów po usunięciu zer i duplikatów:")
    print(len(combined_df))

    return combined_df


# =========================
# GŁÓWNE URUCHOMIENIE
# =========================

existing_df = load_existing_file()

day_to_fetch = decide_day_to_fetch(existing_df)

print("\nDzień wybrany do pobrania:")
print(day_to_fetch)

# Backup przed zmianą pliku
if os.path.exists(CSV_FILE):
    backup_csv = CSV_FILE.replace(".csv", "_backup_latest.csv")
    shutil.copyfile(CSV_FILE, backup_csv)
    print("Zrobiono backup:", backup_csv)

records, failed, success = fetch_one_day(day_to_fetch)

if failed:
    failed_df = pd.DataFrame(failed)

    if os.path.exists(FAILED_CSV):
        old_failed = pd.read_csv(FAILED_CSV, encoding="utf-8-sig")
        failed_df = pd.concat([old_failed, failed_df], ignore_index=True)

    failed_df.to_csv(
        FAILED_CSV,
        index=False,
        encoding="utf-8-sig"
    )

    print("Zapisano błędy do:", FAILED_CSV)

if not success:
    print("\nDzień nie został pobrany do końca.")
    print("Nie zapisuję zmian do głównego pliku.")
    raise SystemExit

new_df = process_records(records)

if new_df.empty:
    print("Po obróbce nie ma nowych rekordów. Nie zapisuję zmian.")
    raise SystemExit

print("\nNowe rekordy po obróbce:")
print(len(new_df))

print("\nPodgląd nowych danych:")
print(new_df.head(30).to_string(index=False))

combined_df = combine_and_clean(existing_df, new_df)

combined_df.to_csv(
    CSV_FILE,
    index=False,
    header=False,
    encoding="utf-8-sig"
)

print("\nGotowe.")
print("Zaktualizowany plik:")
print(CSV_FILE)

print("\nOstatnie rekordy w pliku:")
print(combined_df.tail(50).to_string(index=False))
