import os
import time
import shutil
from urllib.parse import quote, urljoin

import requests
import pandas as pd


# =========================
# KONFIGURACJA
# =========================

BASE_URL = "https://api.raporty.pse.pl/api/gen-jw"

CSV_FILE = "PSE_generation_per_unit_since_10_march.csv"
FAILED_CSV = "PSE_generation_per_unit_failed.csv"

START_DATE = "2026-03-10"
TIMEZONE = "Europe/Warsaw"

REQUEST_TIMEOUT = 90
SLEEP_BETWEEN_PAGES = 2.0

MAX_PAGES = 500
MAX_RETRIES = 8

# Ile dni maksymalnie pobrać przy jednym uruchomieniu workflow
MAX_DAYS_PER_RUN = 3

PAGE_SIZE = 1000

SELECT_FIELDS = (
    "resource_code,value,power_plant,operating_mode,"
    "dtime,period,business_date"
)

COLUMNS = [
    "kod_jw",
    "timestamp",
    "wartosc_mw",
    "elektrownia",
    "tryb_pracy"
]


# =========================
# URL DO API
# =========================

def build_first_page_url(day):
    """
    Ważne:
    Nie używamy requests params={...}, bo requests zmienia $filter na %24filter.
    API PSE zaczęło tego nie przyjmować.
    Dlatego $filter, $select, $orderby, $top są wpisane ręcznie w URL.
    """

    filter_value = quote(f"business_date eq '{day}'", safe="")
    select_value = quote(SELECT_FIELDS, safe=",")
    orderby_value = quote("dtime,resource_code", safe=",")

    return (
        f"{BASE_URL}"
        f"?$filter={filter_value}"
        f"&$select={select_value}"
        f"&$orderby={orderby_value}"
        f"&$top={PAGE_SIZE}"
    )


def normalize_next_link(next_link):
    if not next_link:
        return None

    if next_link.startswith("http"):
        full_url = next_link
    elif next_link.startswith("/"):
        full_url = urljoin("https://api.raporty.pse.pl", next_link)
    else:
        full_url = urljoin(BASE_URL, next_link)

    # Gdyby API zwróciło zakodowane nazwy parametrów OData, poprawiamy tylko %24 na $
    full_url = full_url.replace("%24", "$")

    return full_url


def get_next_link(data):
    return (
        data.get("@odata.nextLink")
        or data.get("odata.nextLink")
        or data.get("nextLink")
    )


# =========================
# FUNKCJE POMOCNICZE
# =========================

def pick(row, *names):
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return None


def parse_mw(value):
    if pd.isna(value):
        return pd.NA

    value = str(value).strip().replace(",", ".")

    return pd.to_numeric(value, errors="coerce")


def format_value(value):
    if pd.isna(value):
        return ""

    value = float(value)

    if value.is_integer():
        return str(int(value))

    return f"{value:.4f}".rstrip("0").rstrip(".")


def timestamp_from_period(row):
    """
    Timestamp traktujemy jako KONIEC okresu.
    Najlepiej używać pola dtime, bo w PSE oznacza koniec okresu 15-minutowego.

    Przykład:
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


def save_failed_rows(failed_rows):
    if not failed_rows:
        return

    failed_df = pd.DataFrame(failed_rows)

    print("\nBŁĘDY POBIERANIA:")
    print(failed_df.tail(30).to_string(index=False))

    if os.path.exists(FAILED_CSV):
        old_failed = pd.read_csv(FAILED_CSV, encoding="utf-8-sig")
        failed_df = pd.concat([old_failed, failed_df], ignore_index=True)

    failed_df = failed_df.drop_duplicates().reset_index(drop=True)

    failed_df.to_csv(
        FAILED_CSV,
        index=False,
        encoding="utf-8-sig"
    )

    print("\nZapisano błędy do:", FAILED_CSV)


def atomic_save_csv(df, path):
    temp_path = path + ".tmp"

    df.to_csv(
        temp_path,
        index=False,
        header=False,
        encoding="utf-8-sig"
    )

    os.replace(temp_path, path)


# =========================
# REQUEST Z RETRY
# =========================

def get_with_retry(url):
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0",
                    "Connection": "close"
                },
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                return response

            print(f"\nHTTP {response.status_code}, próba {attempt}/{MAX_RETRIES}")
            print("URL:", response.url)
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

            print(f"\nBłąd połączenia, próba {attempt}/{MAX_RETRIES}")
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
    url = build_first_page_url(day)

    all_records = []
    failed = []

    page = 1
    seen_urls = set()

    while True:
        try:
            response = get_with_retry(url)

        except Exception as e:
            print("\nNie udało się pobrać strony mimo retry.")
            print(e)

            failed.append({
                "day": day,
                "page": page,
                "url": url,
                "status_code": "connection_error",
                "error": str(e),
                "records_downloaded_before_failure": len(all_records)
            })

            return all_records, failed, False

        print(f"\n{day} | strona {page} | HTTP {response.status_code}")
        print("URL:", response.url)

        if response.status_code != 200:
            print("Błąd API — dzień nie został pobrany do końca.")
            print(response.text[:1000])

            failed.append({
                "day": day,
                "page": page,
                "url": response.url,
                "status_code": response.status_code,
                "error": response.text[:1000],
                "records_downloaded_before_failure": len(all_records)
            })

            return all_records, failed, False

        current_url = response.url

        if current_url in seen_urls:
            print("API zwróciło ten sam URL drugi raz — przerywam, żeby uniknąć pętli.")

            failed.append({
                "day": day,
                "page": page,
                "url": response.url,
                "status_code": "repeated_url",
                "error": "API returned the same URL twice",
                "records_downloaded_before_failure": len(all_records)
            })

            return all_records, failed, False

        seen_urls.add(current_url)

        try:
            data = response.json()
        except Exception as e:
            failed.append({
                "day": day,
                "page": page,
                "url": response.url,
                "status_code": "json_error",
                "error": str(e),
                "records_downloaded_before_failure": len(all_records)
            })

            return all_records, failed, False

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
                "error": "Reached MAX_PAGES",
                "records_downloaded_before_failure": len(all_records)
            })

            return all_records, failed, False

        url = normalize_next_link(next_link)
        page += 1

        time.sleep(SLEEP_BETWEEN_PAGES)

    return all_records, failed, True


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

        wartosc_mw_num = parse_mw(wartosc_mw)

        if pd.isna(wartosc_mw_num):
            continue

        # Usuwamy 0 MW, zostawiamy wartości dodatnie i ujemne
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
        return pd.DataFrame(columns=COLUMNS)

    final = (
        final
        .drop_duplicates()
        .sort_values(["timestamp", "elektrownia", "kod_jw"])
        .reset_index(drop=True)
    )

    final["timestamp"] = final["timestamp"].dt.strftime("%d.%m.%Y %H:%M")
    final["wartosc_mw"] = final["wartosc_mw"].apply(format_value)

    final = final[COLUMNS]

    return final


# =========================
# WCZYTANIE ISTNIEJĄCEGO PLIKU
# =========================

def load_existing_file():
    if not os.path.exists(CSV_FILE):
        print("Nie ma jeszcze pliku. Zaczynam od START_DATE.")

        return pd.DataFrame(columns=COLUMNS)

    df = pd.read_csv(
        CSV_FILE,
        header=None,
        names=COLUMNS,
        encoding="utf-8-sig"
    )

    print("Wczytano istniejący plik:")
    print(CSV_FILE)
    print("Liczba rekordów:", len(df))

    return df


# =========================
# WYBÓR DNI DO POBRANIA
# =========================

def decide_days_to_fetch(existing_df):
    start_date = pd.to_datetime(START_DATE).date()
    today = pd.Timestamp.now(tz=TIMEZONE).date()

    if existing_df.empty:
        first_day = start_date
    else:
        timestamps = pd.to_datetime(
            existing_df["timestamp"],
            format="%d.%m.%Y %H:%M",
            errors="coerce"
        )

        max_ts = timestamps.max()

        if pd.isna(max_ts):
            first_day = start_date
        else:
            print("Ostatni timestamp w pliku:", max_ts.strftime("%d.%m.%Y %H:%M"))

            # Jeżeli ostatni timestamp to np. 01.06.2026 00:00,
            # to znaczy, że skończony został business_date 31.05.2026.
            # Następny business_date to data ostatniego timestampu, czyli 2026-06-01.
            first_day = max_ts.date()

            if first_day < start_date:
                first_day = start_date

    if first_day >= today:
        return [today.strftime("%Y-%m-%d")]

    all_days = pd.date_range(
        start=first_day,
        end=today,
        freq="D"
    )

    all_days = all_days[:MAX_DAYS_PER_RUN]

    return [d.strftime("%Y-%m-%d") for d in all_days]


# =========================
# POŁĄCZENIE I CZYSZCZENIE
# =========================

def combine_and_clean(existing_df, new_df):
    combined_df = pd.concat(
        [existing_df, new_df],
        ignore_index=True
    )

    print("\nLiczba rekordów przed czyszczeniem:")
    print(len(combined_df))

    combined_df["wartosc_mw_num"] = (
        combined_df["wartosc_mw"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )

    combined_df = combined_df[
        combined_df["wartosc_mw_num"].notna()
        & (combined_df["wartosc_mw_num"].abs() > 1e-9)
    ].copy()

    combined_df["wartosc_mw"] = combined_df["wartosc_mw_num"].apply(format_value)
    combined_df = combined_df.drop(columns=["wartosc_mw_num"])

    combined_df["_sort_ts"] = pd.to_datetime(
        combined_df["timestamp"],
        format="%d.%m.%Y %H:%M",
        errors="coerce"
    )

    combined_df = combined_df[combined_df["_sort_ts"].notna()].copy()

    # Jeśli ten sam kod_jw i timestamp pojawią się drugi raz,
    # zostawiamy nowszy rekord z nowego pobrania.
    combined_df = combined_df.drop_duplicates(
        subset=["kod_jw", "timestamp"],
        keep="last"
    )

    combined_df = (
        combined_df
        .sort_values(["_sort_ts", "elektrownia", "kod_jw"])
        .drop(columns=["_sort_ts"])
        .reset_index(drop=True)
    )

    combined_df = combined_df[COLUMNS]

    print("Liczba rekordów po usunięciu zer i duplikatów:")
    print(len(combined_df))

    return combined_df


# =========================
# GŁÓWNE URUCHOMIENIE
# =========================

def main():
    existing_df = load_existing_file()

    days_to_fetch = decide_days_to_fetch(existing_df)

    print("\nDni wybrane do pobrania:")
    for day in days_to_fetch:
        print(day)

    if os.path.exists(CSV_FILE):
        backup_csv = CSV_FILE.replace(".csv", "_backup_latest.csv")
        shutil.copyfile(CSV_FILE, backup_csv)
        print("\nZrobiono backup:", backup_csv)

    all_new_dfs = []
    all_failed = []

    for day in days_to_fetch:
        print("\n" + "=" * 60)
        print("POBIERAM DZIEŃ:", day)
        print("=" * 60)

        records, failed, success = fetch_one_day(day)

        if failed:
            all_failed.extend(failed)

        if not success:
            print(f"\nDzień {day} nie został pobrany do końca.")
            print("Przerywam, żeby nie zrobić dziury w danych.")
            break

        new_df = process_records(records)

        if new_df.empty:
            print(f"Po obróbce dzień {day} nie ma rekordów.")
            continue

        print(f"\nNowe rekordy po obróbce dla dnia {day}:")
        print(len(new_df))

        print("\nPodgląd nowych danych:")
        print(new_df.head(20).to_string(index=False))

        all_new_dfs.append(new_df)

    if all_failed:
        save_failed_rows(all_failed)

    if not all_new_dfs:
        print("\nNie ma żadnych nowych poprawnie pobranych danych.")
        print("Nie zapisuję zmian do głównego pliku.")
        return

    new_all_df = pd.concat(all_new_dfs, ignore_index=True)

    print("\nŁączna liczba nowych rekordów:")
    print(len(new_all_df))

    combined_df = combine_and_clean(existing_df, new_all_df)

    atomic_save_csv(combined_df, CSV_FILE)

    print("\nGotowe.")
    print("Zaktualizowany plik:")
    print(CSV_FILE)

    print("\nOstatnie rekordy w pliku:")
    print(combined_df.tail(50).to_string(index=False))

    if all_failed:
        print("\nUWAGA: część dni/stron miała błędy.")
        print("Główny plik został zaktualizowany tylko poprawnie pobranymi danymi.")
        print("Szczegóły błędów są w:", FAILED_CSV)


if __name__ == "__main__":
    main()
