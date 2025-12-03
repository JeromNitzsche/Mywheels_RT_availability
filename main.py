import os
import json
import time
from datetime import datetime, timedelta

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ==========================
# CONFIG
# ==========================

API_URL = "https://prod-api.mywheels.nl/api/"
SHEET_ID = "1WqOPUbMvW3zF6NG5ZFQbJ4FJifsAwKGUsux1hjwMljM"
SHEET_RANGE = "Data!A:E"  # A: License, B: City, C: Score, D: Franchise, E: ID

# Window & blokken
BLOCK_MINUTES = 15
WINDOW_HOURS = 10
FREE_REQUIRED_MINUTES = 30

# Service account file (wordt door GitHub workflow geschreven als 'sa.json')
SA_PATH = "sa.json"

# ==========================
# HELPERS
# ==========================

def clean_license(lic: str) -> str:
    """Maak kenteken geschikt als JSON-key: alle streepjes/spaties eruit."""
    if lic is None:
        return ""
    return "".join(ch for ch in lic.replace(" ", "").replace("-", "").upper())


def get_credentials():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = service_account.Credentials.from_service_account_file(
        SA_PATH, scopes=scopes
    )
    return creds


def load_cars_from_sheet():
    """
    Leest tab 'Data' in de sheet.
    Verwacht:
      kolom A: License Plate
      kolom B: City
      kolom C: Cleaning Score
      kolom D: Franchise
      kolom E: ID (resource_id)
    """

    print("📄 Sheet uitlezen...")
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    resp = sheet.values().get(spreadsheetId=SHEET_ID, range=SHEET_RANGE).execute()
    values = resp.get("values", [])

    if not values:
        print("⚠️ Geen data gevonden in sheet.")
        return []

    rows = values[1:]

    cars = []
    for row in rows:
        row = row + [""] * (5 - len(row))

        license_plate = row[0]
        city = row[1]
        franchise = row[3]
        resource_id_str = row[4]

        if not resource_id_str:
            continue

        try:
            resource_id = int(resource_id_str)
        except ValueError:
            continue

        cars.append(
            {
                "resource_id": resource_id,
                "license_raw": license_plate,
                "license_clean": clean_license(license_plate),
                "city": city,
                "franchise": franchise,
            }
        )

    print(f"🚗 {len(cars)} auto's geladen uit sheet.")
    return cars


# ==========================
# API CALL
# ==========================

DEBUG_LOG_DONE = False  # zorgt dat we maar 1 debug-output printen

def fetch_calendar_availability(resource_id: int, start_dt: datetime, end_dt: datetime):
    global DEBUG_LOG_DONE

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "search.calendarAvailability",
        "params": {
            "resource": resource_id,
            "timeFrame": {
                "startDate": start_dt.strftime("%Y-%m-%d %H:%M"),
                "endDate": end_dt.strftime("%Y-%m-%d %H:%M"),
            },
        },
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    r = requests.post(API_URL, json=payload, headers=headers, timeout=20)
    r.raise_for_status()

    data = r.json()

    # ⬇️ **DEBUG OUTPUT** alleen voor de eerste auto
    if not DEBUG_LOG_DONE:
        print("\n================ DEBUG MYWHEELS RESPONSE ================\n")
        try:
            print(json.dumps(data, indent=2))
        except:
            print(data)
        print("\n==========================================================\n")
        DEBUG_LOG_DONE = True

    # MyWheels response structuur kennen we nog niet → dit crasht nu
    # Dus return raw voor nu
    return data


# ==========================
# MAIN AVAILABILITY BUILDER
# (werkt nog niet correct tot we de response kennen)
# ==========================

def build_availability_for_car(resource_id: int, license_clean: str,
                               start_dt: datetime, end_dt: datetime):

    # Haal ruwe data op
    data = fetch_calendar_availability(resource_id, start_dt, end_dt)

    # Zolang API structuur onbekend is → return leeg resultaat
    return {
        "no_availability_all_day": False,
        "conflict_tijden": ""
    }


# ==========================
# MAIN
# ==========================

def main():
    now = datetime.now().replace(second=0, microsecond=0)
    start_dt = now
    end_dt = now + timedelta(hours=WINDOW_HOURS)

    print(f"⏱ Window: {start_dt} → {end_dt}")

    cars = load_cars_from_sheet()
    if not cars:
        print("⚠️ Geen auto's om te verwerken, stoppen.")
        return

    availability = {}

    for idx, car in enumerate(cars, start=1):
        rid = car["resource_id"]
        lic_clean = car["license_clean"] or f"ID_{rid}"

        print(f"🔎 [{idx}/{len(cars)}] resource {rid} ({lic_clean})")

        entry = build_availability_for_car(rid, lic_clean, start_dt, end_dt)
        availability[lic_clean] = entry

        time.sleep(0.15)

    out_path = "availability.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(availability, f, ensure_ascii=False, indent=2)

    print(f"✅ availability.json opgeslagen in {out_path}")
    print(f"🚗 Totaal {len(availability)} auto's verwerkt.")


if __name__ == "__main__":
    main()
