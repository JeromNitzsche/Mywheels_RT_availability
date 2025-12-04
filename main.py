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
BLOCK_MINUTES = 15          # marge per kant als auto NIET full-day bezet is
WINDOW_HOURS = 10           # vooruitkijk-window
FREE_REQUIRED_MINUTES = 30  # minimaal vrije blok voor "niet full-day bezet"

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

    rows = values[1:]  # header overslaan

    cars = []
    for row in rows:
        # Zorg dat we minstens 5 kolommen hebben
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


def parse_slot_datetime(s: str) -> datetime:
    """
    Probeert slot-start/end naar datetime te parsen.
    Ondersteunt:
      - 'YYYY-MM-DD HH:MM'
      - ISO 'YYYY-MM-DDTHH:MM[:SS][+offset]'
    """
    if not s:
        raise ValueError("Empty datetime string")

    s_fixed = s.replace("Z", "+00:00")

    # Eerst ISO proberen
    try:
        return datetime.fromisoformat(s_fixed)
    except ValueError:
        pass

    # Dan 'YYYY-MM-DD HH:MM'
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M")
    except ValueError:
        raise


def merge_blocks(blocks):
    """Merge overlappende tijdsblokken (lijst van (start, eind))."""
    if not blocks:
        return []
    blocks = sorted(blocks, key=lambda x: x[0])
    merged = [blocks[0]]
    for s, e in blocks[1:]:
        last_s, last_e = merged[-1]
        if s <= last_e:
            merged[-1] = (last_s, max(last_e, e))
        else:
            merged.append((s, e))
    return merged


def format_blocks(blocks):
    """
    Format als 'HH:MM–HH:MM, HH:MM–HH:MM'.
    Mag over middernacht gaan; dat maakt niet uit voor HH:MM.
    """
    return ", ".join(f"{s.strftime('%H:%M')}–{e.strftime('%H:%M')}" for s, e in blocks)


# ==========================
# API CALL
# ==========================

DEBUG_LOG_DONE = False  # zorgt dat we maar 1 debug-output printen


def fetch_calendar_availability(resource_id: int, start_dt: datetime, end_dt: datetime):
    """
    Roept search.calendarAvailability aan en geeft een lijst met slots terug.
    Elk slot is idealiter een dict met:
      - start / from
      - end / to
      - available / isAvailable (True/False)
    Als de structuur anders is of leeg blijft, geven we een lege lijst terug.
    """
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

    # Debug één keer de ruwe response, zodat we later kunnen tweaken
    if not DEBUG_LOG_DONE:
        print("\n================ DEBUG MYWHEELS RESPONSE ================\n")
        try:
            print(json.dumps(data, indent=2))
        except Exception:
            print(data)
        print("\n==========================================================\n")
        DEBUG_LOG_DONE = True

    # MyWheels kan lijst of dict teruggeven
    if isinstance(data, list) and data:
        env = data[0]
    elif isinstance(data, dict):
        env = data
    else:
        return []

    result = env.get("result", [])

    # result kan óók weer lijst of dict zijn
    if isinstance(result, list):
        # als het direct een lijst van slots is
        return result

    if isinstance(result, dict):
        slots = (
            result.get("availability")
            or result.get("slots")
            or result.get("timeSlots")
            or []
        )
        if isinstance(slots, list):
            return slots

    # Als we hier komen is de structuur onbekend → geen slots
    return []


# ==========================
# AVAILABILITY VOOR ÉÉN AUTO
# ==========================

def build_availability_for_car(resource_id: int, license_clean: str,
                               start_dt: datetime, end_dt: datetime):

    # Haal MyWheels blokken op
    try:
        slots = fetch_calendar_availability(resource_id, start_dt, end_dt)
    except Exception as e:
        print(f"⚠️ Fout bij resource {resource_id}: {e}")
        return {
            "no_availability_all_day": False,
            "conflict_tijden": "",
        }

    conflict_blocks = []

    # MyWheels geeft BEZETTE BLOKKEN met:
    # startDate, endDate, refuelTime
    for slot in slots:
        if not isinstance(slot, dict):
            continue
        
        start_str = slot.get("startDate")
        end_str   = slot.get("endDate")
        refuel    = slot.get("refuelTime", 0)

        if not start_str or not end_str:
            continue
        
        try:
            s = parse_slot_datetime(start_str)
            e = parse_slot_datetime(end_str)
        except Exception:
            continue

        # refuelTime toevoegen → "uitloop-blokkade"
        if isinstance(refuel, (int, float)):
            e += timedelta(minutes=refuel)

        # Alleen blokken in ons window
        if e <= start_dt or s >= end_dt:
            continue

        # Clip aan window
        s = max(s, start_dt)
        e = min(e, end_dt)

        conflict_blocks.append((s, e))

    # Merge overlappende blokken
    conflict_blocks = merge_blocks(conflict_blocks)

    # Zoek of er 30 min vrije ruimte is
    free_period_found = False
    current = start_dt

    for s, e in conflict_blocks:
        if s > current:
            if (s - current) >= timedelta(minutes=FREE_REQUIRED_MINUTES):
                free_period_found = True
                break
        current = max(current, e)

    # Check na laatste blok
    if current < end_dt:
        if (end_dt - current) >= timedelta(minutes=FREE_REQUIRED_MINUTES):
            free_period_found = True

    no_availability_all_day = not free_period_found

    # Marge: alleen als auto NIET all-day bezet is
    if not no_availability_all_day:
        corrected = []

        for s, e in conflict_blocks:
            new_s = s
            new_e = e

            if e < end_dt:
                new_e -= timedelta(minutes=BLOCK_MINUTES)

            if s > start_dt:
                new_s += timedelta(minutes=BLOCK_MINUTES)

            if new_e > new_s:
                corrected.append((new_s, new_e))

        conflict_blocks = merge_blocks(corrected)

    conflict_str = format_blocks(conflict_blocks) if conflict_blocks else ""

    return {
        "no_availability_all_day": no_availability_all_day,
        "conflict_tijden": conflict_str,
    }


# ==========================
# MAIN
# ==========================

def main():
    # Start & eind in lokale tijd
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

        # Klein sleepje om 429 risico nog kleiner te maken
        time.sleep(0.2)

    # JSON in repo-root (voor GitHub Pages)
    out_path = "availability.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(availability, f, ensure_ascii=False, indent=2)

    print(f"✅ availability.json opgeslagen in {out_path}")
    print(f"🚗 Totaal {len(availability)} auto's verwerkt.")


if __name__ == "__main__":
    main()
