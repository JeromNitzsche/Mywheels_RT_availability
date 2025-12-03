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
    """Leest alle auto's uit de Google Sheet."""
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
        row = row + [""] * (5 - len(row))  # minstens 5 kolommen

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


def fetch_calendar_availability(resource_id: int, start_dt: datetime, end_dt: datetime):
    """1 bulk-call naar MyWheels voor 10 uur window."""
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

    r = requests.post(API_URL, json=payload, headers=headers, timeout=15)
    r.raise_for_status()

    data = r.json()

    # De API retourneert óf dict, óf list
    env = data[0] if isinstance(data, list) else data

    result = env.get("result", {})
    slots = result.get("availability") or result.get("slots") or []

    return slots


def parse_slot_datetime(s: str) -> datetime:
    """Parset de datum van MyWheels slot."""
    if not s:
        raise ValueError("Empty datetime string")

    s_fixed = s.replace("Z", "+00:00")

    try:
        return datetime.fromisoformat(s_fixed)
    except ValueError:
        return datetime.strptime(s, "%Y-%m-%d %H:%M")


def merge_blocks(blocks):
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
    return ", ".join(f"{s.strftime('%H:%M')}–{e.strftime('%H:%M')}" for s, e in blocks)


def build_availability_for_car(resource_id: int, license_clean: str,
                               start_dt: datetime, end_dt: datetime):
    """Bouwt het JSON-object voor één auto."""
    try:
        slots = fetch_calendar_availability(resource_id, start_dt, end_dt)
    except Exception as e:
        print(f"⚠️ Fout bij resource {resource_id}: {e}")
        return {"no_availability_all_day": False, "conflict_tijden": ""}

    # 🔥 FIX: Flatten nested lists
    flat = []
    for x in slots:
        if isinstance(x, list):
            flat.extend(x)
        else:
            flat.append(x)
    slots = flat

    conflict_blocks = []

    for slot in slots:
        if not isinstance(slot, dict):
            continue

        available = slot.get("available", True)
        start_str = slot.get("start") or slot.get("from")
        end_str = slot.get("end") or slot.get("to")

        if not start_str or not end_str:
            continue

        try:
            s = parse_slot_datetime(start_str)
            e = parse_slot_datetime(end_str)
        except Exception:
            continue

        if e <= start_dt or s >= end_dt:
            continue

        s = max(s, start_dt)
        e = min(e, end_dt)

        if not available:
            conflict_blocks.append((s, e))

    conflict_blocks = merge_blocks(conflict_blocks)

    # Bepalen of er vrije tijd is
    free_period_found = False
    current_time = start_dt
    for s, e in conflict_blocks:
        if s > current_time and (s - current_time) >= timedelta(minutes=FREE_REQUIRED_MINUTES):
            free_period_found = True
            break
        current_time = max(current_time, e)

    if current_time < end_dt and (end_dt - current_time) >= timedelta(minutes=FREE_REQUIRED_MINUTES):
        free_period_found = True

    no_availability_all_day = not free_period_found

    # Alleen marges toepassen als hij niet volledig bezet is
    if not no_availability_all_day:
        corrected = []
        for s, e in conflict_blocks:
            if e < end_dt:
                e -= timedelta(minutes=BLOCK_MINUTES)
            if s > start_dt:
                s += timedelta(minutes=BLOCK_MINUTES)
            if e > s:
                corrected.append((s, e))

        conflict_blocks = merge_blocks(corrected)

    conflict_str = format_blocks(conflict_blocks)

    return {
        "no_availability_all_day": no_availability_all_day,
        "conflict_tijden": conflict_str,
    }


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

        time.sleep(0.2)  # anti-429

    # JSON wegschrijven (in de repo root)
    out_path = "availability.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(availability, f, ensure_ascii=False, indent=2)

    print(f"✅ availability.json opgeslagen in {out_path}")
    print(f"🚗 Totaal {len(availability)} auto's verwerkt.")


if __name__ == "__main__":
    main()
