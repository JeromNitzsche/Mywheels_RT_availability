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
SHEET_RANGE = "Data!A:E"

BLOCK_MINUTES = 15
WINDOW_HOURS = 12
FREE_REQUIRED_MINUTES = 30
SA_PATH = "sa.json"

SESSION = requests.Session()   # <-- 🔥 SNELHEID BOOST


# ==========================
# HELPERS
# ==========================

def clean_license(lic: str) -> str:
    if lic is None:
        return ""
    return "".join(ch for ch in lic.replace(" ", "").replace("-", "").upper())


def get_credentials():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    return service_account.Credentials.from_service_account_file(
        SA_PATH, scopes=scopes
    )


def load_cars_from_sheet():
    print("📄 Sheet uitlezen...")
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    resp = sheet.values().get(spreadsheetId=SHEET_ID, range=SHEET_RANGE).execute()
    values = resp.get("values", [])

    if not values:
        print("⚠️ Geen data gevonden in sheet.")
        return []

    rows = values[1:]  # header skippen

    cars = []
    for row in rows:
        row = row + [""] * (5 - len(row))
        license_plate, city, _, franchise, rid = row

        if not rid:
            continue

        try:
            resource_id = int(rid)
        except ValueError:
            continue

        cars.append({
            "resource_id": resource_id,
            "license_clean": clean_license(license_plate),
        })

    print(f"🚗 {len(cars)} auto's geladen uit sheet.")
    return cars


def parse_slot_datetime(s: str) -> datetime:
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


# ==========================
# API CALL
# ==========================

DEBUG_LOG_DONE = False


def fetch_calendar_availability(resource_id, start_dt, end_dt, retries=3):
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

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    for attempt in range(retries):
        try:
            r = SESSION.post(API_URL, json=payload, headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()

            if not DEBUG_LOG_DONE:
                print("\n================ DEBUG (eenmalig) ================\n")
                print(json.dumps(data, indent=2))
                print("\n==================================================\n")
                DEBUG_LOG_DONE = True

            env = data[0] if isinstance(data, list) else data
            result = env.get("result", [])

            if isinstance(result, list):
                return result
            if isinstance(result, dict):
                return result.get("availability") or result.get("slots") or []

            return []
        except:
            time.sleep(0.3)

    return []


# ==========================
# AVAILABILITY VOOR AUTO
# ==========================

def build_availability_for_car(resource_id, start_dt, end_dt):
    slots = fetch_calendar_availability(resource_id, start_dt, end_dt)

    conflict_blocks = []

    for slot in slots:
        if not isinstance(slot, dict):
            continue

        start_str = slot.get("startDate")
        end_str = slot.get("endDate")
        refuel = slot.get("refuelTime", 0)

        if not start_str or not end_str:
            continue

        try:
            s = parse_slot_datetime(start_str)
            e = parse_slot_datetime(end_str)
        except:
            continue

        if isinstance(refuel, (int, float)):
            e += timedelta(minutes=refuel)

        if e <= start_dt or s >= end_dt:
            continue

        s = max(s, start_dt)
        e = min(e, end_dt)

        conflict_blocks.append((s, e))

    conflict_blocks = merge_blocks(conflict_blocks)

    free_period_found = False
    current = start_dt

    for s, e in conflict_blocks:
        if s > current and (s - current) >= timedelta(minutes=FREE_REQUIRED_MINUTES):
            free_period_found = True
            break
        current = max(current, e)

    if not free_period_found and (end_dt - current) >= timedelta(minutes=FREE_REQUIRED_MINUTES):
        free_period_found = True

    no_availability_all_day = not free_period_found

    if not no_availability_all_day:
        corrected = []
        for s, e in conflict_blocks:
            #  ❌ DEZE REGELS WEG:
            # if e < end_dt:
            #     e -= timedelta(minutes=BLOCK_MINUTES)
            # if s > start_dt:
            #     s += timedelta(minutes=BLOCK_MINUTES)
            if e > s:
                corrected.append((s, e))
        conflict_blocks = merge_blocks(corrected)

    return {
        "no_availability_all_day": no_availability_all_day,
        "conflict_tijden": format_blocks(conflict_blocks) if conflict_blocks else ""
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
        print("⚠️ Geen auto's om te verwerken.")
        return

    availability = {}

    for idx, car in enumerate(cars, start=1):
        rid = car["resource_id"]
        license_clean = car["license_clean"]

        print(f"🔎 [{idx}/{len(cars)}] {rid} ({license_clean})")

        availability[license_clean] = build_availability_for_car(
            rid, start_dt, end_dt
        )

        time.sleep(0.05)   # <-- 🔥 SNELLER EN VEILIG

    with open("availability.json", "w", encoding="utf-8") as f:
        json.dump(availability, f, ensure_ascii=False, indent=2)

    print("✅ availability.json opgeslagen")
    print(f"🚗 {len(availability)} auto's verwerkt.")


if __name__ == "__main__":
    main()
