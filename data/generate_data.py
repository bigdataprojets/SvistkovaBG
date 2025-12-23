import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _generate_locations() -> Tuple[List[Dict], List[Dict]]:
    zones = [
        ("Холодильная", True),
        ("Сухое хранение", True),
        ("Упаковочная", True),
        ("Погрузочная", True),
        ("Контроль качества", True),
        ("Резерв", False),
    ]
    racks = ["A", "B", "C", "D"]
    shelves = ["S1", "S2", "S3"]

    locations: List[Dict] = []
    for zone_name, is_active in zones:
        racks_for_zone = random.sample(racks, k=random.randint(2, len(racks)))
        for rack in racks_for_zone:
            for shelf in random.sample(shelves, k=random.randint(2, len(shelves))):
                locations.append(
                    {
                        "location_id": len(locations) + 1,
                        "zone": zone_name,
                        "rack": f"R{rack}",
                        "shelf": shelf,
                        "is_active": is_active,
                    }
                )
    return locations, [loc for loc in locations if loc["is_active"]]


def _generate_employees() -> List[Dict]:
    names = [
        "Софья «Соня» Кондратьева",
        "Иван Серов",
        "Наталья Лебедева",
        "Марина Жукова",
        "Дмитрий Орлов",
        "Сергей Кузьмин",
        "Екатерина Шмыкова",
        "Павел Синицын",
        "Елена Бабкина",
        "Роман Белоусов",
        "Анна Кондратьева",
        "Максим Горбунов",
    ]
    roles = ["Инженер по оборудованию", "Мастер смены", "Инженер-наладчик", "Кладовщик"]
    shifts = ["Дневная", "Ночная"]
    departments = [
        "Склад электрооборудования",
        "Склад сырья",
        "Холодильная зона",
        "Участок упаковки",
    ]
    employees = []
    for idx, name in enumerate(names, start=1):
        employees.append(
            {
                "responsible_person_id": idx,
                "full_name": name,
                "role": random.choice(roles),
                "department": random.choice(departments),
                "contact": f"+7-90{random.randint(0, 9)}-{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(10, 99)}",
                "shift": random.choice(shifts),
            }
        )
    return employees


def _generate_equipment(
    equipment_count: int,
    active_locations: List[Dict],
    employees: List[Dict],
) -> List[Dict]:
    categories = {
        "Холодильник": ["Polair PR", "Бирюса F", "Atesy CF"],
        "Конвейер": ["Interroll SL2", "Krones Flow", "Festo Move"],
        "Погрузчик": ["Still RX", "Jungheinrich EFG", "Linde E"],
        "Датчик": ["Siemens Sitrans", "Omron E3", "Schneider XS"],
        "Упаковочная машина": ["Ishida Inspira", "Ulma FM", "Bosch Pack"],
        "Шкаф управления": ["Schneider Prisma", "Siemens SIVACON", "Weidmuller PRO"],
    }
    statuses = ["рабочее", "на ремонте", "списано", "резерв"]

    today = datetime.today().date()
    equipment: List[Dict] = []
    for idx in range(1, equipment_count + 1):
        category = random.choice(list(categories.keys()))
        model = random.choice(categories[category])
        location = random.choice(active_locations)
        resp = random.choice(employees)

        purchase_date = today - timedelta(days=random.randint(200, 2200))
        warranty_years = random.randint(2, 5)
        warranty_end_date = purchase_date + timedelta(days=365 * warranty_years)
        last_maintenance_date = purchase_date + timedelta(days=random.randint(90, max(120, (today - purchase_date).days)))
        last_maintenance_date = min(last_maintenance_date, today - timedelta(days=random.randint(15, 90)))
        next_maintenance_date = last_maintenance_date + timedelta(days=random.randint(80, 220))

        equipment.append(
            {
                "equipment_id": f"EQ{idx:04d}",
                "inventory_number": f"INV-24-{idx:04d}",
                "name": f"{category} #{idx:03d}",
                "category": category,
                "model": model,
                "serial_number": f"{random.choice(['SN', 'EL', 'BB'])}-{random.randint(100000, 999999)}",
                "status": random.choices(statuses, weights=[70, 12, 6, 12])[0],
                "location_id": location["location_id"],
                "purchase_date": purchase_date,
                "warranty_end_date": warranty_end_date,
                "last_maintenance_date": last_maintenance_date,
                "next_maintenance_date": next_maintenance_date,
                "responsible_person_id": resp["responsible_person_id"],
                "created_at": datetime.combine(purchase_date, datetime.min.time()) + timedelta(hours=random.randint(6, 14)),
            }
        )
    return equipment


def _generate_inventory_sessions(
    active_locations: List[Dict],
    employees: List[Dict],
    equipment: List[Dict],
    session_count: int,
) -> Tuple[List[Dict], List[Dict]]:
    sessions: List[Dict] = []
    results: List[Dict] = []
    today = datetime.today().date()

    session_dates = sorted({today - timedelta(days=random.randint(5, 120)) for _ in range(session_count)})
    for idx, session_date in enumerate(session_dates, start=1):
        location = random.choice(active_locations)
        resp = random.choice(employees)
        equipment_in_location = [eq for eq in equipment if eq["location_id"] == location["location_id"]]
        if not equipment_in_location:
            equipment_in_location = random.sample(equipment, k=min(5, len(equipment)))
        inspected = random.sample(
            equipment_in_location,
            k=max(1, min(len(equipment_in_location), int(len(equipment_in_location) * random.uniform(0.4, 0.9)))),
        )
        inventory_id = f"INV-{session_date.strftime('%m%d')}-{idx:02d}"
        status = random.choice(["в процессе", "завершена"])

        discrepancy_count = 0
        for result_idx, eq in enumerate(inspected, start=1):
            expected_presence = 1
            actual_presence = random.choices([0, 1, 2], weights=[5, 88, 7])[0]
            moved = random.random() < 0.08
            recorded_location = random.choice(active_locations)["location_id"] if moved else eq["location_id"]
            discrepancy_flag = actual_presence != expected_presence or moved
            discrepancy_type = "нет" if not discrepancy_flag else random.choice(["отсутствует", "перемещено", "лишнее", "списание"])
            condition = random.choice(["исправно", "повреждено", "требуется ТО", "к списанию"])
            comment = ""
            if discrepancy_flag:
                discrepancy_count += 1
                comment = "Требуется пересчёт" if discrepancy_type in {"лишнее", "перемещено"} else "Рассмотреть списание"

            results.append(
                {
                    "result_id": f"RES{idx:02d}-{result_idx:03d}",
                    "inventory_id": inventory_id,
                    "equipment_id": eq["equipment_id"],
                    "location_id": recorded_location,
                    "expected_presence": expected_presence,
                    "actual_presence": actual_presence,
                    "condition": condition,
                    "discrepancy_flag": discrepancy_flag,
                    "discrepancy_type": discrepancy_type,
                    "comment": comment,
                }
            )

        sessions.append(
            {
                "inventory_id": inventory_id,
                "session_date": session_date,
                "location_id": location["location_id"],
                "responsible_person_id": resp["responsible_person_id"],
                "status": status,
                "discrepancy_count": discrepancy_count,
                "notes": "Инвентаризация электрооборудования на складе",
            }
        )

    return sessions, results


def _generate_maintenance_logs(
    equipment: List[Dict],
    employees: List[Dict],
) -> List[Dict]:
    maintenance_logs: List[Dict] = []
    today = datetime.today().date()
    maintenance_types = ["ТО-1", "ТО-2", "Замена узла", "Диагностика"]

    log_idx = 1
    for eq in equipment:
        log_count = random.randint(1, 3)
        for _ in range(log_count):
            maintenance_date = today - timedelta(days=random.randint(15, 600))
            status = random.choices(["выполнено", "запланировано", "просрочено"], weights=[70, 20, 10])[0]
            maintenance_logs.append(
                {
                    "maintenance_id": f"MNT{log_idx:05d}",
                    "equipment_id": eq["equipment_id"],
                    "maintenance_date": maintenance_date,
                    "maintenance_type": random.choice(maintenance_types),
                    "duration_hours": round(random.uniform(1.0, 6.5), 1),
                    "status": status,
                    "responsible_person_id": random.choice(employees)["responsible_person_id"],
                    "cost": round(random.uniform(1500, 25000), 2),
                }
            )
            log_idx += 1
    return maintenance_logs


def generate(seed: int = 42, equipment_count: int = 110, session_count: int = 8) -> None:
    """
    Синтетические данные для ВКР по теме «Инвентаризация электрооборудования на складе»
    на примере ОАО «Кондитерский концерн Бабаевский».
    """
    random.seed(seed)
    base_dir = Path(__file__).resolve().parent
    raw_dir = base_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    locations, active_locations = _generate_locations()
    employees = _generate_employees()
    equipment = _generate_equipment(equipment_count, active_locations, employees)
    sessions, results = _generate_inventory_sessions(active_locations, employees, equipment, session_count)
    maintenance_logs = _generate_maintenance_logs(equipment, employees)

    _write_csv(
        raw_dir / "storage_locations.csv",
        ["location_id", "zone", "rack", "shelf", "is_active"],
        locations,
    )
    _write_csv(
        raw_dir / "employees.csv",
        ["responsible_person_id", "full_name", "role", "department", "contact", "shift"],
        employees,
    )
    _write_csv(
        raw_dir / "equipment.csv",
        [
            "equipment_id",
            "inventory_number",
            "name",
            "category",
            "model",
            "serial_number",
            "status",
            "location_id",
            "purchase_date",
            "warranty_end_date",
            "last_maintenance_date",
            "next_maintenance_date",
            "responsible_person_id",
            "created_at",
        ],
        equipment,
    )
    _write_csv(
        raw_dir / "inventory_sessions.csv",
        [
            "inventory_id",
            "session_date",
            "location_id",
            "responsible_person_id",
            "status",
            "discrepancy_count",
            "notes",
        ],
        sessions,
    )
    _write_csv(
        raw_dir / "inventory_results.csv",
        [
            "result_id",
            "inventory_id",
            "equipment_id",
            "location_id",
            "expected_presence",
            "actual_presence",
            "condition",
            "discrepancy_flag",
            "discrepancy_type",
            "comment",
        ],
        results,
    )
    _write_csv(
        raw_dir / "maintenance_logs.csv",
        [
            "maintenance_id",
            "equipment_id",
            "maintenance_date",
            "maintenance_type",
            "duration_hours",
            "status",
            "responsible_person_id",
            "cost",
        ],
        maintenance_logs,
    )

    print("Готово! Сырые данные для инвентаризации электрооборудования сохранены:")
    for file_name in [
        "storage_locations.csv",
        "employees.csv",
        "equipment.csv",
        "inventory_sessions.csv",
        "inventory_results.csv",
        "maintenance_logs.csv",
    ]:
        print(f"  → {raw_dir / file_name}")


if __name__ == "__main__":
    generate()
