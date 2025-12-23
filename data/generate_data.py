import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path


def generate(seed: int = 42, days: int = 45) -> None:
    """Синтетические данные для ITSM ООО «ПК Аквалайф»."""
    random.seed(seed)
    base_dir = Path(__file__).resolve().parent
    raw_dir = base_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    start_date = datetime.today() - timedelta(days=days)
    dates = [start_date + timedelta(days=i) for i in range(days)]

    services = ["Корпоративная почта", "VPN", "ERP", "Data Platform", "Сеть офисов", "Сервис-деск"]
    categories = ["Инцидент", "Сервисный запрос", "Проблема"]
    ci = ["FW-01", "DB-PRD-01", "APP-ERP-01", "VPN-GW-02", "MAIL-CL-01", "DS-CL-01"]
    assignment_groups = ["NOC", "Application Support", "Network", "DBA", "Service Desk L1", "Service Desk L2"]
    request_types = ["Доступ", "Оборудование", "Сброс пароля", "Согласование", "BI-отчет", "Учетная запись"]
    priorities = ["P1", "P2", "P3", "P4"]
    channels = ["Портал", "Почта", "Телефон", "Мониторинг"]

    sla_targets_hours = {"P1": 4, "P2": 8, "P3": 24, "P4": 48}

    incidents = []
    requests = []

    for day in dates:
        for _ in range(random.randint(25, 60)):
            opened_at = day + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
            priority = random.choices(priorities, weights=[5, 10, 50, 35])[0]
            target_hours = sla_targets_hours[priority]
            resolved = random.random() > 0.15
            resolution_minutes = random.randint(30, target_hours * 90) if resolved else None
            resolved_at = opened_at + timedelta(minutes=resolution_minutes) if resolved else None
            sla_due = opened_at + timedelta(hours=target_hours)
            sla_breached = resolved_at is None or resolved_at > sla_due

            incidents.append(
                {
                    "incident_id": f"INC{10000 + len(incidents)}",
                    "opened_at": opened_at,
                    "resolved_at": resolved_at,
                    "priority": priority,
                    "service": random.choice(services),
                    "category": random.choice(categories),
                    "assignment_group": random.choice(assignment_groups),
                    "ci": random.choice(ci),
                    "channel": random.choice(channels),
                    "sla_due": sla_due,
                    "sla_breached": sla_breached,
                    "reopen_count": random.choices([0, 1, 2], weights=[88, 10, 2])[0],
                    "customer_sat": random.choice([5, 4, 3, 2, 1, None]),
                }
            )

        for _ in range(random.randint(15, 40)):
            opened_at = day + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
            fulfillment_hours = random.randint(2, 96)
            closed_at = opened_at + timedelta(hours=fulfillment_hours)
            requests.append(
                {
                    "request_id": f"REQ{20000 + len(requests)}",
                    "opened_at": opened_at,
                    "closed_at": closed_at,
                    "service": random.choice(services),
                    "request_type": random.choice(request_types),
                    "assignment_group": random.choice(assignment_groups),
                    "status": random.choice(["Завершен", "В работе", "Отклонен"]),
                    "fulfillment_hours": fulfillment_hours,
                }
            )

    inc_path = raw_dir / "itsm_incidents.csv"
    req_path = raw_dir / "itsm_requests.csv"

    with inc_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "incident_id",
                "opened_at",
                "resolved_at",
                "priority",
                "service",
                "category",
                "assignment_group",
                "ci",
                "channel",
                "sla_due",
                "sla_breached",
                "reopen_count",
                "customer_sat",
            ],
        )
        writer.writeheader()
        writer.writerows(incidents)

    with req_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "request_id",
                "opened_at",
                "closed_at",
                "service",
                "request_type",
                "assignment_group",
                "status",
                "fulfillment_hours",
            ],
        )
        writer.writeheader()
        writer.writerows(requests)

    print("Готово! Сырые данные ITSM:")
    print(f"  → {inc_path}")
    print(f"  → {req_path}")


if __name__ == "__main__":
    generate()
