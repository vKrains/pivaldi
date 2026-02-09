from collections import defaultdict

MAIN_POSITIONS = list(range(1, 19))
WEEKEND_ZONES = {"Small": 4, "Far": 3, "Veranda": 9}


def _main_counts_all(history):
    """
    Возвращает:
      - counts[waiter_id][position] = сколько раз стоял на позиции (за всю историю)
      - by_waiter[waiter_id] = список позиций в хронологическом порядке (старые -> новые)
    """
    counts = defaultdict(lambda: defaultdict(int))
    by_waiter = defaultdict(list)

    # history ожидается уже отсортированным по date ASC
    for h in history:
        if h["zone"] == "Main" and h["position"] is not None:
            wid = int(h["waiter_id"])
            pos = int(h["position"])
            counts[wid][pos] += 1
            by_waiter[wid].append(pos)

    return counts, by_waiter


def _visited_in_current_cycle(pos_list):
    """
    pos_list: позиции Main в хронологическом порядке (старые -> новые)
    Возвращает множество позиций, посещённых в ТЕКУЩЕМ цикле (после последнего полного круга 18).
    """
    seen = set()
    for pos in reversed(pos_list):
        seen.add(pos)
        if len(seen) == 18:
            # полный круг закрыт -> текущий цикл начинается после этого момента
            return set()
    return seen


def assign_shift(present, requests, history, shift_type):
    """
    present: list[int] - кто вышел
    requests: dict[int] = {"zone": "...", "position": int|None} - locked назначения
    history: list[dict] - прошлые финальные назначения (date ASC)
    shift_type: "weekday" | "weekend"
    """
    present = [int(x) for x in present]
    present_set = set(present)

    assignments = {}
    locked = set()

    free_main = set(MAIN_POSITIONS)
    free_zones = WEEKEND_ZONES.copy()

    # main stats
    main_counts, main_by_waiter = _main_counts_all(history)

    # weekend stats (по зонам, без позиций)
    weekend_total = defaultdict(int)
    weekend_zone = defaultdict(lambda: defaultdict(int))
    for h in history:
        z = h["zone"]
        if z in WEEKEND_ZONES:
            wid = int(h["waiter_id"])
            weekend_total[wid] += 1
            weekend_zone[wid][z] += 1

    # ---- ЭТАП 1: locked (приоритетные)
    for wid, req in requests.items():
        wid = int(wid)
        if wid not in present_set:
            continue

        zone = req.get("zone")
        pos = req.get("position", None)

        if zone == "Main":
            if pos is None:
                raise ValueError(f"Для Main-запроса waiter_id={wid} нужна позиция")
            pos = int(pos)
            if pos not in free_main:
                raise ValueError(f"Позиция {pos} уже занята (конфликт locked)")
            assignments[wid] = {"zone": "Main", "position": pos}
            free_main.remove(pos)
        else:
            if zone not in WEEKEND_ZONES:
                raise ValueError(f"Неизвестная зона: {zone}")
            if free_zones.get(zone, 0) <= 0:
                raise ValueError(f"В зоне {zone} нет свободных мест (конфликт locked)")
            assignments[wid] = {"zone": zone, "position": None}
            free_zones[zone] -= 1

        locked.add(wid)

    remaining = [w for w in present if w not in locked]

    # ---- ЭТАП 2: MAIN (18 мест с позициями)
    already_main = sum(1 for a in assignments.values() if a["zone"] == "Main")
    need_main = 18 - already_main
    if need_main < 0:
        raise ValueError("Locked-запросами занято больше 18 мест в Main")

    def main_priority(wid):
        visited = _visited_in_current_cycle(main_by_waiter[wid])
        unvisited_cnt = 18 - len(visited)
        total_main = sum(main_counts[wid].values())
        # "по линейке": кто ближе к закрытию круга + кто чаще уже ходил (вторично)
        return (unvisited_cnt, total_main, wid)

    remaining.sort(key=main_priority)

    for wid in remaining[:]:
        if need_main <= 0:
            break

        visited = _visited_in_current_cycle(main_by_waiter[wid])
        candidates = sorted(list((set(MAIN_POSITIONS) - visited) & free_main))
        if candidates:
            pos = candidates[0]
        else:
            # допуск: минимальная частота по всей истории
            pos = min(free_main, key=lambda p: main_counts[wid][p])

        assignments[wid] = {"zone": "Main", "position": pos}
        free_main.remove(pos)
        remaining.remove(wid)
        need_main -= 1

    # ---- ЭТАП 3: WEEKEND zones (если weekend)
    if shift_type == "weekend":
        # осталось должно быть ровно 16 (если пришли 34)
        remaining.sort(key=lambda w: (weekend_total[w], w))

        for zone, count in WEEKEND_ZONES.items():
            for _ in range(count):
                if not remaining:
                    raise ValueError("Не хватает людей для распределения weekend-зон")
                wid = min(remaining, key=lambda x: (weekend_zone[x][zone], weekend_total[x], x))
                assignments[wid] = {"zone": zone, "position": None}
                remaining.remove(wid)
    else:
        # weekday: после main никто не должен остаться (если пришло ровно 18)
        pass

    return assignments