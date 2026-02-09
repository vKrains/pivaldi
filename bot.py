import asyncio
import json
import sqlite3
from datetime import date

import pandas as pd
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN
from engine import assign_shift

# --------------------
# Загрузка официантов
# --------------------
df = pd.read_excel("waiters.xlsx")
WAITERS = {i + 1: str(n).strip() for i, n in enumerate(df["name"].tolist())}

# --------------------
# SQLite
# --------------------
conn = sqlite3.connect("history.db", check_same_thread=False)
conn.execute("""
CREATE TABLE IF NOT EXISTS history (
    date TEXT,
    waiter_id INTEGER,
    zone TEXT,
    position INTEGER
)
""")
conn.commit()

def load_history():
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT date, waiter_id, zone, position FROM history ORDER BY date ASC"
    ).fetchall()
    return [{"date": r[0], "waiter_id": r[1], "zone": r[2], "position": r[3]} for r in rows]

def save_final(date_str, shift_type, result):
    for wid, a in result.items():
        conn.execute(
            "INSERT INTO history VALUES (?, ?, ?, ?)",
            (date_str, wid, a["zone"], a["position"])
        )
    conn.commit()

    rows = []
    for wid, a in result.items():
        rows.append({
            "date": date_str,
            "shift_type": shift_type,
            "waiter_id": wid,
            "waiter_name": WAITERS[wid],
            "zone": a["zone"],
            "position": a["position"],
        })
    pd.DataFrame(rows).to_excel("current_shift.xlsx", index=False)

# --------------------
# FSM
# --------------------
class ShiftFSM(StatesGroup):
    select_shift_type = State()
    select_present = State()
    confirm = State()

# --------------------
# Бот
# --------------------
bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# --------------------
# /start
# --------------------
@dp.message(F.text == "/start")
async def start(msg: Message, state: FSMContext):
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Будни", callback_data="weekday"),
            InlineKeyboardButton(text="Пт/Сб/Вс", callback_data="weekend"),
        ]
    ])
    await msg.answer("Выбери тип смены:", reply_markup=kb)
    await state.set_state(ShiftFSM.select_shift_type)

# --------------------
# Тип смены
# --------------------
@dp.callback_query(ShiftFSM.select_shift_type)
async def shift_type(cb: CallbackQuery, state: FSMContext):
    await state.update_data(
        shift_type=cb.data,
        present=set(),
        date=date.today().isoformat()
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text=f"☐ {name}",
                callback_data=f"w_{wid}"
            )]
            for wid, name in WAITERS.items()
        ] + [[InlineKeyboardButton(text="Готово", callback_data="done")]]
    )

    await cb.message.edit_text("Кто вышел?", reply_markup=kb)
    await state.set_state(ShiftFSM.select_present)

# --------------------
# Выбор вышедших
# --------------------
@dp.callback_query(ShiftFSM.select_present, F.data.startswith("w_"))
async def toggle_waiter(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    present = set(data["present"])
    wid = int(cb.data.split("_")[1])

    if wid in present:
        present.remove(wid)
    else:
        present.add(wid)

    await state.update_data(present=present)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text=("☑ " if wid in present else "☐ ") + name,
                callback_data=f"w_{wid}"
            )]
            for wid, name in WAITERS.items()
        ] + [[InlineKeyboardButton(text="Готово", callback_data="done")]]
    )

    await cb.message.edit_reply_markup(reply_markup=kb)
    await cb.answer()

# --------------------
# Готово → распределение
# --------------------
@dp.callback_query(ShiftFSM.select_present, F.data == "done")
async def do_assign(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()

    result = assign_shift(
        present=list(data["present"]),
        requests={},
        history=load_history(),
        shift_type=data["shift_type"]
    )

    text = f"📅 {data['date']}\n\n"
    for wid, a in result.items():
        text += f"{WAITERS[wid]} → {a['zone']}"
        if a["position"]:
            text += f" (П{a['position']})"
        text += "\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Сохранить", callback_data="save")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])

    await state.update_data(result=result)
    await cb.message.edit_text(text, reply_markup=kb)
    await state.set_state(ShiftFSM.confirm)

# --------------------
# Сохранение
# --------------------
@dp.callback_query(ShiftFSM.confirm, F.data == "save")
async def save(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    save_final(data["date"], data["shift_type"], data["result"])
    await cb.message.edit_text("✅ Смена сохранена")
    await state.clear()

@dp.callback_query(ShiftFSM.confirm, F.data == "cancel")
async def cancel(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_text("❌ Отменено")
    await state.clear()

# --------------------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
