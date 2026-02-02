import os
import asyncio
import re
import logging
import sys
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from aiohttp import web
import asyncpg
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from config import API_ID, API_HASH, BOT_TOKEN, SOURCE_CHANNEL_ID, BILAN_CHANNEL_ID, PORT, BILAN_INTERVAL_MINUTES, DATABASE_URL, JOUR_START, JOUR_END

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

client = TelegramClient(StringSession(''), API_ID, API_HASH)
bilan_interval = BILAN_INTERVAL_MINUTES
current_jour_id = None

class PostgresDB:
    def __init__(self, database_url):
        self.database_url = database_url
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=10)
        logger.info("PostgreSQL connecte")
        await self.create_tables()

    async def create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute("CREATE TABLE IF NOT EXISTS games (id SERIAL PRIMARY KEY, jour_id VARCHAR(20) NOT NULL, game_number INTEGER NOT NULL, suit VARCHAR(10) NOT NULL, category VARCHAR(10) NOT NULL, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, raw_line TEXT, UNIQUE(jour_id, game_number))")
            await conn.execute("CREATE TABLE IF NOT EXISTS jours (id SERIAL PRIMARY KEY, jour_id VARCHAR(20) UNIQUE NOT NULL, date_str VARCHAR(20) NOT NULL, start_num INTEGER DEFAULT 6, end_num INTEGER DEFAULT 1436, is_complete BOOLEAN DEFAULT FALSE, total_games INTEGER DEFAULT 0, count_0 INTEGER DEFAULT 0, count_1 INTEGER DEFAULT 0, count_2 INTEGER DEFAULT 0, count_3 INTEGER DEFAULT 0, count_loss INTEGER DEFAULT 0)")
            logger.info("Tables crees")

    async def get_or_create_jour(self, game_number):
        global current_jour_id
        today = datetime.now()
        if JOUR_START <= game_number <= JOUR_END:
            jour_id = today.strftime("%Y-%m-%d")
        else:
            if game_number < JOUR_START:
                yesterday = today - timedelta(days=1)
                jour_id = yesterday.strftime("%Y-%m-%d")
            else:
                tomorrow = today + timedelta(days=1)
                jour_id = tomorrow.strftime("%Y-%m-%d")
        current_jour_id = jour_id
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval("SELECT 1 FROM jours WHERE jour_id = $1", jour_id)
            if not exists:
                await conn.execute("INSERT INTO jours (jour_id, date_str, start_num, end_num) VALUES ($1, $2, $3, $4)", jour_id, jour_id, JOUR_START, JOUR_END)
        return jour_id

    async def add_game(self, game_number, suit, category, raw_line):
        jour_id = await self.get_or_create_jour(game_number)
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO games (jour_id, game_number, suit, category, raw_line) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (jour_id, game_number) DO UPDATE SET suit = EXCLUDED.suit, category = EXCLUDED.category", jour_id, game_number, suit, category, raw_line)
            cat_map = {'cat0': 'count_0', 'cat1': 'count_1', 'cat2': 'count_2', 'cat3': 'count_3', 'loss': 'count_loss'}
            cat_key = 'loss' if category == 'loss' else f"cat{category}"
            cat_col = cat_map.get(cat_key)
            if cat_col:
                await conn.execute(f"UPDATE jours SET total_games = total_games + 1, {cat_col} = {cat_col} + 1 WHERE jour_id = $1", jour_id)
            if game_number == JOUR_END:
                await conn.execute("UPDATE jours SET is_complete = TRUE WHERE jour_id = $1", jour_id)

    async def get_jour_stats(self, jour_id=None):
        if jour_id is None:
            jour_id = current_jour_id
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM jours WHERE jour_id = $1", jour_id)

    async def get_numbers_by_category_and_jour(self, category, jour_id=None):
        if jour_id is None:
            jour_id = current_jour_id
        async with self.pool.acquire() as conn:
            return await conn.fetch("SELECT game_number FROM games WHERE jour_id = $1 AND category = $2 ORDER BY game_number", jour_id, category)

    async def get_numbers_by_category_all(self, category):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT DISTINCT game_number FROM games WHERE category = $1 ORDER BY game_number", category)
            return [r['game_number'] for r in rows]

db = PostgresDB(DATABASE_URL)

def parse_game_message(message_text):
    games = []
    lines = message_text.strip().split('\n')
    for line in lines:
        line = line.strip()
        if not line or '—' not in line:
            continue
        number_match = re.match(r'(\d+)\s*—', line)
        if not number_match:
            continue
        game_number = int(number_match.group(1))
        suit_match = re.search(r'игрок\s*([♠♥♦♣❤️♠️♥️♦️♣️])', line)
        if not suit_match:
            continue
        suit = suit_match.group(1)
        suit = suit.replace('❤️', '♥️').replace('❤', '♥️').replace('♥', '♥️').replace('♠', '♠️').replace('♦', '♦️').replace('♣', '♣️')
        category = None
        if '0' in line and '✅' in line:
            category = '0'
        elif '1' in line and '✅' in line:
            category = '1'
        elif '2' in line and '✅' in line:
            category = '2'
        elif '3' in line and '✅' in line:
            category = '3'
        elif '❌' in line:
            category = 'loss'
        if category:
            games.append({'number': game_number, 'suit': suit, 'category': category})
    return games

async def send_bilan():
    try:
        stats = await db.get_jour_stats()
        if not stats:
            return
        msg = f"BILAN\\nJournee: {stats['jour_id']}\\nTotal: {stats['total_games']}\\nCat0: {stats['count_0']}\\nCat1: {stats['count_1']}\\nCat2: {stats['count_2']}\\nCat3: {stats['count_3']}\\nLoss: {stats['count_loss']}"
        await client.send_message(BILAN_CHANNEL_ID, msg)
        logger.info("Bilan envoye")
    except Exception as e:
        logger.error(f"Erreur bilan: {e}")

async def bilan_scheduler():
    while True:
        await asyncio.sleep(bilan_interval * 60)
        await send_bilan()

async def process_edited_message(message_text, chat_id):
    if chat_id != SOURCE_CHANNEL_ID:
        return
    games = parse_game_message(message_text)
    for game in games:
        await db.add_game(game['number'], game['suit'], game['category'], '')
        logger.info(f"Jeu #{game['number']} enregistre")

async def handle_edited_message(event):
    try:
        chat = await event.get_chat()
        chat_id = chat.id
        if hasattr(chat, 'broadcast') and chat.broadcast:
            if not str(chat_id).startswith('-100'):
                chat_id = int(f"-100{abs(chat_id)}")
        if chat_id == SOURCE_CHANNEL_ID:
            await process_edited_message(event.message.message, chat_id)
    except Exception as e:
        logger.error(f"Erreur: {e}")

@client.on(events.NewMessage(pattern='/start'))
async def cmd_start(event):
    if event.is_group or event.is_channel:
        return
    text = "Bot Baccarat\\n/info - Bilan\\n/set_interval <min>\\n/force_bilan\\n/lis0 today/all\\n/lis1 today/all\\n/lis2 today/all\\n/lis3 today/all\\n/lis4 today/all\\n/inter - Export\\n/compare_all"
    await event.respond(text)

@client.on(events.NewMessage(pattern=r'/set_interval\s+(\d+)'))
async def cmd_set_interval(event):
    if event.is_group or event.is_channel:
        return
    global bilan_interval
    try:
        new_interval = int(event.pattern_match.group(1))
        if new_interval < 1:
            await event.respond("Minimum 1 minute")
            return
        bilan_interval = new_interval
        await event.respond(f"Intervalle: {bilan_interval} minutes")
    except:
        await event.respond("Erreur")

@client.on(events.NewMessage(pattern='/force_bilan'))
async def cmd_force_bilan(event):
    if event.is_group or event.is_channel:
        return
    await send_bilan()
    await event.respond("Bilan envoye!")

@client.on(events.NewMessage(pattern='/info'))
async def cmd_info(event):
    if event.is_group or event.is_channel:
        return
    stats = await db.get_jour_stats()
    if not stats:
        await event.respond("Aucune donnee")
        return
    text = f"Journee: {stats['jour_id']}\\nTotal: {stats['total_games']}\\nCat0: {stats['count_0']}\\nCat1: {stats['count_1']}\\nCat2: {stats['count_2']}\\nCat3: {stats['count_3']}\\nLoss: {stats['count_loss']}"
    await event.respond(text)

@client.on(events.NewMessage(pattern=r'/lis([0-4])$'))
async def cmd_lis_help(event):
    if event.is_group or event.is_channel:
        return
    num = event.pattern_match.group(1)
    text = f"/lis{num} today - Aujourd'hui\\n/lis{num} all - Toute la base"
    await event.respond(text)

@client.on(events.NewMessage(pattern=r'/lis([0-4])\s+(today|all)'))
async def cmd_lis_detail(event):
    if event.is_group or event.is_channel:
        return
    num = event.pattern_match.group(1)
    option = event.pattern_match.group(2)
    cat_map = {'0': '0', '1': '1', '2': '2', '3': '3', '4': 'loss'}
    cat = cat_map[num]
    try:
        if option == "today":
            rows = await db.get_numbers_by_category_and_jour(cat)
            numbers = [r['game_number'] for r in rows]
            title = "AUJOURD'HUI"
        else:
            numbers = await db.get_numbers_by_category_all(cat)
            title = "TOUTE LA BASE"
        if not numbers:
            await event.respond(f"Aucun numero categorie {cat}")
            return
        text = f"Categorie {cat} - {title}\\nTotal: {len(numbers)}\\n\\n"
        for i in range(0, len(numbers), 20):
            group = numbers[i:i+20]
            text += ", ".join([str(n) for n in group]) + "\\n"
        await event.respond(text)
    except Exception as e:
        await event.respond(f"Erreur: {e}")

@client.on(events.NewMessage(pattern='/inter'))
async def cmd_inter(event):
    if event.is_group or event.is_channel:
        return
    await event.respond("Creation Excel...")
    try:
        wb = Workbook()
        wb.remove(wb.active)
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        cats = [('0', 'CAT_0'), ('1', 'CAT_1'), ('2', 'CAT_2'), ('3', 'CAT_3'), ('loss', 'CAT_LOSS')]
        for cat, sheet_name in cats:
            ws = wb.create_sheet(title=sheet_name)
            ws['A1'] = "NUMERO"
            ws['B1'] = "COSTUME"
            for col in ['A', 'B']:
                cell = ws[f'{col}1']
                cell.font = header_font
                cell.fill = header_fill
            rows = await db.get_numbers_by_category_and_jour(cat)
            for idx, row in enumerate(rows, 2):
                ws.cell(row=idx, column=1, value=row['game_number'])
            ws.column_dimensions['A'].width = 12
            ws.column_dimensions['B'].width = 12
        filename = f"export_{current_jour_id}.xlsx"
        wb.save(filename)
        await client.send_file(event.chat_id, filename, caption="Export Excel")
        os.remove(filename)
    except Exception as e:
        await event.respond(f"Erreur: {e}")

@client.on(events.NewMessage(pattern='/compare_all'))
async def cmd_compare_all(event):
    if event.is_group or event.is_channel:
        return
    await event.respond("Comparaison...")
    try:
        async with db.pool.acquire() as conn:
            jours = await conn.fetch("SELECT jour_id FROM jours ORDER BY jour_id")
            if len(jours) < 2:
                await event.respond("Pas assez de journees")
                return
            wb = Workbook()
            wb.remove(wb.active)
            ws = wb.create_sheet("FREQUENCE")
            ws['A1'] = "NUMERO"
            ws['B1'] = "NB_JOURS"
            rows = await conn.fetch("SELECT game_number, COUNT(DISTINCT jour_id) as nb FROM games GROUP BY game_number ORDER BY nb DESC")
            for idx, row in enumerate(rows, 2):
                ws.cell(row=idx, column=1, value=row['game_number'])
                ws.cell(row=idx, column=2, value=row['nb'])
            filename = f"comparaison_{datetime.now().strftime('%Y%m%d')}.xlsx"
            wb.save(filename)
            await client.send_file(event.chat_id, filename, caption="Comparaison")
            os.remove(filename)
    except Exception as e:
        await event.respond(f"Erreur: {e}")

async def health_check(request):
    return web.Response(text="OK", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Serveur web port {PORT}")

client.add_event_handler(handle_edited_message, events.MessageEdited())

async def main():
    await db.connect()
    await start_web_server()
    await client.start(bot_token=BOT_TOKEN)
    logger.info("Bot demarre")
    asyncio.create_task(bilan_scheduler())
    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Arret")
    except Exception as e:
        logger.error(f"Erreur: {e}")
