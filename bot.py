"""Crypto tracker Discord bot: discord.py 2.x + Kraken public API (free) + Claude analysis."""
import asyncio
import io
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import aiohttp
import aiosqlite
import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv

try:
    import anthropic
except ImportError:  # AI is optional; falls back to RSI rules
    anthropic = None

from whitelist import ALLOWED_GUILD_IDS

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("cryptobot")

TOKEN = os.environ["DISCORD_TOKEN"]
POLL_SECONDS = max(5, int(os.getenv("POLL_SECONDS", "30")))
MIN_CHANGE_PCT = float(os.getenv("MIN_CHANGE_PCT", "0.1"))  # 0 = post on ANY change
AI_CACHE_SECONDS = int(os.getenv("AI_CACHE_SECONDS", "600"))
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5-5")
DB_PATH = os.getenv("DB_PATH", "tracker.db")

POPULAR_SYMBOLS = ["BTC", "ETH", "SOL", "LTC", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT"]
NAMES = {
    "BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana", "LTC": "Litecoin", "XRP": "XRP",
    "DOGE": "Dogecoin", "ADA": "Cardano", "AVAX": "Avalanche", "LINK": "Chainlink",
    "DOT": "Polkadot", "BCH": "Bitcoin Cash", "XLM": "Stellar", "ATOM": "Cosmos",
    "UNI": "Uniswap", "TRX": "TRON", "SHIB": "Shiba Inu", "PEPE": "Pepe", "MATIC": "Polygon",
}
FIAT = {"USD", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY"}

# ----------------------------------------------------------------- helpers
def fmt_price(p: float) -> str:
    if p >= 1:
        return f"${p:,.2f}"
    return "$" + f"{p:.8f}".rstrip("0").rstrip(".")


def disp(x) -> str:
    """'Bitcoin (BTC)', or just 'ABC' when we have no friendly name."""
    return x["symbol"] if x["name"] == x["symbol"] else f"{x['name']} ({x['symbol']})"


def sma(vals, n):
    chunk = vals[-n:]
    return sum(chunk) / len(chunk)


def rsi(vals, n=14):
    if len(vals) < n + 1:
        return None
    gains = losses = 0.0
    for a, b in zip(vals[-n - 1:-1], vals[-n:]):
        d = b - a
        gains += max(d, 0)
        losses += max(-d, 0)
    if losses == 0:
        return 100.0
    return 100 - 100 / (1 + gains / losses)


# ---------------------------------------------------------------- database
db: aiosqlite.Connection


async def init_db():
    global db
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS guild_coins(
            guild_id INTEGER, coin_id TEXT, symbol TEXT, name TEXT, channel_id INTEGER,
            PRIMARY KEY(guild_id, coin_id));
        CREATE TABLE IF NOT EXISTS priorities(
            guild_id INTEGER, user_id INTEGER, coin_id TEXT, target REAL, direction TEXT,
            PRIMARY KEY(guild_id, user_id, coin_id));
        """
    )
    await db.commit()


async def q(sql, args=()):
    async with db.execute(sql, args) as cur:
        return await cur.fetchall()


async def run(sql, args=()):
    await db.execute(sql, args)
    await db.commit()


# ------------------------------------------------------------------ kraken
class Kraken:
    """Free, keyless Kraken public market data (works from US servers)."""
    BASE = "https://api.kraken.com/0/public"
    ALIASES = {"XBT": "BTC", "XDG": "DOGE"}

    def __init__(self):
        self.session: aiohttp.ClientSession | None = None
        self.coins: dict[str, dict] = {}     # coin_id (Kraken altname, e.g. XBTUSD) -> info
        self.key_to_id: dict[str, str] = {}  # key Kraken uses in responses -> coin_id
        self.loaded_at = 0.0

    async def get(self, path, **params):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        for _ in range(3):
            async with self.session.get(f"{self.BASE}/{path}", params=params) as r:
                r.raise_for_status()
                data = await r.json()
            errors = data.get("error") or []
            if any("rate limit" in e.lower() for e in errors):
                await asyncio.sleep(3)
                continue
            if errors:
                raise RuntimeError(f"Kraken error: {errors}")
            return data["result"]
        raise RuntimeError("Kraken rate limit")

    async def load_pairs(self):
        """Every online <coin>/USD market becomes a trackable coin."""
        result = await self.get("AssetPairs")
        coins, key_to_id = {}, {}
        for key, info in result.items():
            ws = info.get("wsname") or ""
            if not ws.endswith("/USD") or info.get("status", "online") != "online":
                continue
            base = ws.split("/")[0]
            symbol = self.ALIASES.get(base, base)
            if symbol in FIAT:
                continue
            cid = info["altname"]
            coins[cid] = {"id": cid, "symbol": symbol, "name": NAMES.get(symbol, symbol)}
            key_to_id[key] = cid
            key_to_id[cid] = cid
        if coins:
            self.coins, self.key_to_id, self.loaded_at = coins, key_to_id, time.time()
        log.info("Loaded %d USD markets from Kraken", len(self.coins))

    async def maybe_refresh(self):
        if time.time() - self.loaded_at > 6 * 3600:
            try:
                await self.load_pairs()
            except Exception:
                log.exception("coin list refresh failed")

    def search(self, text, limit=25):
        t = text.strip().lower().replace("/usd", "")
        t = {"xbt": "btc", "xdg": "doge"}.get(t, t)
        hits = []
        for c in self.coins.values():
            sym, name = c["symbol"].lower(), c["name"].lower()
            if t in (sym, name):
                rank = 0
            elif sym.startswith(t) or name.startswith(t):
                rank = 1
            elif t in sym or t in name:
                rank = 2
            else:
                continue
            hits.append((rank, sym, c))
        hits.sort(key=lambda h: (h[0], h[1]))
        return [h[2] for h in hits[:limit]]

    def popular(self):
        by_sym = {c["symbol"]: c for c in self.coins.values()}
        return [(by_sym[s]["id"], s, by_sym[s]["name"]) for s in POPULAR_SYMBOLS if s in by_sym]

    async def prices(self, ids):
        """Batched ticker -> {coin_id: {price, change24h}} (change is vs today's 00:00 UTC open)."""
        ids = [i for i in ids if i in self.coins]
        out = {}
        for i in range(0, len(ids), 50):
            result = await self.get("Ticker", pair=",".join(ids[i:i + 50]))
            for key, t in result.items():
                cid = self.key_to_id.get(key)
                if not cid:
                    continue
                price = float(t["c"][0])
                o = t.get("o")
                opening = float(o[0] if isinstance(o, list) else o) if o else 0.0
                out[cid] = {"price": price,
                            "change24h": (price / opening - 1) * 100 if opening else None}
        return out

    async def closes(self, coin_id, interval=60, count=168):
        """Hourly closes (7 days) for the analysis."""
        result = await self.get("OHLC", pair=coin_id, interval=interval)
        for key, candles in result.items():
            if key != "last":
                return [float(c[4]) for c in candles][-count:]
        return []

    async def close(self):
        if self.session:
            await self.session.close()


kraken = Kraken()

# ---------------------------------------------------------------- analysis
AI_SYSTEM = (
    "You are a cautious crypto market analyst. You get technical indicator data for one coin "
    "(hourly data, last 7 days). Decide if now looks like a reasonable time to buy. You cannot "
    "predict prices; weigh trend, RSI, and distance from the 7d range. Reply with ONLY JSON: "
    '{"verdict":"BUY"|"HOLD"|"AVOID","confidence":0-100,"summary":"max 50 words citing the indicators"}'
)


class Analyzer:
    def __init__(self):
        self.cache: dict[str, tuple[float, dict | None]] = {}
        self.locks = defaultdict(asyncio.Lock)
        self.client = (anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
                       if anthropic and ANTHROPIC_KEY else None)

    async def get(self, coin_id, price):
        async with self.locks[coin_id]:
            hit = self.cache.get(coin_id)
            if hit and time.time() - hit[0] < AI_CACHE_SECONDS:
                return hit[1]
            try:
                result = await self._analyze(coin_id, price)
            except Exception:
                log.exception("analysis failed for %s", coin_id)
                result = None
            self.cache[coin_id] = (time.time(), result)
            return result

    async def _analyze(self, coin_id, price):
        series = await kraken.closes(coin_id)
        if len(series) < 30:
            return None
        stats = {
            "price": price,
            "change_24h_pct": round((price / series[-24] - 1) * 100, 2),
            "change_7d_pct": round((price / series[0] - 1) * 100, 2),
            "rsi_14": round(rsi(series) or 50, 1),
            "sma_24h": round(sma(series, 24), 8),
            "sma_72h": round(sma(series, 72), 8),
            "high_7d": max(series),
            "low_7d": min(series),
        }
        # Rule-based fallback
        r = stats["rsi_14"]
        verdict = {"verdict": "BUY" if r < 30 else "AVOID" if r > 70 else "HOLD",
                   "confidence": 50,
                   "summary": f"RSI is {r}; {stats['change_7d_pct']:+}% over 7 days."}
        if self.client:
            try:
                msg = await self.client.messages.create(
                    model=MODEL, max_tokens=300, system=AI_SYSTEM,
                    messages=[{"role": "user", "content": json.dumps(stats)}])
                text = msg.content[0].text
                data = json.loads(text[text.find("{"): text.rfind("}") + 1])
                if data.get("verdict") in ("BUY", "HOLD", "AVOID"):
                    verdict = {"verdict": data["verdict"],
                               "confidence": int(data.get("confidence", 50)),
                               "summary": str(data.get("summary", ""))[:500]}
            except Exception:
                log.exception("AI call failed, using rule-based verdict")
        return verdict


analyzer = Analyzer()

# ------------------------------------------------------------------ embeds
VERDICT_EMOJI = {"BUY": "🟢", "HOLD": "🟡", "AVOID": "🔴"}
FOOTER = "Data: Kraken • Analysis is not financial advice"


def add_analysis(embed, analysis):
    if analysis:
        embed.add_field(
            name=f"{VERDICT_EMOJI[analysis['verdict']]} AI take: {analysis['verdict']} "
                 f"({analysis['confidence']}% confidence)",
            value=analysis["summary"] or "—", inline=False)


def move_embed(row, price, prev, ch24, analysis):
    up = price > prev
    pct = (price - prev) / prev * 100
    e = discord.Embed(
        title=f"{'📈' if up else '📉'} {disp(row)} {'rose' if up else 'dropped'}",
        color=discord.Color.green() if up else discord.Color.red(),
        timestamp=datetime.now(timezone.utc))
    e.add_field(name="Price", value=fmt_price(price))
    e.add_field(name="Change", value=f"{pct:+.2f}% (from {fmt_price(prev)})")
    if ch24 is not None:
        e.add_field(name="Today (UTC)", value=f"{ch24:+.2f}%")
    add_analysis(e, analysis)
    e.set_footer(text=FOOTER)
    return e


def priority_embed(row, price, target, analysis):
    e = discord.Embed(title=f"🎯 {disp(row)} hit your priority price",
                      color=discord.Color.gold(), timestamp=datetime.now(timezone.utc))
    e.add_field(name="Your target", value=fmt_price(target))
    e.add_field(name="Current price", value=fmt_price(price))
    add_analysis(e, analysis)
    e.set_footer(text=FOOTER + " • Alert cleared; set a new one in /settings")
    return e


def coins_embed(rows):
    lines = [f"• **{disp(r)}** → <#{r['channel_id']}>" for r in rows]
    e = discord.Embed(title="📈 Tracked Coins",
                      description="\n".join(lines) or "No coins tracked yet. Tap a coin below to start.",
                      color=discord.Color.blurple())
    e.set_footer(text=f"Checking every {POLL_SECONDS}s • Green = tracked • Needs Manage Server to edit")
    return e


# ------------------------------------------------------------ track / untrack
async def track_coin(guild: discord.Guild, coin_id, symbol, name):
    chan_name = f"{symbol.lower()}-tracker"
    channel = discord.utils.get(guild.text_channels, name=chan_name)
    if channel is None:
        channel = await guild.create_text_channel(
            chan_name, topic=f"{name} price moves + AI analysis. Not financial advice.",
            reason="Coin tracker")
    await run("INSERT OR REPLACE INTO guild_coins VALUES(?,?,?,?,?)",
              (guild.id, coin_id, symbol.upper(), name, channel.id))
    return channel


async def untrack_coin(guild_id, coin_id):
    await run("DELETE FROM guild_coins WHERE guild_id=? AND coin_id=?", (guild_id, coin_id))
    await run("DELETE FROM priorities WHERE guild_id=? AND coin_id=?", (guild_id, coin_id))


async def guild_rows(guild_id):
    return await q("SELECT * FROM guild_coins WHERE guild_id=? ORDER BY symbol", (guild_id,))


async def refresh_coins_message(interaction: discord.Interaction):
    rows = await guild_rows(interaction.guild_id)
    await interaction.edit_original_response(embed=coins_embed(rows), view=CoinsView(rows))


NEED_CHANNELS = "I'm missing the **Manage Channels** permission, so I can't create the tracker channel."

# ---------------------------------------------------------- /coins tracked UI
class CoinToggle(discord.ui.Button):
    def __init__(self, coin_id, symbol, name, tracked, row):
        super().__init__(label=symbol, row=row,
                         style=discord.ButtonStyle.success if tracked else discord.ButtonStyle.secondary)
        self.coin_id, self.symbol, self.coin_name, self.tracked = coin_id, symbol, name, tracked

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            if self.tracked:
                await untrack_coin(interaction.guild_id, self.coin_id)
            else:
                await track_coin(interaction.guild, self.coin_id, self.symbol, self.coin_name)
        except discord.Forbidden:
            await interaction.followup.send(NEED_CHANNELS, ephemeral=True)
            return
        await refresh_coins_message(interaction)


class SearchModal(discord.ui.Modal, title="Search for a coin"):
    query = discord.ui.TextInput(label="Coin name or symbol", placeholder="e.g. pepe", max_length=50)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        coins = kraken.search(str(self.query))
        if not coins:
            await interaction.followup.send("No coins found. Try another name.", ephemeral=True)
            return
        view = discord.ui.View(timeout=300)
        view.add_item(ResultSelect(coins, interaction.message))
        await interaction.followup.send("Pick the coin to track:", view=view, ephemeral=True)


class ResultSelect(discord.ui.Select):
    def __init__(self, coins, source):
        opts = [discord.SelectOption(
            label=disp(c)[:100], value=c["id"],
            description=f"{c['symbol']}/USD on Kraken")
            for c in coins]
        super().__init__(placeholder="Pick a coin to track", options=opts)
        self.coins, self.source = {c["id"]: c for c in coins}, source

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        c = self.coins[self.values[0]]
        try:
            ch = await track_coin(interaction.guild, c["id"], c["symbol"], c["name"])
        except discord.Forbidden:
            await interaction.followup.send(NEED_CHANNELS, ephemeral=True)
            return
        await interaction.followup.send(f"✅ Tracking **{c['name']}** in {ch.mention}", ephemeral=True)
        if self.source:
            try:
                rows = await guild_rows(interaction.guild_id)
                await self.source.edit(embed=coins_embed(rows), view=CoinsView(rows))
            except discord.HTTPException:
                pass


class SearchButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Search any coin", emoji="🔍", style=discord.ButtonStyle.primary, row=2)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(SearchModal())


class RemoveSelect(discord.ui.Select):
    def __init__(self, rows):
        super().__init__(placeholder="Stop tracking a coin…", row=3, options=[
            discord.SelectOption(label=disp(r)[:100], value=r["coin_id"])
            for r in rows[:25]])

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await untrack_coin(interaction.guild_id, self.values[0])
        await refresh_coins_message(interaction)


class CoinsView(discord.ui.View):
    def __init__(self, rows):
        super().__init__(timeout=None)
        tracked = {r["coin_id"] for r in rows}
        for i, (cid, sym, name) in enumerate(kraken.popular()):
            self.add_item(CoinToggle(cid, sym, name, cid in tracked, row=i // 5))
        self.add_item(SearchButton())
        if rows:
            self.add_item(RemoveSelect(rows))

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.guild_permissions.manage_guild:
            return True
        await interaction.response.send_message(
            "You need the **Manage Server** permission to change tracked coins.", ephemeral=True)
        return False


# ------------------------------------------------------------- /settings UI
async def settings_payload(guild_id, user_id):
    tracked = await guild_rows(guild_id)
    mine = await q(
        "SELECT p.*, g.symbol, g.name FROM priorities p JOIN guild_coins g "
        "ON g.guild_id=p.guild_id AND g.coin_id=p.coin_id WHERE p.guild_id=? AND p.user_id=?",
        (guild_id, user_id))
    if mine:
        desc = "\n".join(
            f"• **{r['symbol']}**: DM when it {'rises to' if r['direction'] == 'up' else 'falls to'} "
            f"{fmt_price(r['target'])}" for r in mine)
    elif tracked:
        desc = "No priority prices yet. Pick a coin below to set one."
    else:
        desc = "No coins are tracked in this server yet. Ask an admin to use `/coins tracked`."
    embed = discord.Embed(title="⚙️ Your Priority Prices", description=desc, color=discord.Color.blurple())
    embed.set_footer(text="When a coin hits your price, I'll DM you (make sure your DMs are open)")
    view = discord.ui.View(timeout=None)
    if tracked:
        view.add_item(CoinPick(tracked))
    if mine:
        view.add_item(AlertRemove(mine))
    return embed, view


class PriceModal(discord.ui.Modal):
    price = discord.ui.TextInput(label="Alert me at (USD)", placeholder="58.67", max_length=20)

    def __init__(self, coin_id, symbol):
        super().__init__(title=f"{symbol} priority price")
        self.coin_id = coin_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            target = float(str(self.price).replace("$", "").replace(",", "").strip())
            if target <= 0:
                raise ValueError
        except ValueError:
            await interaction.response.send_message("Enter a number like `58.67`.", ephemeral=True)
            return
        await interaction.response.defer()
        cur = latest.get(self.coin_id)
        if cur is None:
            cur = (await kraken.prices([self.coin_id])).get(self.coin_id, {}).get("price")
        if cur is None:
            await interaction.followup.send("Couldn't fetch the current price. Try again.", ephemeral=True)
            return
        direction = "up" if target >= cur else "down"
        await run("INSERT OR REPLACE INTO priorities VALUES(?,?,?,?,?)",
                  (interaction.guild_id, interaction.user.id, self.coin_id, target, direction))
        embed, view = await settings_payload(interaction.guild_id, interaction.user.id)
        await interaction.edit_original_response(embed=embed, view=view)


class CoinPick(discord.ui.Select):
    def __init__(self, rows):
        super().__init__(placeholder="Set a priority price for…", row=0, options=[
            discord.SelectOption(label=disp(r)[:100], value=r["coin_id"])
            for r in rows[:25]])
        self.symbols = {r["coin_id"]: r["symbol"] for r in rows}

    async def callback(self, interaction: discord.Interaction):
        cid = self.values[0]
        await interaction.response.send_modal(PriceModal(cid, self.symbols[cid]))


class AlertRemove(discord.ui.Select):
    def __init__(self, rows):
        super().__init__(placeholder="Remove an alert…", row=1, options=[
            discord.SelectOption(label=f"{r['symbol']} @ {fmt_price(r['target'])}"[:100], value=r["coin_id"])
            for r in rows[:25]])

    async def callback(self, interaction: discord.Interaction):
        await run("DELETE FROM priorities WHERE guild_id=? AND user_id=? AND coin_id=?",
                  (interaction.guild_id, interaction.user.id, self.values[0]))
        embed, view = await settings_payload(interaction.guild_id, interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=view)


# ---------------------------------------------------------------- the bot
class GuardedTree(app_commands.CommandTree):
    """Blocks every slash command outside whitelisted servers."""

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.guild_id in ALLOWED_GUILD_IDS:
            return True
        await interaction.response.send_message(
            "This server isn't licensed for this bot.", ephemeral=True)
        return False


class TrackerBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree = GuardedTree(self)

    async def setup_hook(self):
        await init_db()
        await kraken.load_pairs()
        await self.tree.sync()
        poll_loop.start()

    async def close(self):
        await kraken.close()
        await super().close()


bot = TrackerBot()

coins_group = app_commands.Group(name="coins", description="Manage tracked coins", guild_only=True)


@coins_group.command(name="tracked", description="View and manage the coins this server tracks")
async def coins_tracked(interaction: discord.Interaction):
    rows = await guild_rows(interaction.guild_id)
    await interaction.response.send_message(embed=coins_embed(rows), view=CoinsView(rows))


@coins_group.command(name="list", description="Show every coin this bot can track")
async def coins_list(interaction: discord.Interaction):
    coins = sorted(kraken.coins.values(), key=lambda c: c["symbol"])
    text = f"{len(coins)} coins (USD markets on Kraken)\n\n" + "\n".join(
        f"{c['symbol']:<10}{c['name']}" for c in coins)
    await interaction.response.send_message(
        f"I can track **{len(coins)}** coins. Full list attached; use `/coins tracked` to add one.",
        file=discord.File(io.BytesIO(text.encode()), filename="supported_coins.txt"), ephemeral=True)


@bot.tree.command(name="settings", description="Set your priority price alerts")
@app_commands.guild_only()
async def settings(interaction: discord.Interaction):
    embed, view = await settings_payload(interaction.guild_id, interaction.user.id)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


bot.tree.add_command(coins_group)

# ------------------------------------------------------------- price loop
latest: dict[str, float] = {}       # newest price per coin
last_posted: dict[str, float] = {}  # price at last channel post per coin


async def get_channel(row):
    ch = bot.get_channel(row["channel_id"])
    if ch:
        return ch
    try:
        return await bot.fetch_channel(row["channel_id"])
    except discord.NotFound:  # channel was deleted: stop tracking
        await untrack_coin(row["guild_id"], row["coin_id"])
    except discord.HTTPException:
        pass
    return None


async def send_embed(dest, embed, content=None):
    try:
        await dest.send(content=content, embed=embed)
        return True
    except discord.HTTPException:
        return False


async def handle_coin(coin_id, d, rows, prios):
    price, ch24 = d["price"], d["change24h"]
    latest[coin_id] = price
    prev = last_posted.get(coin_id)
    if prev is None:
        last_posted[coin_id] = price
    moved = (prev is not None and prev > 0 and price != prev
             and abs(price - prev) / prev * 100 >= MIN_CHANGE_PCT)
    hits = [p for p in prios if (price >= p["target"] if p["direction"] == "up" else price <= p["target"])]
    if not moved and not hits:
        return
    analysis = await analyzer.get(coin_id, price)
    by_guild = {r["guild_id"]: r for r in rows}

    if moved:
        last_posted[coin_id] = price
        embed_cache = {}
        for r in rows:
            ch = await get_channel(r)
            if ch:
                e = embed_cache.setdefault(r["symbol"], move_embed(r, price, prev, ch24, analysis))
                await send_embed(ch, e)

    for p in hits:
        row = by_guild.get(p["guild_id"])
        if not row:
            continue
        e = priority_embed(row, price, p["target"], analysis)
        await run("DELETE FROM priorities WHERE guild_id=? AND user_id=? AND coin_id=?",
                  (p["guild_id"], p["user_id"], coin_id))
        try:
            user = bot.get_user(p["user_id"]) or await bot.fetch_user(p["user_id"])
            await user.send(embed=e)
        except discord.HTTPException:  # DMs closed: ping them in the coin channel instead
            ch = await get_channel(row)
            if ch:
                await send_embed(ch, e, content=f"<@{p['user_id']}> (couldn't DM you)")


async def run_poll():
    rows = [r for r in await q("SELECT * FROM guild_coins") if r["guild_id"] in ALLOWED_GUILD_IDS]
    prios = [p for p in await q("SELECT * FROM priorities") if p["guild_id"] in ALLOWED_GUILD_IDS]
    ids = {r["coin_id"] for r in rows} | {p["coin_id"] for p in prios}
    if not ids:
        return
    await kraken.maybe_refresh()
    data = await kraken.prices(ids)
    rows_by, prios_by = defaultdict(list), defaultdict(list)
    for r in rows:
        rows_by[r["coin_id"]].append(r)
    for p in prios:
        prios_by[p["coin_id"]].append(p)
    results = await asyncio.gather(
        *(handle_coin(cid, d, rows_by[cid], prios_by[cid]) for cid, d in data.items()),
        return_exceptions=True)
    for res in results:
        if isinstance(res, Exception):
            log.error("coin handler failed", exc_info=res)


@tasks.loop(seconds=POLL_SECONDS)
async def poll_loop():
    try:
        await run_poll()
    except Exception:
        log.exception("poll failed")


@poll_loop.before_loop
async def before_poll():
    await bot.wait_until_ready()


async def leave_if_unlicensed(guild: discord.Guild):
    if guild.id in ALLOWED_GUILD_IDS:
        return
    log.warning("Leaving unlicensed guild %s (%s)", guild.name, guild.id)
    if guild.system_channel:
        await send_embed(guild.system_channel, discord.Embed(
            title="Server not licensed",
            description="This bot is only available to servers that purchased it. Leaving now."))
    await guild.leave()


@bot.event
async def on_guild_join(guild: discord.Guild):
    await leave_if_unlicensed(guild)


@bot.event
async def on_ready():
    log.info("Logged in as %s (polling every %ss)", bot.user, POLL_SECONDS)
    for g in list(bot.guilds):  # catches servers added while the bot was offline
        await leave_if_unlicensed(g)


if __name__ == "__main__":
    bot.run(TOKEN)
