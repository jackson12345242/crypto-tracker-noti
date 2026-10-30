"""Crypto tracker Discord bot: discord.py 2.x + Kraken public API (free) + Claude analysis."""
import asyncio
import io
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, time as dtime, timedelta, timezone

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

from whitelist import ALLOWED_GUILD_IDS as _STATIC_ALLOWED

load_dotenv()
ALLOWED_GUILD_IDS = set(_STATIC_ALLOWED)  # whitelist.py + servers added with /whitelist
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("cryptobot")

TOKEN = os.environ["DISCORD_TOKEN"]
POLL_SECONDS = max(5, int(os.getenv("POLL_SECONDS", "15")))
DEFAULT_INTERVAL = 15  # seconds between channel updates unless a server changes it
MIN_INTERVAL = 15
ALWAYS_POST = os.getenv("ALWAYS_POST", "1") == "1"  # 1 = post every check, even if price is unchanged
MIN_CHANGE_PCT = float(os.getenv("MIN_CHANGE_PCT", "0.1"))  # only used when ALWAYS_POST=0  # 0 = post on ANY change
AI_CACHE_SECONDS = int(os.getenv("AI_CACHE_SECONDS", "600"))
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5-5")
DB_PATH = os.getenv("DB_PATH", "tracker.db")
OWNER_ID = int(os.getenv("OWNER_ID", "645395932812279844"))  # only this user can use /whitelist

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
        CREATE TABLE IF NOT EXISTS allowed_guilds(guild_id INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS guild_settings(
            guild_id INTEGER PRIMARY KEY, min_change REAL, interval INTEGER);
        CREATE TABLE IF NOT EXISTS report_subs(
            guild_id INTEGER, user_id INTEGER, PRIMARY KEY(guild_id, user_id));
        CREATE TABLE IF NOT EXISTS daily(
            day TEXT, coin_id TEXT, open REAL, high REAL, low REAL, last REAL,
            PRIMARY KEY(day, coin_id));
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
        self.bad: dict[str, float] = {}      # coin_id -> skip-until timestamp

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

    async def _ticker(self, chunk):
        out = {}
        result = await self.get("Ticker", pair=",".join(chunk))
        for key, t in result.items():
            cid = self.key_to_id.get(key)
            if not cid:
                continue
            price = float(t["c"][0])
            o = t.get("o")
            opening = float(o[0] if isinstance(o, list) else o) if o else 0.0
            def first(v):
                return float(v[0] if isinstance(v, list) else v) if v else None
            out[cid] = {"price": price, "open": opening or None,
                        "high": first(t.get("h")), "low": first(t.get("l")),
                        "change24h": (price / opening - 1) * 100 if opening else None}
        return out

    def _mark_bad(self, cid, err):
        self.bad[cid] = time.time() + 600
        log.warning("Kraken has no ticker data for %s (%s); skipping it for 10 min", cid, err)

    async def _fetch(self, url, **params):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        async with self.session.get(url, params=params) as r:
            r.raise_for_status()
            return await r.json()

    async def _binance(self, ids):
        """Fallback when Kraken's ticker fails: Binance's free public price list (USDT ~ USD)."""
        wanted = {self.coins[c]["symbol"] + "USDT": c for c in ids}
        try:
            data = await self._fetch("https://api.binance.com/api/v3/ticker/price")
        except Exception as e:
            log.warning("Binance fallback failed: %s", e)
            return {}
        out = {}
        for t in data:
            cid = wanted.get(t["symbol"])
            if cid:
                out[cid] = {"price": float(t["price"]), "change24h": None}
        if out:
            log.info("Used Binance fallback for %d coin(s)", len(out))
        return out

    async def prices(self, ids):
        """Batched ticker -> {coin_id: {price, change24h}} (change is vs today's 00:00 UTC open).
        If Kraken rejects a batch, retry coin by coin; anything Kraken can't give falls back to Binance."""
        now = time.time()
        wanted = [i for i in ids if i in self.coins]
        ids = [i for i in wanted if self.bad.get(i, 0) < now]
        out = {}
        for i in range(0, len(ids), 50):
            chunk = ids[i:i + 50]
            try:
                out.update(await self._ticker(chunk))
            except RuntimeError as e:
                if "Kraken error" not in str(e):  # e.g. rate limit: let the loop retry next cycle
                    raise
                if len(chunk) == 1:
                    self._mark_bad(chunk[0], e)
                    continue
                for cid in chunk:
                    try:
                        out.update(await self._ticker([cid]))
                    except RuntimeError as e2:
                        if "Kraken error" not in str(e2):
                            raise
                        self._mark_bad(cid, e2)
        missing = [c for c in wanted if c not in out]
        if missing:
            out.update(await self._binance(missing))
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
    flat = price == prev
    pct = (price - prev) / prev * 100
    e = discord.Embed(
        title=f"{'📊' if flat else '📈' if up else '📉'} {disp(row)} "
              f"{'unchanged' if flat else 'rose' if up else 'dropped'}",
        color=discord.Color.light_grey() if flat else discord.Color.green() if up else discord.Color.red(),
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
    # Welcome post: shows the current price and proves the bot can post in the channel
    try:
        cur = (await kraken.prices([coin_id])).get(coin_id)
        e = discord.Embed(title=f"📡 Tracking {name if name == symbol.upper() else f'{name} ({symbol.upper()})'}",
                          description="I'll post here based on this server's update settings (see /settings).",
                          color=discord.Color.blurple())
        if cur:
            e.add_field(name="Price now", value=fmt_price(cur["price"]))
        e.set_footer(text=FOOTER)
        await channel.send(embed=e)
    except Exception as err:
        log.warning("Couldn't post in %s (needs View Channel, Send Messages, Embed Links): %s", channel.id, err)
    return channel


async def untrack_coin(guild_id, coin_id, delete_channel=True):
    rows = await q("SELECT channel_id FROM guild_coins WHERE guild_id=? AND coin_id=?", (guild_id, coin_id))
    await run("DELETE FROM guild_coins WHERE guild_id=? AND coin_id=?", (guild_id, coin_id))
    await run("DELETE FROM priorities WHERE guild_id=? AND coin_id=?", (guild_id, coin_id))
    if delete_channel and rows:
        ch = bot.get_channel(rows[0]["channel_id"])
        if ch:
            try:
                await ch.delete(reason="Coin is no longer tracked")
            except discord.HTTPException as e:
                log.warning("Couldn't delete channel %s: %s", rows[0]["channel_id"], e)


async def guild_rows(guild_id):
    return await q("SELECT * FROM guild_coins WHERE guild_id=? ORDER BY symbol", (guild_id,))


async def refresh_coins_message(interaction: discord.Interaction):
    rows = await guild_rows(interaction.guild_id)
    try:
        await interaction.edit_original_response(embed=coins_embed(rows), view=CoinsView(rows))
    except discord.HTTPException:  # e.g. the message lived in the channel we just deleted
        pass


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
    sub = bool(await q("SELECT 1 FROM report_subs WHERE guild_id=? AND user_id=?", (guild_id, user_id)))
    desc += "\n\n📰 **Daily report:** " + (
        "ON. I'll DM you a recap of every tracked coin at 00:00 UTC" if sub else "OFF")
    mc, iv = await get_cfg(guild_id)
    desc += f"\n\n📡 **Channel updates (whole server):** at most every {iv}s, " + (
        "posting every time" if mc == 0 else f"only when the price moves {mc:g}% or more")
    embed = discord.Embed(title="⚙️ Your Settings", description=desc, color=discord.Color.blurple())
    embed.set_footer(text="When a coin hits your price, I'll DM you (make sure your DMs are open)")
    view = discord.ui.View(timeout=None)
    if tracked:
        view.add_item(CoinPick(tracked))
    if mine:
        view.add_item(AlertRemove(mine))
    view.add_item(ReportToggle(sub))
    view.add_item(UpdateButton(mc, iv))
    return embed, view


async def get_cfg(guild_id):
    rows = await q("SELECT * FROM guild_settings WHERE guild_id=?", (guild_id,))
    return (rows[0]["min_change"], rows[0]["interval"]) if rows else (0.0, DEFAULT_INTERVAL)


class UpdateModal(discord.ui.Modal, title="Channel update settings"):
    pct = discord.ui.TextInput(label="Min % change to post (0 = every update)", max_length=8)
    secs = discord.ui.TextInput(label=f"Seconds between updates (min {MIN_INTERVAL})", max_length=6)

    def __init__(self, mc, iv):
        super().__init__()
        self.pct.default = f"{mc:g}"
        self.secs.default = str(iv)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mc = float(str(self.pct).replace("%", "").strip())
            iv = int(float(str(self.secs).strip()))
        except ValueError:
            await interaction.response.send_message("Enter numbers, like `0.5` and `30`.", ephemeral=True)
            return
        if not 0 <= mc <= 100:
            await interaction.response.send_message("The % change must be between 0 and 100.", ephemeral=True)
            return
        if iv < MIN_INTERVAL:
            await interaction.response.send_message(
                f"The minimum is {MIN_INTERVAL} seconds.", ephemeral=True)
            return
        iv = min(iv, 86400)
        await interaction.response.defer()
        await run("INSERT OR REPLACE INTO guild_settings VALUES(?,?,?)", (interaction.guild_id, mc, iv))
        embed, view = await settings_payload(interaction.guild_id, interaction.user.id)
        await interaction.edit_original_response(embed=embed, view=view)


class UpdateButton(discord.ui.Button):
    def __init__(self, mc, iv):
        super().__init__(label="Channel updates", emoji="📡", row=3, style=discord.ButtonStyle.secondary)
        self.mc, self.iv = mc, iv

    async def callback(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message(
                "You need the **Manage Server** permission to change channel update settings.", ephemeral=True)
            return
        await interaction.response.send_modal(UpdateModal(self.mc, self.iv))


class ReportToggle(discord.ui.Button):
    def __init__(self, on):
        super().__init__(label=f"Daily report: {'ON' if on else 'OFF'}", emoji="📰", row=2,
                         style=discord.ButtonStyle.success if on else discord.ButtonStyle.secondary)
        self.on = on

    async def callback(self, interaction: discord.Interaction):
        if self.on:
            await run("DELETE FROM report_subs WHERE guild_id=? AND user_id=?",
                      (interaction.guild_id, interaction.user.id))
        else:
            await run("INSERT OR REPLACE INTO report_subs VALUES(?,?)",
                      (interaction.guild_id, interaction.user.id))
        embed, view = await settings_payload(interaction.guild_id, interaction.user.id)
        await interaction.response.edit_message(embed=embed, view=view)


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
        if interaction.guild_id in ALLOWED_GUILD_IDS or interaction.user.id == OWNER_ID:
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
        ALLOWED_GUILD_IDS.update(r["guild_id"] for r in await q("SELECT guild_id FROM allowed_guilds"))
        await kraken.load_pairs()
        await self.tree.sync()
        poll_loop.start()
        daily_report_loop.start()

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


@coins_group.command(name="test", description="Send a test post to every tracker channel and show bot status")
async def coins_test(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message("You need **Manage Server** for this.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    rows = await guild_rows(interaction.guild_id)
    lines = []
    for r in rows:
        ch = await get_channel(r)
        if not ch:
            lines.append(f"❌ {r['symbol']}: can't access its channel")
            continue
        ok = await send_embed(ch, discord.Embed(title=f"✅ Test post: {disp(r)}", color=discord.Color.green()))
        lines.append(f"{'✅' if ok else '❌ no permission'} {r['symbol']} → {ch.mention}")
    ago = f"{int(time.time() - poll_state['ok_at'])}s ago" if poll_state["ok_at"] else "never"
    lines.append(f"\nPrice loop running: {'yes' if poll_loop.is_running() else 'NO'} | "
                 f"last successful check: {ago} | prices received: {poll_state['prices']}")
    if poll_state["error"]:
        lines.append(f"Last error: `{poll_state['error']}`")
    await interaction.followup.send("\n".join(lines) if rows else "No coins tracked yet.", ephemeral=True)


@bot.tree.command(name="whitelist", description="Owner only: allow the bot to join a server")
@app_commands.describe(guild_id="ID of the server to allow")
async def whitelist_cmd(interaction: discord.Interaction, guild_id: str):
    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Owner only.", ephemeral=True)
        return
    if not guild_id.strip().isdigit():
        await interaction.response.send_message("That doesn't look like a server ID (numbers only).", ephemeral=True)
        return
    gid = int(guild_id.strip())
    await run("INSERT OR IGNORE INTO allowed_guilds VALUES(?)", (gid,))
    ALLOWED_GUILD_IDS.add(gid)
    await interaction.response.send_message(
        f"✅ Server `{gid}` is whitelisted. The bot can join it now.", ephemeral=True)


@bot.tree.command(name="unwhitelist", description="Owner only: remove a server and make the bot leave it")
@app_commands.describe(guild_id="ID of the server to remove")
async def unwhitelist_cmd(interaction: discord.Interaction, guild_id: str):
    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Owner only.", ephemeral=True)
        return
    if not guild_id.strip().isdigit():
        await interaction.response.send_message("That doesn't look like a server ID (numbers only).", ephemeral=True)
        return
    gid = int(guild_id.strip())
    await run("DELETE FROM allowed_guilds WHERE guild_id=?", (gid,))
    ALLOWED_GUILD_IDS.discard(gid)
    note = " It's also listed in `whitelist.py`, so remove it there too." if gid in _STATIC_ALLOWED else ""
    guild = bot.get_guild(gid)
    if guild:
        await guild.leave()
    await interaction.response.send_message(f"✅ Removed `{gid}`." + note, ephemeral=True)


@bot.tree.command(name="currentreport", description="Today's report so far for a tracked coin")
@app_commands.describe(coin="Which tracked coin")
@app_commands.guild_only()
async def currentreport(interaction: discord.Interaction, coin: str):
    rows = await guild_rows(interaction.guild_id)
    t = coin.strip().lower()
    row = next((r for r in rows if t in (r["coin_id"].lower(), r["symbol"].lower(), r["name"].lower())), None)
    if not row:
        await interaction.response.send_message(
            "That coin isn't tracked in this server. Add it with `/coins tracked`.", ephemeral=True)
        return
    await interaction.response.defer()
    embed = await report_embed(row, utc_day(), final=False)
    if embed:
        await interaction.followup.send(embed=embed)
    else:
        await interaction.followup.send("No price data for today yet. Try again in a minute.")


@currentreport.autocomplete("coin")
async def currentreport_autocomplete(interaction: discord.Interaction, current: str):
    if not interaction.guild_id:
        return []
    cur = current.lower()
    return [app_commands.Choice(name=disp(r)[:100], value=r["coin_id"])
            for r in await guild_rows(interaction.guild_id)
            if cur in r["symbol"].lower() or cur in r["name"].lower()][:25]


@bot.tree.command(name="settings", description="Set your priority price alerts")
@app_commands.guild_only()
async def settings(interaction: discord.Interaction):
    embed, view = await settings_payload(interaction.guild_id, interaction.user.id)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


bot.tree.add_command(coins_group)

# ------------------------------------------------------------- price loop
latest: dict[str, float] = {}       # newest price per coin
last_posted: dict[str, float] = {}  # (unused now; kept for compatibility)
guild_last: dict[tuple, tuple] = {}  # (guild, coin) -> (price, time) at that server's last post
guild_cfg: dict[int, tuple] = {}     # guild -> (min % change, seconds between updates)
poll_state = {"ok_at": None, "error": None, "prices": 0}


async def get_channel(row):
    ch = bot.get_channel(row["channel_id"])
    if ch:
        return ch
    try:
        return await bot.fetch_channel(row["channel_id"])
    except discord.NotFound:  # channel was deleted: stop tracking
        await untrack_coin(row["guild_id"], row["coin_id"], delete_channel=False)
    except discord.HTTPException as e:
        log.warning("Can't access tracker channel %s (check bot permissions): %s", row["channel_id"], e)
    return None


async def send_embed(dest, embed, content=None):
    try:
        await dest.send(content=content, embed=embed)
        return True
    except discord.HTTPException as e:
        log.warning("Couldn't send to %s: %s", getattr(dest, "id", dest), e)
        return False


async def handle_coin(coin_id, d, rows, prios):
    price, ch24 = d["price"], d["change24h"]
    latest[coin_id] = price
    now = time.time()
    due = []  # (row, price at this server's last post)
    for r in rows:
        key = (r["guild_id"], coin_id)
        min_pct, interval = guild_cfg.get(r["guild_id"], (0.0, DEFAULT_INTERVAL))
        last = guild_last.get(key)
        if last is None:  # first sighting: just remember the price
            guild_last[key] = (price, now)
            continue
        prev_price, prev_ts = last
        if now - prev_ts < interval - 2:  # not time yet (2s slack for timing jitter)
            continue
        if min_pct > 0:
            move = abs(price - prev_price) / prev_price * 100 if prev_price else 0.0
            if price == prev_price or move < min_pct:
                continue
        due.append((r, prev_price))
    hits = [p for p in prios if (price >= p["target"] if p["direction"] == "up" else price <= p["target"])]
    if not due and not hits:
        return
    analysis = await analyzer.get(coin_id, price)
    by_guild = {r["guild_id"]: r for r in rows}

    sent = 0
    for r, prev_price in due:
        guild_last[(r["guild_id"], coin_id)] = (price, now)
        ch = await get_channel(r)
        if ch:
            sent += await send_embed(ch, move_embed(r, price, prev_price, ch24, analysis))
    if due:
        log.info("%s -> posted to %d/%d channel(s)", coin_id, sent, len(due))

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
    guild_cfg.clear()
    guild_cfg.update({c["guild_id"]: (c["min_change"], c["interval"])
                      for c in await q("SELECT * FROM guild_settings")})
    ids = {r["coin_id"] for r in rows} | {p["coin_id"] for p in prios}
    if not ids:
        return
    await kraken.maybe_refresh()
    data = await kraken.prices(ids)
    poll_state["prices"] = len(data)
    day = utc_day()
    await db.executemany(
        "INSERT INTO daily(day,coin_id,open,high,low,last) VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(day,coin_id) DO UPDATE SET high=MAX(high,excluded.high), "
        "low=MIN(low,excluded.low), last=excluded.last",
        [(day, cid, d.get("open") or d["price"], max(d.get("high") or 0, d["price"]),
          min(d.get("low") or d["price"], d["price"]), d["price"]) for cid, d in data.items()])
    await db.commit()
    if not data:
        log.warning("Kraken returned no prices for %d tracked coin(s)", len(ids))
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


def utc_day(offset_minutes=0):
    return (datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)).strftime("%Y-%m-%d")


async def report_embed(row, day, final):
    stats = await q("SELECT * FROM daily WHERE day=? AND coin_id=?", (day, row["coin_id"]))
    if not stats:
        return None
    st = stats[0]
    o, last, hi, lo = st["open"], st["last"], st["high"], st["low"]
    pct = (last / o - 1) * 100 if o else 0.0
    e = discord.Embed(
        title=f"{'📰 Daily report' if final else '🕒 Report so far'}: {disp(row)}",
        description=f"{day} (UTC)",
        color=discord.Color.green() if last >= o else discord.Color.red(),
        timestamp=datetime.now(timezone.utc))
    e.add_field(name="Started at", value=fmt_price(o))
    e.add_field(name="Ended at" if final else "Now", value=fmt_price(last))
    e.add_field(name="Change", value=f"{pct:+.2f}%")
    e.add_field(name="High", value=fmt_price(hi))
    e.add_field(name="Low", value=fmt_price(lo))
    if lo:
        e.add_field(name="Range", value=f"{(hi - lo) / lo * 100:.2f}%")
    add_analysis(e, await analyzer.get(row["coin_id"], last))
    e.set_footer(text=FOOTER)
    return e


async def send_daily_reports():
    day = utc_day(-5)  # the UTC day that just ended
    by_guild = defaultdict(list)
    for sub in await q("SELECT * FROM report_subs"):
        if sub["guild_id"] in ALLOWED_GUILD_IDS:
            by_guild[sub["guild_id"]].append(sub["user_id"])
    cache = {}
    for gid, users in by_guild.items():
        embeds = []
        for r in await guild_rows(gid):
            if r["coin_id"] not in cache:
                cache[r["coin_id"]] = await report_embed(r, day, final=True)
            if cache[r["coin_id"]]:
                embeds.append(cache[r["coin_id"]])
        if not embeds:
            continue
        guild = bot.get_guild(gid)
        header = f"📰 Daily report for **{guild.name if guild else 'your server'}**"
        for uid in users:
            try:
                user = bot.get_user(uid) or await bot.fetch_user(uid)
                for i in range(0, len(embeds), 5):  # keep each message under Discord's size limit
                    await user.send(content=header if i == 0 else None, embeds=embeds[i:i + 5])
                    await asyncio.sleep(1)
            except discord.HTTPException as e:
                log.warning("Couldn't DM daily report to %s: %s", uid, e)
    await run("DELETE FROM daily WHERE day < ?", (utc_day(-60 * 24 * 30),))
    log.info("Daily reports sent for %s", day)


@tasks.loop(time=dtime(hour=0, minute=0, tzinfo=timezone.utc))
async def daily_report_loop():
    try:
        await send_daily_reports()
    except Exception:
        log.exception("daily report failed")


@daily_report_loop.before_loop
async def before_daily():
    await bot.wait_until_ready()


@tasks.loop(seconds=POLL_SECONDS)
async def poll_loop():
    try:
        await run_poll()
        poll_state["ok_at"], poll_state["error"] = time.time(), None
    except Exception as e:
        poll_state["error"] = repr(e)[:300]
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
