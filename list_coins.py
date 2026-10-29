"""Prints every coin the bot can track (online <coin>/USD markets on Kraken) and saves supported_coins.txt.
Run: python list_coins.py   (no key or install needed)"""
import json
import urllib.request

ALIASES = {"XBT": "BTC", "XDG": "DOGE"}
FIAT = {"USD", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY"}

req = urllib.request.Request("https://api.kraken.com/0/public/AssetPairs", headers={"User-Agent": "coin-list"})
data = json.load(urllib.request.urlopen(req, timeout=20))["result"]
symbols = sorted({
    ALIASES.get(i["wsname"].split("/")[0], i["wsname"].split("/")[0])
    for i in data.values()
    if (i.get("wsname") or "").endswith("/USD") and i.get("status", "online") == "online"
} - FIAT)
open("supported_coins.txt", "w").write("\n".join(symbols))
print(f"{len(symbols)} coins:\n" + ", ".join(symbols))
