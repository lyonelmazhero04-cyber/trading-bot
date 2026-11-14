"""
bot.py - Deriv 1HZ10V Async Trading Bot for Render
"""
import asyncio
import json
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

import websockets
import aiohttp
from google import genai
from google.genai import types

DERIV_TOKEN = os.environ.get("DERIV_TOKEN")
DERIV_WS = "wss://ws.binaryws.com/websockets/v3?app_id=1089"
GEMINI_KEY = os.environ.get("GEMINI_KEY")
TG_TOKEN = os.environ.get("TG_TOKEN")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID")
SYMBOL = "1HZ10V"
BASE_STAKE = 0.35

# Runtime state
STARTING_BALANCE = 0.0
PROFIT_TARGET = 0.0
TRADING_ACTIVE = True
BOT_RUNNING = True
LAST_BALANCE = 0.0

# Statistics
TOTAL_TRADES = 0
WINS = 0
LOSSES = 0
CONSECUTIVE_LOSSES = 0
COOLDOWN_UNTIL = 0.0

# Price cache
TICK_BUFFER = deque(maxlen=30)

# ==================== Mini HTTP Server for Render Health Check ====================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is active and running!")

def run_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()

# Start the health check server in a background thread so Render's port check passes
threading.Thread(target=run_health_server, daemon=True).start()

# ==================== 2. Utilities ====================
def now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

async def tg_send(text: str):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    print(f"[TG] Failed: {await resp.text()}")
    except Exception as e:
        print(f"[TG] Error: {e}")

# ==================== 3. Gemini AI Filter ====================
async def gemini_filter(direction: str, rsi: float, net_change: float) -> bool:
    client = genai.Client(api_key=GEMINI_KEY)
    
    if direction == "CALL" and rsi <= 58:
        return False
    if direction == "PUT" and rsi >= 42:
        return False
    
    prompt = f"""You are a strict trading filter for {SYMBOL}. Current state:
- Proposed direction: {direction}
- RSI(14): {rsi:.1f}
- 10-tick net change: {net_change:.5f}

Rules:
- CALL requires RSI > 58 and positive momentum.
- PUT requires RSI < 42 and negative momentum.
- Confidence must be >= 7/10.

Reply ONLY with: CONFIRM or REJECT. No explanation."""

    try:
        resp = await client.aio.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.1, max_output_tokens=10)
        )
        answer = resp.text.strip().upper()
        return "CONFIRM" in answer
    except Exception as e:
        print(f"[Gemini] Error: {e}, falling back to RSI-only")
        return (direction == "CALL" and rsi > 58) or (direction == "PUT" and rsi < 42)

# ==================== 4. Trading Logic ====================
def dynamic_stake(balance: float) -> float:
    return max(round(balance * 0.10, 2), BASE_STAKE)

def check_kill_switch(balance: float) -> bool:
    if balance < STARTING_BALANCE * 0.5:
        return True
    return False

async def place_trade(ws, direction: str, stake: float) -> Optional[dict]:
    contract_type = "CALL" if direction == "CALL" else "PUT"
    
    proposal_req = {
        "proposal": 1,
        "amount": stake,
        "basis": "stake",
        "contract_type": contract_type,
        "currency": "USD",
        "duration": 5,
        "duration_unit": "t",
        "underlying_symbol": SYMBOL
    }
    await ws.send(json.dumps(proposal_req))
    
    proposal_id = None
    ask_price = None
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
            if msg.get("msg_type") == "proposal":
                proposal_id = msg["proposal"]["id"]
                ask_price = msg["proposal"]["ask_price"]
                break
            if msg.get("error"):
                print(f"[Proposal] Error: {msg['error']['message']}")
                return None
        except asyncio.TimeoutError:
            continue
    
    if not proposal_id:
        return None
    
    buy_req = {"buy": proposal_id, "price": ask_price}
    await ws.send(json.dumps(buy_req))
    
    contract_id = None
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
            if msg.get("msg_type") == "buy":
                contract_id = msg["buy"]["contract_id"]
                break
            if msg.get("error"):
                print(f"[Buy] Error: {msg['error']['message']}")
                return None
        except asyncio.TimeoutError:
            continue
    
    if not contract_id:
        return None
    
    sub_req = {"proposal_open_contract": 1, "contract_id": contract_id, "subscribe": 1}
    await ws.send(json.dumps(sub_req))
    
    result = {"contract_id": contract_id, "stake": stake, "profit": 0.0, "won": False}
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            if msg.get("msg_type") == "proposal_open_contract":
                poc = msg["proposal_open_contract"]
                if poc.get("is_sold") or poc.get("status") == "sold":
                    result["profit"] = float(poc.get("profit", 0))
                    result["won"] = result["profit"] > 0
                    break
        except asyncio.TimeoutError:
            continue
    
    try:
        await ws.send(json.dumps({"forget": msg.get("subscription", {}).get("id", "")}))
    except:
        pass
    
    return result

# ==================== 5. Main Trading Loop ====================
async def trading_loop(ws):
    global TRADING_ACTIVE, TOTAL_TRADES, WINS, LOSSES, CONSECUTIVE_LOSSES, COOLDOWN_UNTIL, BOT_RUNNING, LAST_BALANCE
    
    await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
    await ws.send(json.dumps({"balance": 1, "subscribe": 0}))
    
    while BOT_RUNNING:
        try:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=1))
            
            if msg.get("msg_type") == "balance":
                bal = float(msg["balance"]["balance"])
                LAST_BALANCE = bal
                
                if bal >= PROFIT_TARGET and PROFIT_TARGET > 0:
                    await shutdown_and_summary("🎯 PROFIT TARGET REACHED (+25%)")
                    return
                
                if check_kill_switch(bal):
                    await shutdown_and_summary("🛑 HARD KILL SWITCH: Balance < 50% of starting")
                    return
            
            if msg.get("msg_type") == "tick":
                price = float(msg["tick"]["quote"])
                TICK_BUFFER.append(price)
                
                if len(TICK_BUFFER) < 20:
                    continue
                
                if time.time() < COOLDOWN_UNTIL:
                    continue
                
                if not TRADING_ACTIVE:
                    continue
                
                closes = list(TICK_BUFFER)
                rsi = calc_rsi(closes, 14)
                net_change = closes[-1] - closes[-11] if len(closes) >= 11 else 0
                
                direction = None
                if rsi > 58 and net_change > 0:
                    direction = "CALL"
                elif rsi < 42 and net_change < 0:
                    direction = "PUT"
                
                if not direction:
                    continue
                
                confirmed = await gemini_filter(direction, rsi, net_change)
                if not confirmed:
                    continue
                
                stake = dynamic_stake(LAST_BALANCE)
                print(f"[{now_ts()}] Trade: {direction} stake=${stake:.2f} RSI={rsi:.1f}")
                
                result = await place_trade(ws, direction, stake)
                if result:
                    TOTAL_TRADES += 1
                    if result["won"]:
                        WINS += 1
                        CONSECUTIVE_LOSSES = 0
                        print(f"[{now_ts()}] WIN +${result['profit']:.2f}")
                    else:
                        LOSSES += 1
                        CONSECUTIVE_LOSSES += 1
                        print(f"[{now_ts()}] LOSS -${abs(result['profit']):.2f}")
                        
                        if CONSECUTIVE_LOSSES >= 2:
                            COOLDOWN_UNTIL = time.time() + 1800
                            CONSECUTIVE_LOSSES = 0
                            await tg_send("⏸ *Cooldown activated*: 2 consecutive losses. Pausing for 30 minutes.")
                
                await asyncio.sleep(1)
                
        except asyncio.TimeoutError:
            continue
        except websockets.exceptions.ConnectionClosed:
            raise
        except Exception as e:
            print(f"[Loop] Error: {e}")
            await asyncio.sleep(1)

# ==================== 6. Telegram Command Listener ====================
async def telegram_listener():
    global TRADING_ACTIVE, BOT_RUNNING
    offset = 0
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    
    while BOT_RUNNING:
        try:
            params = {"offset": offset, "timeout": 30}
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, params=params, timeout=35) as resp:
                    data = await resp.json()
            
            if not data.get("ok"):
                await asyncio.sleep(5)
                continue
            
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                text = msg.get("text", "").strip().lower()
                
                if text == "/stop":
                    TRADING_ACTIVE = False
                    await tg_send("⏸ *Bot paused by user command.*")
                elif text == "/start":
                    TRADING_ACTIVE = True
                    await tg_send("▶ *Bot resumed trading.*")
                    
        except Exception as e:
            print(f"[TG Listener] Error: {e}")
            await asyncio.sleep(5)

# ==================== 7. Summary & Shutdown ====================
async def shutdown_and_summary(reason: str):
    global BOT_RUNNING, TRADING_ACTIVE
    BOT_RUNNING = False
    TRADING_ACTIVE = False
    
    net = LAST_BALANCE - STARTING_BALANCE
    net_pct = (net / STARTING_BALANCE * 100) if STARTING_BALANCE > 0 else 0
    win_rate = (WINS / TOTAL_TRADES * 100) if TOTAL_TRADES > 0 else 0
    
    summary = f"""📊 *Session Summary*

*Reason:* {reason}

*Starting Balance:* ${STARTING_BALANCE:.2f}
*Final Balance:* ${LAST_BALANCE:.2f}
*Net Profit:* ${net:.2f} ({net_pct:+.1f}%)

*Total Trades:* {TOTAL_TRADES}
*Wins:* {WINS} | *Losses:* {LOSSES}
*Win Rate:* {win_rate:.1f}%
"""
    await tg_send(summary)
    print(summary)

# ==================== 8. Main Entry ====================
async def main():
    global STARTING_BALANCE, PROFIT_TARGET, LAST_BALANCE
    
    tg_task = asyncio.create_task(telegram_listener())
    
    backoff = 1
    while BOT_RUNNING:
        try:
            async with websockets.connect(DERIV_WS, ping_interval=20) as ws:
                await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
                auth_msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
                
                if auth_msg.get("error"):
                    print(f"[Auth] Failed: {auth_msg['error']['message']}")
                    await tg_send(f"❌ Auth failed: {auth_msg['error']['message']}")
                    return
                
                await ws.send(json.dumps({"balance": 1}))
                bal_msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
                STARTING_BALANCE = float(bal_msg["balance"]["balance"])
                LAST_BALANCE = STARTING_BALANCE
                PROFIT_TARGET = STARTING_BALANCE * 1.25
                
                print(f"[{now_ts()}] Authorized. Starting Balance: ${STARTING_BALANCE:.2f}")
                print(f"[{now_ts()}] Profit Target: ${PROFIT_TARGET:.2f} (+25%)")
                
                await tg_send(
                    f"🚀 *Bot Started on Render*\n"
                    f"Starting Balance: ${STARTING_BALANCE:.2f}\n"
                    f"Target: ${PROFIT_TARGET:.2f} (+25%)\n"
                    f"Symbol: {SYMBOL}"
                )
                
                backoff = 1
                await trading_loop(ws)
                
        except websockets.exceptions.ConnectionClosed as e:
            print(f"[WS] Disconnected: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            print(f"[Main] Error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
    
    tg_task.cancel()
    try:
        await tg_task
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())