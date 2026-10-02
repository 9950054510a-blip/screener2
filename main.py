import asyncio
import os
import requests
from dotenv import load_dotenv
from screener2_v2 import StrategyEngine
from binance_provider import BinanceLiveDataProvider
from notifier import send_email_alert

load_dotenv()

async def run_scan_cycle(engine: StrategyEngine):
    print("\n🔄 Запуск цикла сканирования рынка...")

    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    tickers = resp.json()

    sorted_tickers = sorted(tickers, key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
    universe = [t["symbol"] for t in sorted_tickers if t["symbol"].endswith("USDT")][:30]

    candidates = []
    for i, symbol in enumerate(universe, 1):
        print(f"[{i}/{len(universe)}] Сканируем {symbol:<12}", end="\r")
        try:
            res = await engine.process_symbol(symbol)
            if res:
                candidates.extend(res)
        except Exception:
            pass

    print(f"\n✅ Цикл завершен. Найдено кандидатов: {len(candidates)}")

    if candidates:
        report_lines = ["Обнаружены торговые сетапы (SCREENER #2):\n"]
        for c in candidates:
            line = f"- Символ: {c.symbol} | Сторона: {c.side} | Сетап: {c.setup_type} | Статус: {c.state}"
            print(f" {line}")
            report_lines.append(line)

        subject = f"🚨 Binance Screener #2: Найдено сетапов ({len(candidates)})"
        message = "\n".join(report_lines)
        send_email_alert(subject, message)
    else:
        print("💤 Подходящих сетапов в этом цикле не обнаружено.")

async def main():
    print("🚀 Запуск постоянного мониторинга Binance Screener #2...")
    provider = BinanceLiveDataProvider()
    engine = StrategyEngine(provider=provider)

    while True:
        try:
            await run_scan_cycle(engine)
        except Exception as e:
            print(f"⚠️ Ошибка в цикле сканирования: {e}")

        print("⏳ Ожидание следующего цикла (15 минут)... Нажмите Ctrl+C для выхода.")
        await asyncio.sleep(900)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Скринер остановлен пользователем.")
