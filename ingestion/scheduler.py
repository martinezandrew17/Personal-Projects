from apscheduler.schedulers.blocking import BlockingScheduler
from ingestion.load import main
from datetime import datetime

scheduler = BlockingScheduler()

@scheduler.scheduled_job("interval", minutes=1)
def scheduled_market_load():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running scheduled market data load...")
    main()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Market data load complete.")

if __name__ == "__main__":
    print("Starting scheduler... Press Ctrl+C to stop.")
    main()
    scheduler.start()