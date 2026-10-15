# Запускает и сайт (server.py), и бота (bot.py) одним процессом: python start.py
import os, threading, uvicorn
import server

threading.Thread(target=lambda: uvicorn.run(server.app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), log_level="warning"), daemon=True).start()
import bot  # запускает бота (long polling)
