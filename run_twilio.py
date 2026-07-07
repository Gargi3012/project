"""Convenience launcher: `python run_twilio.py`
Equivalent to: uvicorn app.twilio_bot:app --host $HOST --port $PORT
"""

import uvicorn

from app.config import settings

if __name__ == "__main__":
    uvicorn.run("app.twilio_bot:app", host=settings.host, port=settings.port, reload=False)
