import argparse
import urllib.parse

from loguru import logger
from twilio.rest import Client

from app.config import settings


def place_outbound_call(to_number: str, company_context: str | None = None) -> str:
    if not settings.public_base_url:
        raise ValueError(
            "PUBLIC_BASE_URL is empty in .env — set it to your current ngrok URL first "
            "(e.g. https://xxxx.ngrok-free.app), then re-run this script."
        )

    client = Client(settings.twilio_account_sid, settings.twilio_auth_token)

    # Reuse the exact same webhook used for inbound calls — it just returns
    # TwiML that opens the media stream, regardless of who initiated the call.
    webhook_url = f"{settings.public_base_url.rstrip('/')}/twilio/incoming"

    # Optional: pass company context through as a query param so the webhook
    # can thread it into build_pipeline_task(..., company_context=...).
    if company_context:
        webhook_url += f"?company_context={urllib.parse.quote(company_context)}"

    call = client.calls.create(
        to=to_number,
        from_=settings.twilio_phone_number,
        url=webhook_url,
    )

    logger.info(f"Outbound call placed. Twilio Call SID: {call.sid}")
    return call.sid


def main():
    parser = argparse.ArgumentParser(description="Place an outbound call through the voice agent")
    parser.add_argument("--to", required=True, help="Phone number to call, in E.164 format, e.g. +91XXXXXXXXXX")
    parser.add_argument("--company-context", default=None, help="Optional B2B record text to inject into the call")
    args = parser.parse_args()

    place_outbound_call(args.to, args.company_context)


if __name__ == "__main__":
    main()