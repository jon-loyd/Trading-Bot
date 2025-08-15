from dotenv import load_dotenv
import os

def get_alpaca_credentials():
    load_dotenv()
    api_key = os.getenv("ALPACA_API_KEY")
    api_secret = os.getenv("ALPACA_API_SECRET")

    if not api_key or not api_secret:
        raise ValueError("Missing API credentials. Check your .env file.")
    
    return api_key, api_secret