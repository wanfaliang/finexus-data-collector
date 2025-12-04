import requests
import datetime
from datetime import timedelta

# Configuration
OUTPUT_FILENAME = "treasury_auctions.ics"
# We target the specific maturities you listed
TARGET_TERMS = ["2-Year", "5-Year", "7-Year", "10-Year", "20-Year", "30-Year"]
# Auctions generally close at 1:00 PM Eastern Time
AUCTION_TIME_HOUR = 13 
AUCTION_TIME_MINUTE = 0

def fetch_upcoming_auctions():
    """
    Fetches upcoming auction data from the official US Treasury FiscalData API.
    """
    base_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/upcoming_auctions"
    
    # Get today's date for filtering
    today = datetime.date.today().strftime("%Y-%m-%d")
    
    # Parameters: Filter for Notes and Bonds in the future, sort by date
    params = {
        "fields": "security_term,auction_date,cusip,offering_amount,security_type",
        "filter": f"auction_date:gte:{today},security_type:in:(Note,Bond)",
        "sort": "auction_date",
        "page[size]": 100  # Grab enough to cover the next few months
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json().get('data', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def create_ics_event(auction):
    """
    Formats a single auction entry into an ICS event block.
    """
    term = auction.get('security_term')
    
    # Double check this is a term we care about (2Y, 5Y, etc)
    if term not in TARGET_TERMS:
        return ""

    date_str = auction.get('auction_date') # Format YYYY-MM-DD
    cusip = auction.get('cusip')
    amount = auction.get('offering_amount', '0')
    
    # Format amount to billions for readability
    try:
        amount_billions = float(amount) / 1_000_000_000
        amount_str = f"${amount_billions:.1f}B"
    except:
        amount_str = amount

    # Create Datetime objects
    # Note: We are hardcoding to 13:00 (1 PM) assuming Eastern Time.
    # For a robust production app, timezone handling (UTC conversion) is recommended,
    # but for a personal script, floating time works well for calendar imports.
    dt_start = datetime.datetime.strptime(date_str, "%Y-%m-%d").replace(hour=AUCTION_TIME_HOUR, minute=AUCTION_TIME_MINUTE)
    dt_end = dt_start + timedelta(minutes=15) # 15 min event

    # ICS Date Format: YYYYMMDDTHHMMSS
    fmt_start = dt_start.strftime("%Y%m%dT%H%M%S")
    fmt_end = dt_end.strftime("%Y%m%dT%H%M%S")
    
    # Direct link to results page
    results_url = "https://www.treasurydirect.gov/auctions/announcements-data-results/"

    event = [
        "BEGIN:VEVENT",
        f"SUMMARY:🇺🇸 US {term} Treasury Auction",
        f"DTSTART;TZID=America/New_York:{fmt_start}",
        f"DTEND;TZID=America/New_York:{fmt_end}",
        f"DESCRIPTION:Watch for Tail/Stop-Through.\\n\\nSize: {amount_str}\\nCUSIP: {cusip}\\n\\nLINK TO RESULTS PDF: {results_url}",
        "BEGIN:VALARM",
        "TRIGGER:-PT5M", # Alert 5 minutes before
        "ACTION:DISPLAY",
        "DESCRIPTION:Auction Results Imminent",
        "END:VALARM",
        "END:VEVENT"
    ]
    return "\n".join(event)

def generate_ics_file(auctions):
    """
    Compiles all events into a standard .ics file.
    """
    ics_header = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Treasury Auction Bot//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "BEGIN:VTIMEZONE", # Basic Eastern Time definition for compatibility
        "TZID:America/New_York",
        "X-LIC-LOCATION:America/New_York",
        "BEGIN:DAYLIGHT",
        "TZOFFSETFROM:-0500",
        "TZOFFSETTO:-0400",
        "TZNAME:EDT",
        "DTSTART:19700308T020000",
        "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU",
        "END:DAYLIGHT",
        "BEGIN:STANDARD",
        "TZOFFSETFROM:-0400",
        "TZOFFSETTO:-0500",
        "TZNAME:EST",
        "DTSTART:19701101T020000",
        "RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU",
        "END:STANDARD",
        "END:VTIMEZONE"
    ]
    
    events = []
    print(f"Scanning {len(auctions)} upcoming auctions...")
    
    count = 0
    for auc in auctions:
        event_block = create_ics_event(auc)
        if event_block:
            events.append(event_block)
            count += 1
            
    ics_footer = ["END:VCALENDAR"]
    
    full_content = "\n".join(ics_header + events + ics_footer)
    
    with open(OUTPUT_FILENAME, "w") as f:
        f.write(full_content)
        
    print(f"Success! {count} auctions added to '{OUTPUT_FILENAME}'.")
    print("Import this file into Google Calendar, Outlook, or Apple Calendar.")

if __name__ == "__main__":
    data = fetch_upcoming_auctions()
    if data:
        generate_ics_file(data)
    else:
        print("No auction data found.")