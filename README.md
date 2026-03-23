# visa_rescheduler

The visa_rescheduler is a bot for US VISA (usvisa-info.com) appointment rescheduling. This bot can help you reschedule your appointment to your desired time period.

## Prerequisites

- Having a US VISA appointment scheduled already.

## Installation

```
pip3 install -r requirements.txt
```

## Configuration

```
cp config.ini.example config.ini
```

## Update your config.ini file (Username, Password, Targeted Dates, Timing)
```
[PERSONAL_INFO]
; Account and current appointment info from https://ais.usvisa-info.com
USERNAME = 
PASSWORD = 
; Find SCHEDULE_ID in re-schedule page link:
; https://ais.usvisa-info.com/en-am/niv/schedule/{SCHEDULE_ID}/appointment
SCHEDULE_ID = 
; Target Period:
PRIOD_START = 2025-02-15
PRIOD_END = 2026-12-31
; Change "en-ca-tor", based on your embassy Abbreviation in embassy.py list.
YOUR_EMBASSY = en-ca-tor

[CHROMEDRIVER]
; Details for the script to control Chrome
LOCAL_USE = True
; Required when LOCAL_USE = True (local binary, no auto-download)
CHROMEDRIVER_PATH = C:\\tools\\chromedriver.exe
; Optional: HUB_ADDRESS is mandatory only when LOCAL_USE = False
HUB_ADDRESS = http://localhost:9515/wd/hub

[NOTIFICATION]
; Optional email notifications (Gmail or Outlook 365)
EMAIL_ENABLED = False
; Gmail: smtp.gmail.com | Outlook 365: smtp.office365.com
SMTP_HOST = smtp.gmail.com
SMTP_PORT = 587
EMAIL_FROM = your_email@example.com
EMAIL_TO = destination@example.com
SMTP_USERNAME = your_email@example.com
; Prefer env var SMTP_APP_PASSWORD. Set here only if needed.
SMTP_APP_PASSWORD =

[TIME]
; Time between retries/checks for available dates (seconds)
RETRY_TIME = 60
; Extra random jitter in seconds to avoid fixed poll pattern
RETRY_JITTER_SECONDS = 10
; Cooling down after WORK_LIMIT_TIME hours of work (Avoiding Ban)(hours)
WORK_LIMIT_TIME = 8
WORK_COOLDOWN_TIME = 1
; Temporary Banned (empty list): wait COOLDOWN_TIME (hours)
BAN_COOLDOWN_TIME = 0.5
; Cooldown when network/rate-limit block is detected (hours)
BLOCK_COOLDOWN_TIME = 2
; Max automatic relogin attempts when API returns HTTP 401
AUTH_RECOVERY_MAX_ATTEMPTS = 3
; Waiting seconds before relogin after HTTP 401
AUTH_RECOVERY_WAIT_SECONDS = 20
; Wait after consular reschedule before CAS reschedule (seconds)
CAS_DELAY_AFTER_CONSULAR = 5
; Max candidate dates to try in each cycle when one fails
MAX_DATES_PER_CYCLE = 3
; Max CAS dates to evaluate in each CAS cycle
MAX_CAS_DATES_PER_CYCLE = 5
; Fast path: submit directly to backend first (less UI latency)
DIRECT_SUBMIT_FIRST = True
; If direct submit is not confirmed, fallback to UI submit
DIRECT_SUBMIT_UI_FALLBACK = True
; Number of status checks after direct submit
DIRECT_STATUS_RECHECKS = 2
; Wait between status checks after direct submit (seconds)
DIRECT_STATUS_RECHECK_WAIT_SECONDS = 0.8

[LOGGING]
LEVEL = INFO
FORMAT = %(asctime)s %(levelname)s %(name)s %(message)s

```

## Running

```
python3 visa.py
```

## Reschedule Behavior

- When a target consular slot is found, the bot submits consular reschedule first.
- By default it uses a fast backend POST path (`DIRECT_SUBMIT_FIRST = True`) before UI interactions.
- Then it waits `CAS_DELAY_AFTER_CONSULAR` seconds (default 5) and tries to reschedule CAS.
- If a candidate date fails, it can try additional candidate dates in the same cycle (`MAX_DATES_PER_CYCLE`).
- If the portal/network blocks requests (`403`, `ERR_CONNECTION_REFUSED`, `ERR_EMPTY_RESPONSE`), the bot applies progressive cooldown (`BLOCK_COOLDOWN_TIME`) before retrying.
- If API calls return `HTTP 401`, the bot closes/restarts session and relogs automatically (`AUTH_RECOVERY_MAX_ATTEMPTS`, `AUTH_RECOVERY_WAIT_SECONDS`).
- For `appointment/days/*`, if `ConnectionError` persists, the bot retries up to 4 times and then restarts session automatically.
- If consular succeeds but CAS fails, result is reported as `PARTIAL_SUCCESS`.

## Email Notification Setup

1. Configure `[NOTIFICATION]` and set `EMAIL_ENABLED = True`.
2. For Gmail use `SMTP_HOST = smtp.gmail.com` and an App Password.
3. For Outlook 365 use `SMTP_HOST = smtp.office365.com` with port `587`.
4. Recommended: export the secret as env var `SMTP_APP_PASSWORD` instead of putting it in `config.ini`.
5. Run `python3 visa.py`.

## Security Policy

- The script only sends requests to `https://ais.usvisa-info.com`.
- Optional notifications are sent only through approved SMTP hosts (`smtp.gmail.com` or `smtp.office365.com`) when `EMAIL_ENABLED = True`.
- If `LOCAL_USE = False`, `HUB_ADDRESS` must point to localhost.
