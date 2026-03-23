import configparser
import logging
import os
import random
import re
import smtplib
import subprocess
import time
import traceback
from datetime import datetime
from email.message import EmailMessage
from html import escape
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
import unicodedata
from selenium import webdriver
from selenium.common.exceptions import (
    InvalidSessionIdException,
    NoSuchWindowException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as Wait, Select

from embassy import *

config = configparser.ConfigParser()
config.read('config.ini')

LOG_LEVEL = os.getenv("LOG_LEVEL", config.get('LOGGING', 'LEVEL', fallback='INFO')).upper()
LOG_FORMAT = config.get('LOGGING', 'FORMAT', fallback='%(asctime)s %(levelname)s %(name)s %(message)s')
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format=LOG_FORMAT)
logger = logging.getLogger("visa_scheduler")

# Personal Info:
# Account and current appointment info from https://ais.usvisa-info.com
USERNAME = config['PERSONAL_INFO']['USERNAME']
PASSWORD = config['PERSONAL_INFO']['PASSWORD']
# Find SCHEDULE_ID in re-schedule page link:
# https://ais.usvisa-info.com/en-am/niv/schedule/{SCHEDULE_ID}/appointment
SCHEDULE_ID = config['PERSONAL_INFO']['SCHEDULE_ID']
# Target Period:
PRIOD_START = config['PERSONAL_INFO']['PRIOD_START']
PRIOD_END = config['PERSONAL_INFO']['PRIOD_END']
# Embassy Section:
YOUR_EMBASSY = config['PERSONAL_INFO']['YOUR_EMBASSY']
EMBASSY = Embassies[YOUR_EMBASSY][0]
FACILITY_ID = Embassies[YOUR_EMBASSY][1]
REGEX_CONTINUE = Embassies[YOUR_EMBASSY][2]

# Time Section:
minute = 60
hour = 60 * minute
# Time between steps (interactions with forms)
STEP_TIME = 0.5
# Time between retries/checks for available dates (seconds)
RETRY_TIME = config['TIME'].getfloat('RETRY_TIME')
# Cooling down after WORK_LIMIT_TIME hours of work (Avoiding Ban)
WORK_LIMIT_TIME = config['TIME'].getfloat('WORK_LIMIT_TIME')
WORK_COOLDOWN_TIME = config['TIME'].getfloat('WORK_COOLDOWN_TIME')
# Temporary Banned (empty list): wait COOLDOWN_TIME hours
BAN_COOLDOWN_TIME = config['TIME'].getfloat('BAN_COOLDOWN_TIME')
BLOCK_COOLDOWN_TIME = config['TIME'].getfloat('BLOCK_COOLDOWN_TIME', fallback=max(BAN_COOLDOWN_TIME, 0.5))
RETRY_JITTER_SECONDS = config['TIME'].getfloat('RETRY_JITTER_SECONDS', fallback=10.0)
AUTH_RECOVERY_MAX_ATTEMPTS = config['TIME'].getint('AUTH_RECOVERY_MAX_ATTEMPTS', fallback=3)
AUTH_RECOVERY_WAIT_SECONDS = config['TIME'].getfloat('AUTH_RECOVERY_WAIT_SECONDS', fallback=20.0)
# Wait before attempting CAS reschedule after consular reschedule (seconds)
CAS_DELAY_AFTER_CONSULAR = config['TIME'].getfloat('CAS_DELAY_AFTER_CONSULAR', fallback=5.0)
# Max candidate dates to try in the same cycle when reschedule fails
MAX_DATES_PER_CYCLE = config['TIME'].getint('MAX_DATES_PER_CYCLE', fallback=3)
# Max CAS dates to evaluate in one CAS attempt cycle
MAX_CAS_DATES_PER_CYCLE = config['TIME'].getint('MAX_CAS_DATES_PER_CYCLE', fallback=5)
# Fast path: submit directly to server before UI interactions
DIRECT_SUBMIT_FIRST = config['TIME'].getboolean('DIRECT_SUBMIT_FIRST', fallback=True)
# If direct submit is not confirmed, fallback to UI submit flow
DIRECT_SUBMIT_UI_FALLBACK = config['TIME'].getboolean('DIRECT_SUBMIT_UI_FALLBACK', fallback=True)
# Number of status validations after direct submit (>=1)
DIRECT_STATUS_RECHECKS = max(1, config['TIME'].getint('DIRECT_STATUS_RECHECKS', fallback=2))
# Wait between status validations after direct submit (seconds)
DIRECT_STATUS_RECHECK_WAIT_SECONDS = config['TIME'].getfloat('DIRECT_STATUS_RECHECK_WAIT_SECONDS', fallback=0.8)
# Optional network recovery hook: rotate IP via local command when connection keeps failing
IP_ROTATION_ENABLED = config.getboolean('NETWORK_RECOVERY', 'IP_ROTATION_ENABLED', fallback=False)
IP_ROTATION_COMMAND = config.get('NETWORK_RECOVERY', 'IP_ROTATION_COMMAND', fallback='').strip()
IP_ROTATION_TRIGGER_STREAK = max(
    1, config.getint('NETWORK_RECOVERY', 'IP_ROTATION_TRIGGER_STREAK', fallback=2)
)
IP_ROTATION_MAX_ATTEMPTS = max(
    1, config.getint('NETWORK_RECOVERY', 'IP_ROTATION_MAX_ATTEMPTS', fallback=3)
)
IP_ROTATION_WAIT_SECONDS = max(
    0.0, config.getfloat('NETWORK_RECOVERY', 'IP_ROTATION_WAIT_SECONDS', fallback=20.0)
)
IP_ROTATION_COMMAND_TIMEOUT_SECONDS = max(
    5.0,
    config.getfloat('NETWORK_RECOVERY', 'IP_ROTATION_COMMAND_TIMEOUT_SECONDS', fallback=120.0),
)
FULL_BROWSER_RESTART_FAILURE_STREAK = max(
    2, config['TIME'].getint('FULL_BROWSER_RESTART_FAILURE_STREAK', fallback=4)
)
FULL_BROWSER_RESTART_WAIT_SECONDS = max(
    60.0, config['TIME'].getfloat('FULL_BROWSER_RESTART_WAIT_SECONDS', fallback=300.0)
)
DEGRADED_MODE_FAILURE_STREAK = max(
    2, config['TIME'].getint('DEGRADED_MODE_FAILURE_STREAK', fallback=4)
)
DEGRADED_MODE_RECOVERY_SUCCESS_STREAK = max(
    1, config['TIME'].getint('DEGRADED_MODE_RECOVERY_SUCCESS_STREAK', fallback=3)
)
DEGRADED_MODE_WAIT_MULTIPLIER = max(
    1.0, config['TIME'].getfloat('DEGRADED_MODE_WAIT_MULTIPLIER', fallback=2.5)
)
DEGRADED_MODE_MAX_WAIT_SECONDS = max(
    60.0, config['TIME'].getfloat('DEGRADED_MODE_MAX_WAIT_SECONDS', fallback=900.0)
)
DEGRADED_MODE_MAX_DATES_PER_CYCLE = max(
    1, config['TIME'].getint('DEGRADED_MODE_MAX_DATES_PER_CYCLE', fallback=1)
)
LONG_COOLDOWN_SECONDS = max(
    300.0, config['TIME'].getfloat('LONG_COOLDOWN_SECONDS', fallback=1800.0)
)
VIEW_LIMIT_COOLDOWN_SECONDS = max(
    LONG_COOLDOWN_SECONDS,
    config['TIME'].getfloat('VIEW_LIMIT_COOLDOWN_SECONDS', fallback=7200.0),
)
LONG_COOLDOWN_HARD_RESTART_THRESHOLD = max(
    1, config['TIME'].getint('LONG_COOLDOWN_HARD_RESTART_THRESHOLD', fallback=2)
)
LONG_COOLDOWN_WINDOW_SECONDS = max(
    60.0, config['TIME'].getfloat('LONG_COOLDOWN_WINDOW_SECONDS', fallback=3600.0)
)
LONG_COOLDOWN_NETWORK_STREAK = max(
    3, config['TIME'].getint('LONG_COOLDOWN_NETWORK_STREAK', fallback=8)
)
LONG_COOLDOWN_BLOCK_STREAK = max(
    2, config['TIME'].getint('LONG_COOLDOWN_BLOCK_STREAK', fallback=3)
)
PAGE_LOAD_TIMEOUT_SECONDS = max(
    20.0, config['TIME'].getfloat('PAGE_LOAD_TIMEOUT_SECONDS', fallback=45.0)
)
SCRIPT_TIMEOUT_SECONDS = max(
    20.0, config['TIME'].getfloat('SCRIPT_TIMEOUT_SECONDS', fallback=45.0)
)
NAVIGATION_DRIVER_RESET_MAX_ATTEMPTS = max(
    1, config['TIME'].getint('NAVIGATION_DRIVER_RESET_MAX_ATTEMPTS', fallback=2)
)
BROWSER_JSON_FALLBACK_ENABLED = config['TIME'].getboolean(
    'BROWSER_JSON_FALLBACK_ENABLED', fallback=True
)
BROWSER_JSON_FALLBACK_TIMEOUT_SECONDS = max(
    10.0, config['TIME'].getfloat('BROWSER_JSON_FALLBACK_TIMEOUT_SECONDS', fallback=30.0)
)
DATE_QUERY_CACHE_TTL_SECONDS = max(
    15.0, config['TIME'].getfloat('DATE_QUERY_CACHE_TTL_SECONDS', fallback=75.0)
)
DATE_QUERY_CACHE_STALE_GRACE_SECONDS = max(
    DATE_QUERY_CACHE_TTL_SECONDS,
    config['TIME'].getfloat('DATE_QUERY_CACHE_STALE_GRACE_SECONDS', fallback=180.0),
)
DATE_ENDPOINT_RETRIES = max(
    2, config['TIME'].getint('DATE_ENDPOINT_RETRIES', fallback=3)
)
NETWORK_FAST_FAIL_COOLDOWN_SECONDS = max(
    30.0, config['TIME'].getfloat('NETWORK_FAST_FAIL_COOLDOWN_SECONDS', fallback=180.0)
)

# CHROMEDRIVER
# Details for the script to control Chrome
LOCAL_USE = config['CHROMEDRIVER'].getboolean('LOCAL_USE')
CHROMEDRIVER_PATH = config['CHROMEDRIVER'].get('CHROMEDRIVER_PATH', '').strip()
# Optional: HUB_ADDRESS is mandatory only when LOCAL_USE = False
HUB_ADDRESS = config['CHROMEDRIVER']['HUB_ADDRESS']

# Notification (optional)
EMAIL_ENABLED = config.getboolean('NOTIFICATION', 'EMAIL_ENABLED', fallback=False)
SMTP_HOST = config.get('NOTIFICATION', 'SMTP_HOST', fallback='smtp.gmail.com').strip()
SMTP_PORT = config.getint('NOTIFICATION', 'SMTP_PORT', fallback=587)
EMAIL_FROM = config.get('NOTIFICATION', 'EMAIL_FROM', fallback='').strip()
EMAIL_TO = config.get('NOTIFICATION', 'EMAIL_TO', fallback='').strip()
SMTP_USERNAME = config.get('NOTIFICATION', 'SMTP_USERNAME', fallback='').strip()
SMTP_APP_PASSWORD = (
        os.getenv('SMTP_APP_PASSWORD', '').strip()
        or config.get('NOTIFICATION', 'SMTP_APP_PASSWORD', fallback='').strip()
).strip("\"'")

SIGN_IN_LINK = f"https://ais.usvisa-info.com/{EMBASSY}/niv/users/sign_in"
APPOINTMENT_URL = f"https://ais.usvisa-info.com/{EMBASSY}/niv/schedule/{SCHEDULE_ID}/appointment"
SCHEDULE_STATUS_URL = f"https://ais.usvisa-info.com/{EMBASSY}/niv/schedule/{SCHEDULE_ID}"
SIGN_OUT_LINK = f"https://ais.usvisa-info.com/{EMBASSY}/niv/users/sign_out"
ALLOWED_AIS_HOST = "ais.usvisa-info.com"
ALLOWED_REMOTE_DRIVER_HOSTS = {"localhost", "127.0.0.1", "::1"}
ALLOWED_SMTP_PORTS_BY_HOST = {
    "smtp.gmail.com": {465, 587},
    "smtp.office365.com": {587},
}
NOTIFICATION_META = {
    "SUCCESS": {
        "badge": "REAGENDADO",
        "headline": "Reagendado completado con exito",
        "summary": "Se confirmo reagendado de cita consular y cita CAS.",
        "action": "Busqueda de cupo, envio de solicitud consular y ajuste CAS completados.",
        "color": "#1b5e20",
        "soft": "#e8f5e9",
    },
    "PARTIAL_SUCCESS": {
        "badge": "PARCIAL",
        "headline": "Reagendado parcial",
        "summary": "La cita consular se reagendo, pero CAS requiere revision manual.",
        "action": "Se envio solicitud consular y luego intento CAS sin confirmacion final.",
        "color": "#8a4b08",
        "soft": "#fff3e0",
    },
    "FAIL": {
        "badge": "SIN CAMBIOS",
        "headline": "No se pudo reagendar",
        "summary": "La solicitud no fue aceptada por el portal.",
        "action": "Se detecto fecha objetivo, pero el reagendado no fue confirmado.",
        "color": "#b71c1c",
        "soft": "#ffebee",
    },
    "EXCEPTION": {
        "badge": "ERROR",
        "headline": "Ejecucion detenida por excepcion",
        "summary": "El proceso termino por un error inesperado.",
        "action": "Se interrumpio el flujo y se registro detalle tecnico para diagnostico.",
        "color": "#b71c1c",
        "soft": "#ffebee",
    },
    "BAN": {
        "badge": "PAUSA",
        "headline": "Portal sin disponibilidad temporal",
        "summary": "El portal no retorno fechas y se activo enfriamiento.",
        "action": "Se cerro sesion y se aplico pausa segun configuracion.",
        "color": "#6d4c41",
        "soft": "#efebe9",
    },
    "REST": {
        "badge": "DESCANSO",
        "headline": "Pausa operativa programada",
        "summary": "Se alcanzo el limite de trabajo continuo.",
        "action": "Se detuvo temporalmente para reducir riesgo de bloqueo.",
        "color": "#0d47a1",
        "soft": "#e3f2fd",
    },
    "AUTH": {
        "badge": "SESION",
        "headline": "Recuperacion de sesion",
        "summary": "Se detecto sesion invalida y se iniciara reautenticacion.",
        "action": "Cierre y reapertura de sesion automatica para continuar ejecucion.",
        "color": "#1565c0",
        "soft": "#e3f2fd",
    },
    "TENTATIVE_DATE": {
        "badge": "CANDIDATA",
        "headline": "Fecha tentativa detectada",
        "summary": "Se encontro una fecha en rango objetivo.",
        "action": "Se notifico el hallazgo y el bot continua con el intento de reagendamiento.",
        "color": "#2e7d32",
        "soft": "#e8f5e9",
    },
}
USER_AGENT_CACHE = ""
API_RESULT_CACHE = {}
API_COOLDOWN_STATE = {
    "dates_until": 0.0,
    "dates_reason": "",
}
LAST_DATE_QUERY_SOURCE = "network"

JS_SCRIPT = ("var req = new XMLHttpRequest();"
             f"req.open('GET', '%s', false);"
             "req.setRequestHeader('Accept', 'application/json, text/javascript, */*; q=0.01');"
             "req.setRequestHeader('X-Requested-With', 'XMLHttpRequest');"
             f"req.setRequestHeader('Cookie', '_yatri_session=%s');"
             "req.send(null);"
             "return req.responseText;")


def build_days_url(facility_id):
    return (
        f"https://ais.usvisa-info.com/{EMBASSY}/niv/schedule/{SCHEDULE_ID}/appointment/days/"
        f"{facility_id}.json"
    )


def build_times_url(facility_id, date):
    return (
        f"https://ais.usvisa-info.com/{EMBASSY}/niv/schedule/{SCHEDULE_ID}/appointment/times/"
        f"{facility_id}.json"
    )


def deduplicate_param_pairs(param_pairs):
    deduped = []
    seen = set()
    for key, value in param_pairs:
        normalized_pair = (str(key), str(value))
        if normalized_pair in seen:
            continue
        seen.add(normalized_pair)
        deduped.append((str(key), str(value)))
    return deduped


def get_current_applicant_params():
    applicant_params = []
    try:
        current_url = driver.current_url or ""
        query_pairs = parse_qsl(urlparse(current_url).query, keep_blank_values=True)
        for key, value in query_pairs:
            if key.startswith("applicants"):
                applicant_params.append((key, value))
    except Exception:
        pass
    try:
        form = get_reschedule_form()
        applicant_inputs = form.find_elements(
            By.XPATH,
            ".//input[@name='applicants[]' or starts-with(@name, 'applicants[')]",
        )
        for element in applicant_inputs:
            value = (element.get_attribute("value") or "").strip()
            if value:
                applicant_params.append(("applicants[]", value))
    except Exception:
        pass
    return deduplicate_param_pairs(applicant_params)


def build_request_params(base_pairs):
    pairs = list(base_pairs or [])
    pairs.extend(get_current_applicant_params())
    return deduplicate_param_pairs(pairs)


def append_query_params_to_url(url, param_pairs):
    if not param_pairs:
        return url
    parsed = urlparse(url)
    existing_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    merged_pairs = deduplicate_param_pairs(existing_pairs + list(param_pairs))
    new_query = urlencode(merged_pairs, doseq=True)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment,
        )
    )


DATE_URL = build_days_url(FACILITY_ID)
TIME_URL = build_times_url(FACILITY_ID, "%s")
LOCK_FILE = os.path.join(os.getcwd(), ".visa_scheduler.lock")


def validate_ais_url(url):
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != ALLOWED_AIS_HOST:
        raise ValueError(f"Blocked URL outside allowed host: {url}")


def is_browser_error_page():
    try:
        current_url = driver.current_url or ""
    except Exception:
        current_url = ""
    if current_url.startswith("chrome-error://"):
        return True
    try:
        source = driver.page_source or ""
    except Exception:
        source = ""
    normalized = normalize_lookup_text(source)
    markers = [
        "err_empty_response",
        "err_connection_reset",
        "err_connection_timed_out",
        "this page isnt working",
        "esta pagina no funciona",
        "no ha enviado ningun dato",
    ]
    return any(marker in normalized for marker in markers)


def is_forbidden_or_block_page():
    try:
        source = driver.page_source or ""
        title = driver.title or ""
    except Exception:
        source = ""
        title = ""
    normalized = normalize_lookup_text(f"{title}\n{source}")
    markers = [
        "error 403",
        "http 403",
        "forbidden",
        "access denied",
        "acceso denegado",
    ]
    return any(marker in normalized for marker in markers)


def navigate_ais_page(url, attempts=4, wait_seconds=1.5):
    validate_ais_url(url)
    last_error = ""
    reset_attempts = 0
    for attempt in range(1, attempts + 1):
        try:
            driver.get(url)
            Wait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            if not is_browser_error_page() and not is_forbidden_or_block_page():
                return
            title = (driver.title or "").strip()
            excerpt = strip_html_tags(driver.page_source or "")[:180]
            last_error = f"{title} | {excerpt}"
            log_warning(
                f"Navegacion fallida ({attempt}/{attempts}) a {url}. "
                "Se detecto pagina de error o bloqueo temporal."
            )
        except WebDriverException as exc:
            error_summary = extract_webdriver_error_summary(exc)
            last_error = f"{exc.__class__.__name__}: {error_summary}"
            log_warning(
                f"Navegacion fallida ({attempt}/{attempts}) a {url}. "
                f"Error WebDriver: {exc.__class__.__name__}. Detalle: {error_summary}"
            )
            if is_connection_refused_failure(last_error):
                log_warning(
                    f"Fast-fail de navegacion a {url} por rechazo de conexion del portal."
                )
                break
            should_reset_driver = (
                reset_attempts < NAVIGATION_DRIVER_RESET_MAX_ATTEMPTS
                and is_driver_session_failure(last_error)
            )
            if should_reset_driver and attempt < attempts:
                reset_attempts += 1
                try:
                    reset_webdriver_session(reason=f"fallo de navegacion hacia {url}")
                    log_warning(
                        f"Se recrea la sesion WebDriver tras fallo de navegacion "
                        f"({reset_attempts}/{NAVIGATION_DRIVER_RESET_MAX_ATTEMPTS})."
                    )
                except Exception as reset_exc:
                    reset_summary = summarize_error_text(f"{reset_exc.__class__.__name__}: {reset_exc}")
                    log_warning(
                        "No se pudo recrear la sesion WebDriver tras fallo de navegacion. "
                        f"Detalle: {reset_summary}"
                    )
        except Exception as exc:
            last_error = f"{exc.__class__.__name__}: {exc}"
            log_warning(
                f"Navegacion fallida ({attempt}/{attempts}) a {url}. "
                f"Error inesperado: {exc.__class__.__name__}"
            )
            if is_connection_refused_failure(last_error):
                log_warning(
                    f"Fast-fail de navegacion a {url} por rechazo de conexion del portal."
                )
                break
        if attempt < attempts:
            time.sleep(wait_seconds * attempt)
    raise RuntimeError(f"No se pudo cargar la URL despues de {attempts} intentos: {url}. Detalle: {last_error}")


def is_transient_block_failure(error_text):
    if is_view_limit_block_failure(error_text):
        return True
    normalized = normalize_lookup_text(error_text)
    markers = [
        "http 403",
        "error 403",
        "forbidden",
        "access denied",
        "acceso denegado",
        "too many requests",
        "429",
        "service unavailable",
        "temporarily unavailable",
    ]
    return any(marker in normalized for marker in markers)


def is_view_limit_block_failure(error_text):
    normalized = normalize_lookup_text(error_text)
    markers = [
        "you have exceeded the limit for viewing this page",
        "you are approaching the maximum number of times you may view this page",
        "maximum number of times you may view this page",
        "your limit will be reset as of tomorrow",
        "account has been frozen",
        "suspicious activity",
        "excedido el limite de visualizacion",
        "limite de veces que puede ver esta pagina",
    ]
    return any(marker in normalized for marker in markers)


def has_view_limit_banner():
    try:
        source = strip_html_tags(driver.page_source or "")
    except Exception:
        source = ""
    return is_view_limit_block_failure(source)


def is_transient_network_failure(error_text):
    normalized = normalize_lookup_text(error_text)
    markers = [
        "invalidjsonresponse",
        "jsondecodeerror",
        "expecting value",
        "content-type=text/html",
        "connectionerror",
        "proxyerror",
        "newconnectionerror",
        "httpsconnectionpool",
        "max retries exceeded",
        "failed to establish a new connection",
        "connection refused",
        "winerror 10061",
        "err_connection_refused",
        "err_connection_reset",
        "err_connection_timed_out",
        "err_empty_response",
        "read timed out",
        "connect timeout",
        "temporary failure in name resolution",
        "name or service not known",
        "network is unreachable",
        "no se puede establecer una conexion",
        "denego expresamente dicha conexion",
        "se ha agotado el tiempo de espera",
    ]
    return any(marker in normalized for marker in markers)


def is_auth_session_failure(error_text):
    normalized = normalize_lookup_text(error_text)
    markers = [
        "http 401",
        "unauthorized",
        "authsessionerror",
        "session invalid",
        "sesion invalida",
    ]
    return any(marker in normalized for marker in markers)


def is_driver_session_failure(error_text):
    normalized = normalize_lookup_text(error_text)
    markers = [
        "invalidsessionidexception",
        "nosuchwindowexception",
        "no such window",
        "target window already closed",
        "web view not found",
        "session deleted because of page crash",
        "disconnected",
        "chrome not reachable",
        "invalid session id",
    ]
    return any(marker in normalized for marker in markers)


def is_page_unresponsive_failure(error_text):
    normalized = normalize_lookup_text(error_text)
    markers = [
        "timeout",
        "timed out receiving message from renderer",
        "page crash",
        "chrome-error://",
        "err_empty_response",
        "err_connection_timed_out",
        "err_connection_reset",
        "err_connection_refused",
        "unable to locate host",
        "dns_probe_finished",
        "net::",
    ]
    if any(marker in normalized for marker in markers):
        return True
    return is_browser_error_page() or is_transient_network_failure(error_text)


def is_connection_refused_failure(error_text):
    normalized = normalize_lookup_text(error_text)
    markers = [
        "err_connection_refused",
        "connection refused",
        "winerror 10061",
        "failed to fetch",
    ]
    return any(marker in normalized for marker in markers)


def summarize_error_text(value, limit=220):
    compact = re.sub(r"\s+", " ", str(value or "")).strip()
    if not compact:
        return "N/D"
    if len(compact) <= limit:
        return compact
    return compact[: max(20, limit - 3)] + "..."


def extract_webdriver_error_summary(exc):
    raw_message = getattr(exc, "msg", "") or str(exc) or exc.__class__.__name__
    return summarize_error_text(raw_message)


def is_webdriver_session_alive():
    try:
        _ = driver.current_url or ""
        _ = driver.title or ""
        return True
    except (InvalidSessionIdException, NoSuchWindowException, WebDriverException):
        return False


def get_cache_entry(cache_bucket, cache_key):
    return API_RESULT_CACHE.get((cache_bucket, cache_key))


def get_cached_result(cache_bucket, cache_key, max_age_seconds):
    entry = get_cache_entry(cache_bucket, cache_key)
    if not entry:
        return None
    age_seconds = time.time() - entry["stored_at"]
    if age_seconds > max_age_seconds:
        return None
    return entry["data"], age_seconds


def store_cached_result(cache_bucket, cache_key, data):
    API_RESULT_CACHE[(cache_bucket, cache_key)] = {
        "stored_at": time.time(),
        "data": data,
    }


def clear_api_cooldown(state_key):
    API_COOLDOWN_STATE[f"{state_key}_until"] = 0.0
    API_COOLDOWN_STATE[f"{state_key}_reason"] = ""


def activate_api_cooldown(state_key, reason, cooldown_seconds):
    until_key = f"{state_key}_until"
    reason_key = f"{state_key}_reason"
    current_until = float(API_COOLDOWN_STATE.get(until_key) or 0.0)
    next_until = max(current_until, time.time() + max(1.0, float(cooldown_seconds)))
    API_COOLDOWN_STATE[until_key] = next_until
    API_COOLDOWN_STATE[reason_key] = summarize_error_text(reason)


def get_api_cooldown_remaining(state_key):
    until_value = float(API_COOLDOWN_STATE.get(f"{state_key}_until") or 0.0)
    remaining = until_value - time.time()
    if remaining <= 0:
        clear_api_cooldown(state_key)
        return 0.0, ""
    return remaining, API_COOLDOWN_STATE.get(f"{state_key}_reason") or ""


def compute_retry_wait_seconds():
    jitter = random.uniform(0, max(0.0, RETRY_JITTER_SECONDS))
    return max(1.0, RETRY_TIME + jitter)


def compute_adaptive_wait_seconds(base_wait_seconds, degraded_mode=False, failure_streak=0):
    wait_seconds = max(1.0, float(base_wait_seconds))
    if degraded_mode:
        wait_seconds = min(
            DEGRADED_MODE_MAX_WAIT_SECONDS,
            wait_seconds * DEGRADED_MODE_WAIT_MULTIPLIER + min(120.0, float(failure_streak) * 10.0),
        )
    return wait_seconds


def maybe_rotate_ip_on_network_failure(network_streak, rotation_attempts, context_label):
    if not IP_ROTATION_ENABLED:
        return rotation_attempts
    if network_streak < IP_ROTATION_TRIGGER_STREAK:
        return rotation_attempts
    if rotation_attempts >= IP_ROTATION_MAX_ATTEMPTS:
        log_warning(
            "Rotacion de IP omitida: maximo de intentos alcanzado "
            f"({IP_ROTATION_MAX_ATTEMPTS}) en este ciclo de ejecucion."
        )
        return rotation_attempts
    if not IP_ROTATION_COMMAND:
        log_warning(
            "Rotacion de IP habilitada pero IP_ROTATION_COMMAND esta vacio. "
            "No se puede ejecutar cambio de IP."
        )
        return rotation_attempts

    next_attempt = rotation_attempts + 1
    log_warning(
        "Fallo de red persistente. "
        f"Intentando rotacion de IP {next_attempt}/{IP_ROTATION_MAX_ATTEMPTS} "
        f"en contexto '{context_label}'."
    )
    try:
        result = subprocess.run(
            IP_ROTATION_COMMAND,
            shell=True,
            check=False,
            capture_output=True,
            text=True,
            timeout=IP_ROTATION_COMMAND_TIMEOUT_SECONDS,
        )
        if result.returncode == 0:
            log_info(f"Rotacion de IP ejecutada correctamente (exit={result.returncode}).")
        else:
            log_warning(f"Comando de rotacion de IP finalizo con exit={result.returncode}.")
            stderr_excerpt = truncate_text((result.stderr or "").strip(), 180)
            stdout_excerpt = truncate_text((result.stdout or "").strip(), 180)
            if stderr_excerpt:
                log_warning(f"stderr rotacion IP: {stderr_excerpt}")
            elif stdout_excerpt:
                log_warning(f"stdout rotacion IP: {stdout_excerpt}")
    except Exception as exc:
        log_warning(
            "No se pudo ejecutar rotacion de IP: "
            f"{exc.__class__.__name__}: {sanitize_text(exc)}"
        )

    if IP_ROTATION_WAIT_SECONDS > 0:
        log_info(f"Espera post-rotacion de IP: {IP_ROTATION_WAIT_SECONDS:.1f} segundos.")
        time.sleep(IP_ROTATION_WAIT_SECONDS)

    return next_attempt


def validate_local_hub(url):
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("HUB_ADDRESS must use http/https")
    if parsed.hostname not in ALLOWED_REMOTE_DRIVER_HOSTS:
        raise ValueError("HUB_ADDRESS must point to localhost/127.0.0.1/::1")


def validate_local_driver(path):
    if not path:
        raise ValueError("CHROMEDRIVER_PATH is required when LOCAL_USE=True")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"CHROMEDRIVER_PATH not found: {path}")


def validate_notification_settings():
    if not EMAIL_ENABLED:
        return
    smtp_host = SMTP_HOST.lower()
    if smtp_host not in ALLOWED_SMTP_PORTS_BY_HOST:
        allowed_hosts = ", ".join(sorted(ALLOWED_SMTP_PORTS_BY_HOST.keys()))
        raise ValueError(f"SMTP_HOST must be one of: {allowed_hosts}")
    if SMTP_PORT not in ALLOWED_SMTP_PORTS_BY_HOST[smtp_host]:
        allowed_ports = ", ".join(str(p) for p in sorted(ALLOWED_SMTP_PORTS_BY_HOST[smtp_host]))
        raise ValueError(f"SMTP_PORT for {smtp_host} must be one of: {allowed_ports}")
    if not EMAIL_FROM or not EMAIL_TO or not SMTP_USERNAME or not SMTP_APP_PASSWORD:
        raise ValueError(
            "EMAIL_FROM, EMAIL_TO, SMTP_USERNAME and SMTP_APP_PASSWORD are required when EMAIL_ENABLED=True")


def sanitize_text(value):
    text = str(value)
    sensitive_values = [USERNAME, PASSWORD, SCHEDULE_ID, SMTP_USERNAME, SMTP_APP_PASSWORD]
    for secret in sensitive_values:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    return text


def log_info(message):
    logger.info(sanitize_text(message))


def log_warning(message):
    logger.warning(sanitize_text(message))


def log_error(message):
    logger.error(sanitize_text(message))


class AuthSessionError(RuntimeError):
    pass


class SessionRestartRequiredError(RuntimeError):
    pass


def build_request_url(url, params=None):
    validate_ais_url(url)
    return append_query_params_to_url(url, params or [])


def ensure_browser_fetch_context():
    try:
        current_url = driver.current_url or ""
    except Exception:
        current_url = ""
    expected_prefix = f"https://{ALLOWED_AIS_HOST}/"
    if current_url.startswith(expected_prefix) and not current_url.startswith("chrome-error://"):
        return
    try:
        ensure_appointment_page_ready(force_navigate=False)
    except Exception:
        navigate_ais_page(APPOINTMENT_URL, attempts=2)


def fetch_json_via_browser(url, params=None, timeout=30):
    request_url = build_request_url(url, params=params)
    ensure_browser_fetch_context()
    try:
        driver.set_script_timeout(max(SCRIPT_TIMEOUT_SECONDS, float(timeout) + 5.0))
    except Exception:
        pass
    fetch_script = """
const requestUrl = arguments[0];
const timeoutMs = arguments[1];
const callback = arguments[arguments.length - 1];
const headers = {
  "Accept": "application/json, text/javascript, */*; q=0.01",
  "X-Requested-With": "XMLHttpRequest"
};
let settled = false;
const finish = (payload) => {
  if (settled) {
    return;
  }
  settled = true;
  callback(payload);
};
const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
const timer = setTimeout(() => {
  if (controller) {
    controller.abort();
    return;
  }
  finish({ ok: false, errorName: "TimeoutError", errorMessage: "Browser fetch timeout" });
}, timeoutMs);

fetch(requestUrl, {
  method: "GET",
  credentials: "same-origin",
  headers,
  signal: controller ? controller.signal : undefined
})
  .then(async (response) => {
    const text = await response.text();
    let data = null;
    let parseError = "";
    if (text) {
      try {
        data = JSON.parse(text);
      } catch (error) {
        parseError = (error && error.message) ? error.message : String(error);
      }
    }
    finish({
      ok: true,
      status: response.status,
      contentType: response.headers.get("content-type") || "",
      data,
      parseError,
      bodyExcerpt: text.slice(0, 500)
    });
  })
  .catch((error) => {
    finish({
      ok: false,
      errorName: (error && error.name) ? error.name : "Error",
      errorMessage: (error && error.message) ? error.message : String(error)
    });
  })
  .finally(() => clearTimeout(timer));
"""
    try:
        payload = driver.execute_async_script(fetch_script, request_url, int(max(1000, timeout * 1000)))
    except AuthSessionError:
        raise
    except (InvalidSessionIdException, NoSuchWindowException) as exc:
        raise RuntimeError(
            f"Browser JSON fallback no disponible: {exc.__class__.__name__}"
        ) from exc
    except TimeoutException as exc:
        raise RuntimeError(
            f"Browser JSON fallback agoto tiempo de espera: {extract_webdriver_error_summary(exc)}"
        ) from exc
    except WebDriverException as exc:
        raise RuntimeError(
            "Browser JSON fallback fallo por WebDriver: "
            f"{extract_webdriver_error_summary(exc)}"
        ) from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Browser JSON fallback retorno una respuesta invalida.")
    if not payload.get("ok"):
        error_name = payload.get("errorName") or "Error"
        error_message = summarize_error_text(payload.get("errorMessage"))
        raise RuntimeError(f"Browser JSON fallback error {error_name}: {error_message}")

    status = int(payload.get("status") or 0)
    body_excerpt = strip_html_tags(payload.get("bodyExcerpt") or "")[:180]
    if status == 401:
        raise AuthSessionError(f"HTTP 401 via browser fetch for {request_url}")
    if status != 200:
        raise RuntimeError(
            f"HTTP {status} via browser fetch for {request_url}. "
            f"Body={body_excerpt or 'N/D'}"
        )

    parse_error = summarize_error_text(payload.get("parseError"))
    if payload.get("data") is None:
        raise RuntimeError(
            "Browser JSON fallback recibio payload no JSON. "
            f"Content-Type={(payload.get('contentType') or 'N/D').strip()} | "
            f"Parse={parse_error} | Body={body_excerpt or 'N/D'}"
        )
    return payload["data"]


def request_get_json_with_retry(
        url,
        headers,
        params=None,
        retries=3,
        timeout=30,
        restart_session_on_connection_error=False,
):
    last_error = ""
    delay_seconds = 1.0
    browser_fallback_used = False
    transient_statuses = {408, 425, 429, 500, 502, 503, 504}
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as exc:
                    content_type = (response.headers.get("Content-Type") or "").strip()
                    body_excerpt = strip_html_tags(response.text or "")[:180]
                    last_error = (
                        "InvalidJSONResponse: "
                        f"{exc.__class__.__name__}: {exc}. "
                        f"Content-Type={content_type or 'N/D'}. "
                        f"Body={body_excerpt or 'N/D'}"
                    )
                    if not browser_fallback_used and BROWSER_JSON_FALLBACK_ENABLED:
                        try:
                            data = fetch_json_via_browser(
                                url,
                                params=params,
                                timeout=max(timeout, BROWSER_JSON_FALLBACK_TIMEOUT_SECONDS),
                            )
                            log_info(f"GET browser fallback succeeded for {url}")
                            return data
                        except AuthSessionError:
                            raise
                        except Exception as browser_exc:
                            browser_fallback_used = True
                            fallback_summary = summarize_error_text(
                                f"{browser_exc.__class__.__name__}: {browser_exc}"
                            )
                            last_error = f"{last_error} | BrowserFallback={fallback_summary}"
                            log_warning(
                                f"Browser JSON fallback failed for {url} after invalid JSON. "
                                f"Detalle: {fallback_summary}"
                            )
                    if attempt >= retries:
                        break
                    log_warning(
                        f"GET retry {attempt}/{retries} for {url} due to invalid JSON payload "
                        f"(content-type={content_type or 'N/D'})."
                    )
                    time.sleep(delay_seconds)
                    delay_seconds *= 2
                    continue
            if response.status_code == 401:
                raise AuthSessionError(f"HTTP 401 for {url}")
            if response.status_code in transient_statuses and attempt < retries:
                last_error = f"HTTP {response.status_code}"
                log_warning(
                    f"GET retry {attempt}/{retries} for {url} due to transient status {response.status_code}"
                )
            else:
                raise RuntimeError(f"HTTP {response.status_code}")
        except AuthSessionError:
            raise
        except Exception as exc:
            last_error = f"{exc.__class__.__name__}: {exc}"
            should_restart_session = (
                    restart_session_on_connection_error
                    and isinstance(exc, requests.exceptions.ConnectionError)
            )
            should_try_browser_fallback = (
                BROWSER_JSON_FALLBACK_ENABLED
                and not browser_fallback_used
                and isinstance(exc, requests.exceptions.RequestException)
            )
            if should_try_browser_fallback:
                try:
                    data = fetch_json_via_browser(
                        url,
                        params=params,
                        timeout=max(timeout, BROWSER_JSON_FALLBACK_TIMEOUT_SECONDS),
                    )
                    log_info(f"GET browser fallback succeeded for {url}")
                    return data
                except AuthSessionError:
                    raise
                except Exception as browser_exc:
                    browser_fallback_used = True
                    fallback_summary = summarize_error_text(
                        f"{browser_exc.__class__.__name__}: {browser_exc}"
                    )
                    last_error = f"{last_error} | BrowserFallback={fallback_summary}"
                    log_warning(
                        f"Browser JSON fallback failed for {url}. Detalle: {fallback_summary}"
                    )
            if attempt >= retries:
                if should_restart_session:
                    raise SessionRestartRequiredError(
                        f"ConnectionError persisted after {retries} retries for {url}. Session restart required."
                    ) from exc
                break
            log_warning(
                f"GET retry {attempt}/{retries} for {url} after error {exc.__class__.__name__}"
            )
        time.sleep(delay_seconds)
        delay_seconds *= 2
    raise RuntimeError(f"GET request failed after {retries} retries for {url}. Last error: {last_error}")


def request_post_with_retry(url, headers, data=None, retries=3, timeout=45):
    last_error = ""
    delay_seconds = 1.0
    transient_statuses = {408, 425, 429, 500, 502, 503, 504}
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, headers=headers, data=data, timeout=timeout, allow_redirects=True)
            if response.status_code in {200, 302}:
                return response
            if response.status_code == 401:
                raise AuthSessionError(f"HTTP 401 for {url}")
            if response.status_code in transient_statuses and attempt < retries:
                last_error = f"HTTP {response.status_code}"
                log_warning(
                    f"POST retry {attempt}/{retries} for {url} due to transient status {response.status_code}"
                )
            else:
                raise RuntimeError(f"HTTP {response.status_code}")
        except AuthSessionError:
            raise
        except Exception as exc:
            last_error = f"{exc.__class__.__name__}: {exc}"
            if attempt >= retries:
                break
            log_warning(
                f"POST retry {attempt}/{retries} for {url} after error {exc.__class__.__name__}"
            )
        time.sleep(delay_seconds)
        delay_seconds *= 2
    raise RuntimeError(f"POST request failed after {retries} retries for {url}. Last error: {last_error}")


def strip_html_tags(value):
    text = re.sub(r"<[^>]+>", " ", value or "")
    return re.sub(r"\s+", " ", text).strip()


def extract_response_message(html):
    patterns = [
        r'<div[^>]*class="[^"]*alert[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*id="error_explanation"[^>]*>(.*?)</div>',
        r'<p[^>]*class="[^"]*error[^"]*"[^>]*>(.*?)</p>',
    ]
    for pattern in patterns:
        matches = re.findall(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        for match in matches:
            cleaned = strip_html_tags(match)
            if cleaned:
                return cleaned[:240]
    return ""


def is_schedule_success(html):
    success_class = re.search(
        r'class="[^"]*(alert-success|flash-success|notice-success|success)[^"]*"',
        html or "",
        flags=re.IGNORECASE,
    )
    if success_class:
        return True
    markers = [
        "Successfully Scheduled",
        "ha sido reprogramada",
        "se ha reprogramado",
        "cita ha sido programada",
        "appointment has been scheduled",
    ]
    text = (html or "").lower()
    return any(marker.lower() in text for marker in markers)


def build_date_candidates(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    es_months = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
        5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
        9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
    }
    en_months = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December",
    }
    day = date_obj.day
    month_es = es_months[date_obj.month]
    month_en = en_months[date_obj.month]
    year = date_obj.year
    return {
        date_str,
        f"{day} {month_es}, {year}",
        f"{day} {month_es} de {year}",
        f"{month_en} {day}, {year}",
        f"{day} {month_en}, {year}",
    }


def parse_status_sections(raw_text):
    normalized = re.sub(r"\s+", " ", raw_text or "").strip()
    location_boundary = r"Ubicaci\w+\s+de\s+entrega"
    consular_match = re.search(
        rf"Cita\s+Consular:\s*(.*?)\s*(Cita\s+CAS:|{location_boundary}|Cuenta\s+de\s+usuario)",
        normalized,
        flags=re.IGNORECASE,
    )
    cas_match = re.search(
        rf"Cita\s+CAS:\s*(.*?)\s*({location_boundary}|Cuenta\s+de\s+usuario)",
        normalized,
        flags=re.IGNORECASE,
    )
    consular_text = consular_match.group(1).strip() if consular_match else ""
    cas_text = cas_match.group(1).strip() if cas_match else ""
    return consular_text, cas_text, normalized


def get_status_sections(headers):
    # Primary path: HTTP request (fast and avoids opening extra browser tabs).
    try:
        response = requests.get(SCHEDULE_STATUS_URL, headers=headers, timeout=30)
        if response.status_code == 200:
            raw_text = strip_html_tags(response.text)
            consular_text, cas_text, normalized = parse_status_sections(raw_text)
            if consular_text or cas_text:
                return consular_text, cas_text, normalized
    except Exception:
        pass

    # Fallback path: Selenium on current tab (no new windows).
    original_url = driver.current_url
    try:
        navigate_ais_page(SCHEDULE_STATUS_URL, attempts=3)
        Wait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        body_text = driver.find_element(By.TAG_NAME, "body").text
        consular_text, cas_text, normalized = parse_status_sections(body_text)
        if consular_text or cas_text:
            return consular_text, cas_text, normalized
    except Exception:
        pass
    finally:
        try:
            if original_url:
                navigate_ais_page(original_url, attempts=2)
        except Exception:
            pass
    return "", "", ""


def status_section_has_target(section_text, target_date):
    if not section_text:
        return False
    section = section_text.lower()
    candidates = {candidate.lower() for candidate in build_date_candidates(target_date)}
    return any(candidate in section for candidate in candidates)


def validate_post_reschedule(headers, expected_consular_date=None, expected_cas_date=None):
    consular_text, cas_text, normalized_text = get_status_sections(headers)
    consular_ok = True
    cas_ok = True
    if expected_consular_date:
        consular_ok = status_section_has_target(consular_text, expected_consular_date)
    if expected_cas_date:
        cas_ok = status_section_has_target(cas_text, expected_cas_date)
    summary = (
        "Validacion posterior estado actual -> "
        f"Consular: {consular_text or 'N/D'} | CAS: {cas_text or 'N/D'}"
    )
    if not consular_text and not cas_text:
        summary = "Validacion posterior: no fue posible leer seccion de estado actual."
        if normalized_text:
            summary += f" Extracto: {normalized_text[:220]}"
    return consular_ok and cas_ok, summary


def truncate_text(value, max_length=220):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_length:
        return text
    return text[: max(0, max_length - 3)].rstrip() + "..."


def build_compact_exception_details(safe_msg):
    lines = [line.strip() for line in str(safe_msg or "").splitlines() if line.strip()]
    if not lines:
        return ["Se produjo una excepcion inesperada."]

    primary_line = ""
    for line in lines:
        normalized = line.lower()
        if normalized.startswith("traceback") or normalized.startswith("file "):
            continue
        if line.startswith("^"):
            continue
        primary_line = line
        break
    if not primary_line:
        primary_line = lines[0]

    details = []
    parsed = re.search(
        r"(se detuvo el ciclo por excepcion|startup failed)\s*:\s*([A-Za-z_][\w]*)\s*:\s*(.+)$",
        primary_line,
        flags=re.IGNORECASE,
    )
    if parsed:
        error_type = parsed.group(2)
        error_message = parsed.group(3).strip()
        if "Last error:" in error_message:
            head, last_error = error_message.split("Last error:", 1)
            details.append(f"Error: {error_type}: {truncate_text(head.strip(), 180)}")
            details.append(f"Causa principal: {truncate_text(last_error.strip(), 180)}")
        else:
            details.append(f"Error: {error_type}: {truncate_text(error_message, 220)}")
    else:
        details.append(f"Error: {truncate_text(primary_line, 220)}")

    details.append("Detalle tecnico completo disponible en el archivo de log diario.")
    return details


def build_notification_details(key, safe_msg):
    if key == "EXCEPTION":
        return build_compact_exception_details(safe_msg)
    details = [line.strip() for line in str(safe_msg or "").splitlines() if line.strip()]
    if not details:
        details = [str(safe_msg or "")]
    return details


def build_notification_payload(title, msg):
    safe_title = sanitize_text(title)
    safe_msg = sanitize_text(msg)
    key = safe_title.upper()
    meta = NOTIFICATION_META.get(
        key,
        {
            "badge": "INFORMACION",
            "headline": "Actualizacion de ejecucion",
            "summary": "Se genero un evento durante el monitoreo de citas.",
            "action": "Revisa el detalle para confirmar el resultado del proceso.",
            "color": "#263238",
            "soft": "#eceff1",
        },
    )
    event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    details = build_notification_details(key, safe_msg)
    details_html = "".join(f"<li>{escape(line)}</li>" for line in details)
    plain_details = "\n".join(f"- {line}" for line in details)

    plain_text = (
        "Visa Scheduler - Notificacion\n"
        f"Estado: {meta['badge']}\n"
        f"Titulo: {safe_title}\n"
        f"Resumen: {meta['summary']}\n"
        f"Accion ejecutada: {meta['action']}\n"
        f"Fecha y hora: {event_time}\n"
        f"Embajada: {EMBASSY}\n"
        f"Rango objetivo: {PRIOD_START} a {PRIOD_END}\n"
        "Detalles:\n"
        f"{plain_details}\n"
    )

    html_text = f"""\
<!DOCTYPE html>
<html lang="es">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Visa Scheduler</title>
  </head>
  <body style="margin:0;padding:24px;background:#f4f7fb;font-family:Segoe UI,Arial,sans-serif;color:#1f2937;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:720px;margin:0 auto;border-collapse:collapse;">
      <tr>
        <td style="padding:0;">
          <div style="background:linear-gradient(120deg,#0f172a,#1e3a8a);border-radius:14px 14px 0 0;padding:22px 24px;">
            <p style="margin:0;color:#c7d2fe;font-size:12px;letter-spacing:1px;text-transform:uppercase;">Visa Scheduler</p>
            <h1 style="margin:8px 0 0 0;color:#ffffff;font-size:22px;line-height:1.2;">{escape(meta['headline'])}</h1>
          </div>
          <div style="background:#ffffff;border:1px solid #dbe5f1;border-top:0;border-radius:0 0 14px 14px;padding:24px;">
            <div style="display:inline-block;background:{meta['soft']};color:{meta['color']};font-weight:700;font-size:12px;padding:6px 10px;border-radius:999px;margin-bottom:14px;">
              {escape(meta['badge'])}
            </div>
            <p style="margin:0 0 8px 0;font-size:16px;font-weight:700;color:#111827;">{escape(safe_title)}</p>
            <p style="margin:0 0 10px 0;font-size:14px;line-height:1.6;color:#374151;">{escape(meta['summary'])}</p>
            <p style="margin:0 0 18px 0;font-size:14px;line-height:1.6;color:#374151;"><strong>Accion ejecutada:</strong> {escape(meta['action'])}</p>
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;">
              <tr><td style="padding:14px 16px;font-size:13px;color:#334155;"><strong>Fecha y hora:</strong> {escape(event_time)}</td></tr>
              <tr><td style="padding:0 16px 14px 16px;font-size:13px;color:#334155;"><strong>Embajada:</strong> {escape(EMBASSY)}</td></tr>
              <tr><td style="padding:0 16px 14px 16px;font-size:13px;color:#334155;"><strong>Rango objetivo:</strong> {escape(PRIOD_START)} a {escape(PRIOD_END)}</td></tr>
            </table>
            <h2 style="margin:20px 0 10px 0;font-size:15px;color:#0f172a;">Detalle del evento</h2>
            <ul style="margin:0;padding-left:18px;color:#334155;line-height:1.6;font-size:14px;">
              {details_html}
            </ul>
            <p style="margin:18px 0 0 0;font-size:12px;color:#64748b;">Mensaje generado automaticamente por el monitor de citas.</p>
          </div>
        </td>
      </tr>
    </table>
  </body>
</html>
"""
    return safe_title, safe_msg, plain_text, html_text


for static_url in [SIGN_IN_LINK, APPOINTMENT_URL, DATE_URL, SIGN_OUT_LINK]:
    validate_ais_url(static_url)
validate_notification_settings()


def create_webdriver():
    options = webdriver.ChromeOptions()
    options.add_argument("--incognito")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--no-first-run")
    options.page_load_strategy = "eager"
    if LOCAL_USE:
        validate_local_driver(CHROMEDRIVER_PATH)
        driver_instance = webdriver.Chrome(service=ChromeService(CHROMEDRIVER_PATH), options=options)
    else:
        validate_local_hub(HUB_ADDRESS)
        driver_instance = webdriver.Remote(command_executor=HUB_ADDRESS, options=options)
    try:
        driver_instance.set_page_load_timeout(PAGE_LOAD_TIMEOUT_SECONDS)
    except Exception:
        pass
    try:
        driver_instance.set_script_timeout(SCRIPT_TIMEOUT_SECONDS)
    except Exception:
        pass
    return driver_instance


def reset_webdriver_session(reason=""):
    global driver
    global USER_AGENT_CACHE
    try:
        if driver is not None:
            try:
                driver.stop_client()
            except Exception:
                pass
            try:
                driver.quit()
            except Exception:
                pass
    except Exception:
        pass
    driver = create_webdriver()
    USER_AGENT_CACHE = ""
    if reason:
        log_warning(f"WebDriver reiniciado: {reason}")


driver = create_webdriver()


def send_notification(title, msg):
    safe_title, safe_msg, plain_text, html_text = build_notification_payload(title, msg)
    log_summary = " | ".join(build_notification_details(safe_title.upper(), safe_msg))
    log_info(f"[LOCAL_NOTIFICATION] {safe_title}: {log_summary}")
    if not EMAIL_ENABLED:
        return

    email = EmailMessage()
    email["Subject"] = f"[Visa Scheduler] {safe_title}"
    email["From"] = EMAIL_FROM
    email["To"] = EMAIL_TO
    email.set_content(plain_text)
    email.add_alternative(html_text, subtype="html")
    try:
        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15) as smtp:
                smtp.login(SMTP_USERNAME, SMTP_APP_PASSWORD)
                smtp.send_message(email)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(SMTP_USERNAME, SMTP_APP_PASSWORD)
                smtp.send_message(email)
        log_info("Email notification sent.")
    except Exception as exc:
        log_error(f"Email notification failed: {sanitize_text(exc)}")


def auto_action(label, find_by, el_type, action, value, sleep_time=0):
    log_info(f"Action start: {label}")
    # Find Element By
    match find_by.lower():
        case 'id':
            item = driver.find_element(By.ID, el_type)
        case 'name':
            item = driver.find_element(By.NAME, el_type)
        case 'class':
            item = driver.find_element(By.CLASS_NAME, el_type)
        case 'xpath':
            item = driver.find_element(By.XPATH, el_type)
        case _:
            return 0
    # Do Action:
    match action.lower():
        case 'send':
            item.send_keys(value)
        case 'click':
            item.click()
        case _:
            return 0
    log_info(f"Action completed: {label}")
    if sleep_time:
        time.sleep(sleep_time)


def start_process():
    # Bypass reCAPTCHA
    navigate_ais_page(SIGN_IN_LINK, attempts=4)
    time.sleep(STEP_TIME)
    Wait(driver, 60).until(EC.presence_of_element_located((By.NAME, "commit")))
    auto_action("Click bounce", "xpath", '//a[@class="down-arrow bounce"]', "click", "", STEP_TIME)
    auto_action("Email", "id", "user_email", "send", USERNAME, STEP_TIME)
    auto_action("Password", "id", "user_password", "send", PASSWORD, STEP_TIME)
    auto_action("Privacy", "class", "icheckbox", "click", "", STEP_TIME)
    auto_action("Enter Panel", "name", "commit", "click", "", STEP_TIME)
    continue_xpath = "//a[contains(text(), '" + REGEX_CONTINUE + "')]"
    continue_link = Wait(driver, 60).until(
        EC.presence_of_element_located((By.XPATH, continue_xpath))
    )
    log_info("Login completed successfully.")
    continue_href = (continue_link.get_attribute("href") or "").strip()
    try:
        if continue_href:
            validate_ais_url(continue_href)
            navigate_ais_page(continue_href, attempts=4)
        else:
            driver.execute_script("arguments[0].click();", continue_link)
            Wait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except Exception as exc:
        log_warning(f"Could not open continue link directly: {exc}")
    ensure_appointment_page_ready(force_navigate=False)


def find_first_interactable(elements):
    for element in elements:
        try:
            if element.is_displayed() and element.is_enabled():
                return element
        except Exception:
            continue
    return None


def get_continue_button():
    button_xpaths = [
        "//button[normalize-space()='Continuar' or normalize-space()='Continue']",
        "//input[( @type='submit' or @type='button') and (@value='Continuar' or @value='Continue')]",
        "//a[normalize-space()='Continuar' or normalize-space()='Continue']",
        "//button[contains(., 'Continuar') or contains(., 'Continue')]",
        "//a[contains(., 'Continuar') or contains(., 'Continue')]",
    ]
    for xpath in button_xpaths:
        button = find_first_interactable(driver.find_elements(By.XPATH, xpath))
        if button:
            return button
    return None


def is_applicant_selection_step():
    has_tokens = bool(driver.find_elements(By.NAME, "authenticity_token"))
    if has_tokens:
        return False
    continue_button = get_continue_button()
    if not continue_button:
        return False
    # Some accounts show a pure "Continuar" step without checkboxes.
    return True


def confirm_all_applicants_and_continue():
    checkboxes = driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
    selected_count = 0
    for cb in checkboxes:
        try:
            if not cb.is_enabled():
                continue
            if not cb.is_selected():
                driver.execute_script("arguments[0].click();", cb)
            if cb.is_selected():
                selected_count += 1
        except Exception:
            continue

    continue_button = get_continue_button()
    if not continue_button:
        raise RuntimeError("No se encontro el boton Continuar en seleccion de solicitantes.")
    driver.execute_script("arguments[0].click();", continue_button)
    Wait(driver, 30).until(EC.presence_of_element_located((By.NAME, "authenticity_token")))
    log_info(f"Applicant step confirmed. Selected: {selected_count}")


def validate_reschedule_form_ready():
    required_names = [
        "authenticity_token",
        "confirmed_limit_message",
        "appointments[consulate_appointment][facility_id]",
        "appointments[consulate_appointment][date]",
        "appointments[consulate_appointment][time]",
        "appointments[asc_appointment][facility_id]",
    ]
    missing = []
    for field_name in required_names:
        elements = driver.find_elements(By.NAME, field_name)
        if not elements:
            missing.append(field_name)
    if missing:
        raise RuntimeError(
            "La pantalla de reagendado no esta completa. "
            f"Campos faltantes: {', '.join(missing)}"
        )
    log_info("Reschedule page validated (Consular + CAS).")


def has_reschedule_form_fields():
    required_names = [
        "authenticity_token",
        "appointments[consulate_appointment][facility_id]",
        "appointments[consulate_appointment][date]",
        "appointments[consulate_appointment][time]",
        "appointments[asc_appointment][facility_id]",
    ]
    missing = []
    for field_name in required_names:
        elements = driver.find_elements(By.NAME, field_name)
        if not elements:
            missing.append(field_name)
    return not missing


def get_reschedule_form():
    form_xpaths = [
        "//form[.//*[@name='appointments[consulate_appointment][date]']]",
        "//form[.//*[@name='appointments[asc_appointment][facility_id]']]",
    ]
    for xpath in form_xpaths:
        forms = driver.find_elements(By.XPATH, xpath)
        if forms:
            return forms[0]
    raise RuntimeError("No se encontro formulario principal de reagendado.")


def pick_preferred_element(elements, required_tag=None):
    candidates = []
    for element in elements:
        try:
            if required_tag and element.tag_name.lower() != required_tag.lower():
                continue
            candidates.append(element)
        except Exception:
            continue
    if not candidates:
        return None
    for element in candidates:
        try:
            if element.is_displayed():
                return element
        except Exception:
            continue
    return candidates[0]


def find_named_element(field_name, required_tag=None, root=None):
    search_root = root or driver
    try:
        elements = search_root.find_elements(By.NAME, field_name)
    except Exception:
        elements = []
    element = pick_preferred_element(elements, required_tag=required_tag)
    if element:
        return element
    if root is not None:
        fallback_elements = driver.find_elements(By.NAME, field_name)
        fallback = pick_preferred_element(fallback_elements, required_tag=required_tag)
        if fallback:
            return fallback
    scope = "formulario" if root is not None else "pagina"
    raise RuntimeError(f"No se encontro campo '{field_name}' en {scope}.")


def extract_portal_feedback():
    try:
        feedback = extract_response_message(driver.page_source)
        if feedback:
            return feedback
        selectors = [
            "div.alert",
            "div.notice",
            "div.flash",
            "p.error",
            "span.help-inline",
            "span.help-block",
            ".field_with_errors",
            "#error_explanation",
        ]
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = strip_html_tags(element.get_attribute("innerHTML") or element.text or "")
                if text:
                    return text[:240]
    except Exception:
        return ""
    return ""


def get_session_cookie_value():
    try:
        cookie = driver.get_cookie("_yatri_session")
    except (InvalidSessionIdException, NoSuchWindowException) as exc:
        raise RuntimeError(f"Sesion del navegador no disponible: {exc.__class__.__name__}") from exc
    if cookie and cookie.get("value"):
        return cookie["value"]
    # One retry after opening appointment page in case session cookie was not loaded yet.
    ensure_appointment_page_ready(force_navigate=False)
    try:
        cookie = driver.get_cookie("_yatri_session")
    except (InvalidSessionIdException, NoSuchWindowException) as exc:
        raise RuntimeError(f"Sesion del navegador no disponible: {exc.__class__.__name__}") from exc
    if cookie and cookie.get("value"):
        return cookie["value"]
    raise RuntimeError("Sesion invalida: no existe cookie _yatri_session activa.")


def build_cookie_header():
    try:
        cookies = driver.get_cookies()
    except (InvalidSessionIdException, NoSuchWindowException) as exc:
        raise RuntimeError(f"No se pueden leer cookies de sesion: {exc.__class__.__name__}") from exc
    cookie_pairs = []
    for cookie in cookies:
        name = (cookie.get("name") or "").strip()
        value = (cookie.get("value") or "").strip()
        if name and value:
            cookie_pairs.append(f"{name}={value}")
    if not cookie_pairs:
        # Ensure at least _yatri_session exists; raises clear message otherwise.
        _ = get_session_cookie_value()
        cookies = driver.get_cookies()
        for cookie in cookies:
            name = (cookie.get("name") or "").strip()
            value = (cookie.get("value") or "").strip()
            if name and value:
                cookie_pairs.append(f"{name}={value}")
    if not cookie_pairs:
        raise RuntimeError("No se encontraron cookies activas para construir headers.")
    return "; ".join(cookie_pairs)


def ensure_appointment_page_ready(force_navigate=False):
    validate_ais_url(APPOINTMENT_URL)
    if force_navigate:
        try:
            navigate_ais_page(APPOINTMENT_URL, attempts=3)
        except Exception:
            # Fallback: load schedule status and use the portal's own "Continuar" navigation path.
            navigate_ais_page(SCHEDULE_STATUS_URL, attempts=3)
            continue_button = get_continue_button()
            if continue_button:
                driver.execute_script("arguments[0].click();", continue_button)
                Wait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            else:
                raise
    else:
        if has_reschedule_form_fields():
            log_info("Reschedule page already loaded. Skipping repeated approval.")
            return
        reload_buttons = driver.find_elements(
            By.XPATH,
            "//button[contains(., 'Volver a cargar') or contains(., 'Reload')]",
        )
        if reload_buttons:
            log_warning("Portal returned reload screen. Reopening appointment form.")
            navigate_ais_page(APPOINTMENT_URL, attempts=4)
        current = driver.current_url or ""
        if not current.startswith(APPOINTMENT_URL):
            continue_button = get_continue_button()
            if continue_button:
                try:
                    driver.execute_script("arguments[0].click();", continue_button)
                    Wait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    current = driver.current_url or ""
                except Exception:
                    current = driver.current_url or ""
        if not current.startswith(APPOINTMENT_URL):
            navigate_ais_page(APPOINTMENT_URL, attempts=4)
    if is_applicant_selection_step():
        log_info("Applicant selection screen detected. Confirming selection.")
        confirm_all_applicants_and_continue()
    else:
        Wait(driver, 30).until(EC.presence_of_element_located((By.NAME, "authenticity_token")))
    validate_reschedule_form_ready()


def get_user_agent():
    global USER_AGENT_CACHE
    if USER_AGENT_CACHE:
        return USER_AGENT_CACHE
    try:
        USER_AGENT_CACHE = driver.execute_script("return navigator.userAgent;") or "Mozilla/5.0"
    except Exception:
        USER_AGENT_CACHE = "Mozilla/5.0"
    return USER_AGENT_CACHE


def get_request_headers():
    cookie_header = build_cookie_header()
    return {
        "User-Agent": get_user_agent(),
        "Referer": APPOINTMENT_URL,
        "Cookie": cookie_header,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }


def get_form_payload_items(root=None):
    form = root or get_reschedule_form()
    payload_items = []
    fields = form.find_elements(By.XPATH, ".//*[@name]")
    for field in fields:
        try:
            name = (field.get_attribute("name") or "").strip()
            if not name:
                continue
            if field.get_attribute("disabled") is not None:
                continue
            tag_name = (field.tag_name or "").lower()
            if tag_name == "input":
                input_type = (field.get_attribute("type") or "text").lower()
                if input_type in {"submit", "button", "image", "file"}:
                    continue
                if input_type in {"checkbox", "radio"} and not field.is_selected():
                    continue
                value = field.get_attribute("value")
                payload_items.append((name, value if value is not None else ""))
                continue
            if tag_name == "select":
                try:
                    selected = Select(field).first_selected_option
                    value = selected.get_attribute("value")
                except Exception:
                    value = field.get_attribute("value")
                payload_items.append((name, value if value is not None else ""))
                continue
            if tag_name == "textarea":
                value = field.get_attribute("value")
                if value is None:
                    value = field.text or ""
                payload_items.append((name, value))
        except Exception:
            continue
    return payload_items


def upsert_payload_item(payload_items, name, value):
    filtered = [(k, v) for (k, v) in payload_items if k != name]
    if value is not None:
        filtered.append((name, str(value)))
    return filtered


def get_form_tokens(root=None):
    return get_form_payload_items(root=root)


def collect_page_structure_report():
    try:
        field_names = [
            "authenticity_token",
            "confirmed_limit_message",
            "appointments[consulate_appointment][facility_id]",
            "appointments[consulate_appointment][date]",
            "appointments[consulate_appointment][time]",
            "appointments[asc_appointment][facility_id]",
            "appointments[asc_appointment][date]",
            "appointments[asc_appointment][time]",
            "commit",
        ]
        field_counts = []
        for name in field_names:
            count = len(driver.find_elements(By.NAME, name))
            field_counts.append(f"{name}={count}")

        button_texts = []
        button_elements = (
                driver.find_elements(By.XPATH, "//button")
                + driver.find_elements(By.XPATH, "//input[@type='submit' or @type='button']")
                + driver.find_elements(By.XPATH, "//a[contains(@class,'button') or contains(@class,'btn')]")
        )
        for button in button_elements:
            if len(button_texts) >= 8:
                break
            try:
                text = (button.text or button.get_attribute("value") or "").strip()
                if text:
                    button_texts.append(text)
            except Exception:
                continue

        url = driver.current_url
        buttons_part = ", ".join(button_texts) if button_texts else "N/D"
        fields_part = ", ".join(field_counts)
        feedback = extract_portal_feedback() or "N/D"
        return (
            f"Diagnostico estructura -> URL: {url} | "
            f"Campos: {fields_part} | Botones: {buttons_part} | Mensaje portal: {feedback}"
        )
    except Exception as exc:
        return f"Diagnostico estructura no disponible: {exc.__class__.__name__}: {exc}"


def get_field_value(field_name):
    try:
        form = get_reschedule_form()
        element = find_named_element(field_name, root=form)
        value = element.get_attribute("value")
        if value is None:
            return None
        value = value.strip()
        return value if value else None
    except Exception:
        return None


def get_element_action_label(element):
    try:
        value = (element.get_attribute("value") or element.text or "").strip()
    except Exception:
        value = ""
    return value


def is_reschedule_action_label(value):
    normalized = normalize_lookup_text(value)
    markers = ["reprogramar", "reschedule", "schedule appointment"]
    return any(marker in normalized for marker in markers)


def get_commit_value(root=None):
    search_root = root or driver
    buttons = search_root.find_elements(By.NAME, "commit")
    for button in buttons:
        value = get_element_action_label(button)
        if value and is_reschedule_action_label(value):
            return value
    for button in buttons:
        value = get_element_action_label(button)
        if value:
            return value
    return "Reprogramar"


DATEPICKER_MONTH_MAP = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def normalize_lookup_text(text):
    normalized = unicodedata.normalize("NFKD", text or "")
    no_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return no_accents.strip().lower()


def month_name_to_number(month_name):
    return DATEPICKER_MONTH_MAP.get(normalize_lookup_text(month_name))


def get_visible_datepicker_state():
    try:
        state = driver.execute_script(
            "var root=document.getElementById('ui-datepicker-div');"
            "if(!root){return null;}"
            "var monthEl=root.querySelector('.ui-datepicker-month');"
            "var yearEl=root.querySelector('.ui-datepicker-year');"
            "return {"
            " visible: window.getComputedStyle(root).display !== 'none',"
            " month: monthEl ? monthEl.textContent.trim() : '',"
            " year: yearEl ? yearEl.textContent.trim() : ''"
            "};"
        )
    except Exception:
        return None
    return state


def set_date_with_script(element, value):
    driver.execute_script(
        "var el=arguments[0], val=arguments[1];"
        "el.removeAttribute('readonly');"
        "if (window.jQuery) {"
        "  var $el = window.jQuery(el);"
        "  if (window.jQuery.datepicker) {"
        "    try {"
        "      if (typeof $el.datepicker === 'function') { $el.datepicker('setDate', val); }"
        "    } catch (e) {}"
        "    try {"
        "      if (typeof window.jQuery.datepicker._setDateDatepicker === 'function') {"
        "        window.jQuery.datepicker._setDateDatepicker(el, val);"
        "      }"
        "    } catch (e) {}"
        "    try {"
        "      if (typeof window.jQuery.datepicker._selectDate === 'function') {"
        "        window.jQuery.datepicker._selectDate(el, val);"
        "      }"
        "    } catch (e) {}"
        "    try {"
        "      if (typeof window.jQuery.datepicker._getInst === 'function') {"
        "        var inst = window.jQuery.datepicker._getInst(el);"
        "        if (inst && inst.settings && typeof inst.settings.onSelect === 'function') {"
        "          inst.settings.onSelect.call(el, val, inst);"
        "        }"
        "      }"
        "    } catch (e) {}"
        "    try { $el.trigger('change'); $el.trigger('blur'); } catch (e) {}"
        "  }"
        "}"
        "el.value = val;"
        "el.dispatchEvent(new Event('input', {bubbles: true}));"
        "el.dispatchEvent(new Event('change', {bubbles: true}));"
        "el.dispatchEvent(new Event('blur', {bubbles: true}));",
        element,
        value,
    )


def pick_date_with_datepicker_click(field_name, value, root=None):
    try:
        target_date = datetime.strptime(str(value or "").strip(), "%Y-%m-%d")
    except ValueError:
        return False
    element = find_named_element(field_name, root=root)
    try:
        driver.execute_script(
            "arguments[0].removeAttribute('readonly');"
            "arguments[0].scrollIntoView({block: 'center'});"
            "arguments[0].focus();"
            "arguments[0].click();",
            element,
        )
    except Exception:
        return False

    try:
        Wait(driver, 8).until(
            lambda _:
            (get_visible_datepicker_state() or {}).get("visible") is True
        )
    except Exception:
        return False

    for _ in range(24):
        state = get_visible_datepicker_state() or {}
        month_value = month_name_to_number(state.get("month", ""))
        try:
            year_value = int(str(state.get("year", "")).strip())
        except Exception:
            year_value = None
        if month_value and year_value:
            delta_months = (target_date.year - year_value) * 12 + (target_date.month - month_value)
            if delta_months == 0:
                break
            button_selector = ".ui-datepicker-next" if delta_months > 0 else ".ui-datepicker-prev"
            moved = driver.execute_script(
                "var root=document.getElementById('ui-datepicker-div');"
                "if(!root){return false;}"
                "var btn=root.querySelector(arguments[0]);"
                "if(!btn){return false;}"
                "btn.click();"
                "return true;",
                button_selector,
            )
            if not moved:
                return False
            time.sleep(0.2)
            continue
        return False

    click_result = driver.execute_script(
        "var root=document.getElementById('ui-datepicker-div');"
        "if(!root){return 'missing';}"
        "var targetDay=String(arguments[0]);"
        "var dayNodes=root.querySelectorAll('td[data-handler=\"selectDay\"]');"
        "for(var i=0;i<dayNodes.length;i++){"
        "  var td=dayNodes[i];"
        "  var cls=(td.getAttribute('class')||'');"
        "  if(cls.indexOf('ui-datepicker-unselectable')!==-1 || cls.indexOf('ui-state-disabled')!==-1){continue;}"
        "  var link=td.querySelector('a');"
        "  if(!link){continue;}"
        "  if((link.textContent||'').trim()===targetDay){"
        "    link.click();"
        "    return 'clicked';"
        "  }"
        "}"
        "return 'not-found';",
        str(target_date.day),
    )
    if click_result != "clicked":
        return False

    time.sleep(0.2)
    current_value = get_selected_value(field_name, root=root)
    return values_match(field_name, current_value, value)


def set_input_value(field_name, value, root=None):
    element = find_named_element(field_name, root=root)
    if field_name.endswith("[date]"):
        picked_with_click = pick_date_with_datepicker_click(field_name, value, root=root)
        if not picked_with_click:
            set_date_with_script(element, value)
    else:
        driver.execute_script(
            "arguments[0].value = arguments[1];"
            "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));"
            "arguments[0].dispatchEvent(new Event('change', {bubbles: true}));",
            element,
            value,
        )


def normalize_time_slot(time_value):
    value = str(time_value or "").strip()
    if not value:
        return ""
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).strftime("%H:%M")
        except ValueError:
            continue
    match = re.match(r"^(\d{1,2}):(\d{2})", value)
    if match:
        hour = int(match.group(1))
        minute_part = int(match.group(2))
        if 0 <= hour <= 23 and 0 <= minute_part <= 59:
            return f"{hour:02d}:{minute_part:02d}"
    return ""


def normalize_date_slot(date_value):
    value = str(date_value or "").strip()
    if not value:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", value)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return ""


def is_quarter_hour_slot(time_value):
    normalized = normalize_time_slot(time_value)
    if not normalized:
        return False
    minute_part = int(normalized.split(":")[1])
    return minute_part % 15 == 0


def values_match(field_name, current, expected):
    current_text = str(current or "").strip()
    expected_text = str(expected or "").strip()
    if field_name.endswith("[time]"):
        normalized_current = normalize_time_slot(current_text)
        normalized_expected = normalize_time_slot(expected_text)
        if normalized_current and normalized_expected:
            return normalized_current == normalized_expected
    if field_name.endswith("[date]"):
        normalized_current = normalize_date_slot(current_text)
        normalized_expected = normalize_date_slot(expected_text)
        if normalized_current and normalized_expected:
            return normalized_current == normalized_expected
    return current_text == expected_text


def get_selected_value(field_name, root=None):
    element = find_named_element(field_name, root=root)
    if element.tag_name.lower() == "select":
        selected = Select(element).first_selected_option.get_attribute("value")
        return selected if selected is not None else (element.get_attribute("value") or "")
    return element.get_attribute("value") or ""


def get_enabled_select_values(field_name, root=None):
    element = find_named_element(field_name, required_tag="select", root=root)
    values = []
    for option in Select(element).options:
        value = (option.get_attribute("value") or "").strip()
        disabled = option.get_attribute("disabled") is not None
        if value and not disabled:
            values.append(value)
    return values


def describe_select_options(field_name, root=None):
    element = find_named_element(field_name, required_tag="select", root=root)
    described = []
    for option in Select(element).options:
        value = (option.get_attribute("value") or "").strip()
        text = (option.text or "").strip()
        disabled = option.get_attribute("disabled") is not None
        label = value if value else f"text:{text}"
        if disabled:
            label += "(disabled)"
        described.append(label)
    return described


def get_date_field_for_time_field(field_name):
    if "consulate_appointment" in field_name:
        return "appointments[consulate_appointment][date]"
    if "asc_appointment" in field_name:
        return "appointments[asc_appointment][date]"
    return None


def trigger_time_options_reload(field_name, root=None):
    date_field = get_date_field_for_time_field(field_name)
    if not date_field:
        return
    try:
        date_value = (get_selected_value(date_field, root=root) or "").strip()
    except Exception:
        date_value = ""
    if not date_value:
        return
    try:
        set_input_value(date_field, date_value, root=root)
    except Exception:
        return


def wait_for_time_options(field_name, root=None, timeout_seconds=12):
    start_time = time.time()
    deadline = time.time() + timeout_seconds
    last_values = []
    reload_attempts = 0
    while time.time() < deadline:
        values = get_enabled_select_values(field_name, root=root)
        valid_values = [value for value in values if is_quarter_hour_slot(value)]
        if valid_values:
            return valid_values
        last_values = values
        elapsed = time.time() - start_time
        if reload_attempts < 2 and elapsed >= (2 + reload_attempts * 2):
            trigger_time_options_reload(field_name, root=root)
            reload_attempts += 1
        time.sleep(0.5)
    valid_values = [value for value in last_values if is_quarter_hour_slot(value)]
    return valid_values


def get_api_times_for_field(field_name, root=None):
    if "consulate_appointment" in field_name:
        facility_field = "appointments[consulate_appointment][facility_id]"
        date_field = "appointments[consulate_appointment][date]"
    elif "asc_appointment" in field_name:
        facility_field = "appointments[asc_appointment][facility_id]"
        date_field = "appointments[asc_appointment][date]"
    else:
        return []

    facility_id = (get_selected_value(facility_field, root=root) or "").strip()
    date_value = (get_selected_value(date_field, root=root) or "").strip()
    if not facility_id or not date_value:
        return []
    try:
        time_url = build_times_url(facility_id, date_value)
        validate_ais_url(time_url)
        headers = get_request_headers()
        params = build_request_params([("date", date_value), ("appointments[expedite]", "false")])
        response = requests.get(time_url, headers=headers, params=params, timeout=30)
        if response.status_code != 200:
            return []
        available = response.json().get("available_times") or []
        valid_times = [slot for slot in available if is_quarter_hour_slot(slot)]
        return valid_times
    except Exception:
        return []


def resolve_time_for_form(field_name, preferred_time, root=None, pick_last=True, strict_preferred=False):
    valid_times = wait_for_time_options(field_name, root=root)
    if not valid_times:
        options_snapshot = describe_select_options(field_name, root=root)
        api_times = get_api_times_for_field(field_name, root=root)
        raise RuntimeError(
            f"No hay horas habilitadas en selector '{field_name}'. "
            f"Opciones detectadas: {', '.join(options_snapshot) or 'N/D'} | "
            f"Horas API: {', '.join(api_times) or 'N/D'}"
        )
    if preferred_time:
        for option in valid_times:
            if values_match(field_name, option, preferred_time):
                return option
        if strict_preferred:
            raise RuntimeError(
                f"La hora preferida {preferred_time} no esta habilitada en '{field_name}'. "
                f"Opciones vigentes: {', '.join(valid_times)}"
            )
    selected_time = valid_times[-1] if pick_last else valid_times[0]
    if preferred_time:
        log_warning(
            f"Hora preferida {preferred_time} no esta habilitada en '{field_name}'. "
            f"Se usa {selected_time}."
        )
    return selected_time


def normalize_match_text(text):
    normalized = unicodedata.normalize("NFKD", text or "")
    no_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return no_accents.lower()


def contains_invalid_time_text(text):
    normalized = normalize_match_text(text)
    markers = [
        "hora inhabil",
        "hora no valida",
        "hora no disponible",
        "invalid time",
        "selected time is not valid",
    ]
    return any(marker in normalized for marker in markers)


def contains_invalid_day_text(text):
    normalized = normalize_match_text(text)
    markers = [
        "dia inhabil",
        "dia no habil",
        "selected non-business day",
        "selected a non-business day",
        "selected day is not valid",
        "invalid day",
        "invalid date",
        "fecha no valida",
        "fecha no disponible",
        "date is not valid",
        "date is no longer available",
    ]
    return any(marker in normalized for marker in markers)


def get_feedback_text_snapshot():
    try:
        body_elements = driver.find_elements(By.TAG_NAME, "body")
        body_text = body_elements[0].text if body_elements else (driver.page_source or "")
    except Exception:
        body_text = driver.page_source or ""
    feedback = extract_portal_feedback()
    return f"{body_text}\n{feedback}"


def has_invalid_time_message():
    return contains_invalid_time_text(get_feedback_text_snapshot())


def has_invalid_day_message():
    return contains_invalid_day_text(get_feedback_text_snapshot())


def validate_date_still_available(facility_id, date_value, label):
    available_dates = get_dates_for_facility(facility_id)
    available_values = sorted(
        {
            (item.get("date") or "").strip()
            for item in (available_dates or [])
            if isinstance(item, dict) and (item.get("date") or "").strip()
        }
    )
    if date_value not in available_values:
        preview = ", ".join(available_values[:12])
        if len(available_values) > 12:
            preview += ", ..."
        raise RuntimeError(
            f"La fecha {label} {date_value} ya no esta disponible al momento de enviar. "
            f"Fechas vigentes: {preview or 'N/D'}"
        )


def ensure_value_kept(field_name, expected_value, root=None, as_select=False):
    if expected_value is None:
        return
    setter = set_select_value if as_select else set_input_value
    for _ in range(2):
        setter(field_name, expected_value, root=root)
        current = get_selected_value(field_name, root=root)
        if values_match(field_name, current, expected_value):
            return
    current = get_selected_value(field_name, root=root)
    raise RuntimeError(
        f"No se pudo mantener la seleccion de '{field_name}'. "
        f"Esperado: {expected_value}, Actual: {current}"
    )


def set_select_value(field_name, value, root=None, allow_create_option=False):
    element = find_named_element(field_name, required_tag="select", root=root)
    select = Select(element)
    options = {
        (opt.get_attribute("value") or "").strip(): (opt.get_attribute("disabled") is not None)
        for opt in select.options
        if (opt.get_attribute("value") or "").strip()
    }
    if value not in options:
        if not allow_create_option:
            raise RuntimeError(
                f"El valor '{value}' no existe en el selector '{field_name}'. "
                f"Opciones actuales: {', '.join(options.keys()) or 'N/D'}"
            )
        driver.execute_script(
            "var sel=arguments[0], val=arguments[1];"
            "var opt=document.createElement('option');"
            "opt.value=val; opt.text=val; sel.appendChild(opt);",
            element,
            value,
        )
        select = Select(element)
        options[value] = False
    if options.get(value):
        raise RuntimeError(f"El valor '{value}' en '{field_name}' esta deshabilitado.")
    try:
        select.select_by_value(value)
    except Exception:
        driver.execute_script(
            "arguments[0].value = arguments[1];",
            element,
            value,
        )
    try:
        option = element.find_element(By.XPATH, f".//option[@value=\"{value}\"]")
        driver.execute_script(
            "arguments[0].selected = true;"
            "arguments[0].dispatchEvent(new Event('click', {bubbles: true}));",
            option,
        )
    except Exception:
        pass
    driver.execute_script(
        "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));"
        "arguments[0].dispatchEvent(new Event('change', {bubbles: true}));"
        "arguments[0].dispatchEvent(new Event('blur', {bubbles: true}));",
        element,
    )


def submit_reprogramar_request(
        consular_facility_id,
        consular_date,
        consular_time,
        asc_facility_id=None,
        asc_date=None,
        asc_time=None,
):
    form = get_reschedule_form()
    action_url = form.get_attribute("action") or APPOINTMENT_URL
    action_url = append_query_params_to_url(action_url, get_current_applicant_params())
    validate_ais_url(action_url)
    payload = get_form_tokens(root=form)
    commit_value = get_commit_value(root=form)
    payload = upsert_payload_item(payload, "appointments[consulate_appointment][facility_id]", consular_facility_id)
    payload = upsert_payload_item(payload, "appointments[consulate_appointment][date]", consular_date)
    payload = upsert_payload_item(payload, "appointments[consulate_appointment][time]", consular_time)
    payload = upsert_payload_item(payload, "commit", commit_value)
    if asc_facility_id:
        payload = upsert_payload_item(payload, "appointments[asc_appointment][facility_id]", asc_facility_id)
    if asc_date:
        payload = upsert_payload_item(payload, "appointments[asc_appointment][date]", asc_date)
    if asc_time:
        payload = upsert_payload_item(payload, "appointments[asc_appointment][time]", asc_time)

    headers = get_request_headers()
    headers["Origin"] = f"https://{ALLOWED_AIS_HOST}"
    headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    response = request_post_with_retry(
        action_url,
        headers=headers,
        data=payload,
        retries=3,
        timeout=45,
    )
    log_info(
        f"Direct submit HTTP {response.status_code} for {action_url} "
        f"(commit={commit_value}, consular={consular_date} {consular_time})"
    )
    feedback = extract_response_message(response.text)
    if feedback:
        log_info(f"Portal feedback after submit: {feedback}")
    return response.status_code, feedback


def submit_reprogramar_form(
        consular_facility_id,
        consular_date,
        consular_time,
        asc_facility_id=None,
        asc_date=None,
        asc_time=None,
):
    ensure_appointment_page_ready(force_navigate=False)
    form = get_reschedule_form()
    consular_facility_id = str(consular_facility_id)
    asc_facility_id = str(asc_facility_id) if asc_facility_id else None
    ensure_value_kept(
        "appointments[consulate_appointment][facility_id]",
        consular_facility_id,
        root=form,
        as_select=True,
    )
    ensure_value_kept(
        "appointments[consulate_appointment][date]",
        consular_date,
        root=form,
        as_select=False,
    )
    validate_date_still_available(consular_facility_id, consular_date, "consular")
    if has_invalid_day_message():
        log_warning(
            "Se detecto posible mensaje de fecha invalida antes del envio consular. "
            "Se continuara y se validara resultado despues del submit."
        )

    selected_consular_time = consular_time
    try:
        selected_consular_time = resolve_time_for_form(
            "appointments[consulate_appointment][time]",
            consular_time,
            root=form,
            pick_last=True,
            strict_preferred=True,
        )
        ensure_value_kept(
            "appointments[consulate_appointment][time]",
            selected_consular_time,
            root=form,
            as_select=True,
        )
        if has_invalid_time_message():
            log_warning(
                "Se detecto posible mensaje de hora invalida antes del envio consular. "
                "Se continuara y se validara resultado despues del submit."
            )
    except Exception as exc:
        raise RuntimeError(
            "No se pudo determinar una hora consular habilitada desde la lista de la pagina. "
            f"Detalle: {exc}"
        )

    if asc_facility_id:
        ensure_value_kept(
            "appointments[asc_appointment][facility_id]",
            asc_facility_id,
            root=form,
            as_select=True,
        )
    if asc_date:
        ensure_value_kept(
            "appointments[asc_appointment][date]",
            asc_date,
            root=form,
            as_select=False,
        )
        if asc_facility_id:
            validate_date_still_available(asc_facility_id, asc_date, "CAS")
        if has_invalid_day_message():
            log_warning(
                "Se detecto posible mensaje de fecha invalida antes del envio CAS. "
                "Se continuara y se validara resultado despues del submit."
            )

    selected_asc_time = asc_time
    if asc_time:
        try:
            selected_asc_time = resolve_time_for_form(
                "appointments[asc_appointment][time]",
                asc_time,
                root=form,
                pick_last=False,
                strict_preferred=True,
            )
            ensure_value_kept(
                "appointments[asc_appointment][time]",
                selected_asc_time,
                root=form,
                as_select=True,
            )
            if has_invalid_time_message():
                log_warning(
                    "Se detecto posible mensaje de hora invalida antes del envio CAS. "
                    "Se continuara y se validara resultado despues del submit."
                )
        except Exception as exc:
            raise RuntimeError(
                "No se pudo determinar una hora CAS habilitada desde la lista de la pagina. "
                f"Detalle: {exc}"
            )

    commit_candidates = (
            form.find_elements(By.XPATH, ".//*[@name='commit' and (self::button or self::input)]")
            + form.find_elements(By.XPATH, ".//button[contains(., 'Reprogramar') or contains(., 'Reschedule')]")
            + form.find_elements(By.XPATH, ".//input[@type='submit' and (@value='Reprogramar' or @value='Reschedule')]")
    )
    preferred_commit_candidates = []
    fallback_commit_candidates = []
    for candidate in commit_candidates:
        label = get_element_action_label(candidate)
        if is_reschedule_action_label(label):
            preferred_commit_candidates.append(candidate)
        else:
            fallback_commit_candidates.append(candidate)

    commit_button = find_first_interactable(preferred_commit_candidates)
    if not commit_button:
        commit_button = find_first_interactable(fallback_commit_candidates)
    if commit_button:
        selected_label = get_element_action_label(commit_button) or "N/D"
        log_info(f"UI submit button selected: {selected_label}")
        driver.execute_script("arguments[0].click();", commit_button)
    elif commit_candidates:
        selected_label = get_element_action_label(commit_candidates[0]) or "N/D"
        log_warning(f"No interactive submit button found. Forcing click on first candidate: {selected_label}")
        driver.execute_script("arguments[0].click();", commit_candidates[0])
    else:
        submit_reprogramar_request(
            consular_facility_id=consular_facility_id,
            consular_date=consular_date,
            consular_time=selected_consular_time,
            asc_facility_id=asc_facility_id,
            asc_date=asc_date,
            asc_time=selected_asc_time,
        )

    time.sleep(STEP_TIME)
    alert_text = ""
    try:
        alert = driver.switch_to.alert
        alert_text = alert.text
        alert.accept()
        log_warning(f"JS alert confirmed: {alert_text}")
    except Exception:
        pass
    Wait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    portal_feedback = extract_portal_feedback()
    if portal_feedback:
        log_info(f"Portal feedback after form submit: {portal_feedback}")
    if contains_invalid_day_text(alert_text) or contains_invalid_day_text(portal_feedback) or has_invalid_day_message():
        raise RuntimeError(
            "El portal reporto que la fecha seleccionada no es valida o es inhabil. "
            f"Alerta: {alert_text or 'N/D'} | Mensaje: {portal_feedback or 'N/D'}"
        )
    if contains_invalid_time_text(alert_text) or contains_invalid_time_text(
            portal_feedback) or has_invalid_time_message():
        raise RuntimeError(
            "El portal reporto que la hora seleccionada no es valida. "
            f"Alerta: {alert_text or 'N/D'} | Mensaje: {portal_feedback or 'N/D'}"
        )
    return selected_consular_time, selected_asc_time


def get_dates_for_facility(facility_id):
    global LAST_DATE_QUERY_SOURCE
    date_url = build_days_url(facility_id)
    validate_ais_url(date_url)
    params = build_request_params([("appointments[expedite]", "false")])
    request_url = build_request_url(date_url, params=params)
    cached_fresh = get_cached_result("dates", request_url, DATE_QUERY_CACHE_TTL_SECONDS)
    if cached_fresh:
        LAST_DATE_QUERY_SOURCE = "cache_fresh"
        cached_data, cache_age = cached_fresh
        log_info(
            f"Consulta de fechas reutiliza cache fresca para sede {facility_id} "
            f"(edad {cache_age:.1f}s)."
        )
        return cached_data

    cooldown_remaining, cooldown_reason = get_api_cooldown_remaining("dates")
    if cooldown_remaining > 0:
        cached_stale = get_cached_result("dates", request_url, DATE_QUERY_CACHE_STALE_GRACE_SECONDS)
        if cached_stale:
            LAST_DATE_QUERY_SOURCE = "cache_stale"
            cached_data, cache_age = cached_stale
            log_warning(
                f"Consulta de fechas omitida por cooldown de red ({cooldown_remaining:.1f}s restantes). "
                f"Se usa cache de {cache_age:.1f}s. Motivo: {cooldown_reason or 'N/D'}"
            )
            return cached_data
        raise SessionRestartRequiredError(
            "Date endpoint in cooldown. "
            f"Retry after {cooldown_remaining:.1f} seconds. Reason: {cooldown_reason or 'N/D'}"
        )

    headers = get_request_headers()
    try:
        data = request_get_json_with_retry(
            date_url,
            headers=headers,
            params=params,
            retries=DATE_ENDPOINT_RETRIES,
            timeout=30,
            restart_session_on_connection_error=True,
        )
        store_cached_result("dates", request_url, data)
        clear_api_cooldown("dates")
        LAST_DATE_QUERY_SOURCE = "network"
        return data
    except AuthSessionError:
        raise
    except SessionRestartRequiredError as exc:
        cooldown_seconds = NETWORK_FAST_FAIL_COOLDOWN_SECONDS
        if is_connection_refused_failure(str(exc)):
            cooldown_seconds = max(cooldown_seconds, NETWORK_FAST_FAIL_COOLDOWN_SECONDS)
        activate_api_cooldown("dates", str(exc), cooldown_seconds)
        cached_stale = get_cached_result("dates", request_url, DATE_QUERY_CACHE_STALE_GRACE_SECONDS)
        if cached_stale:
            LAST_DATE_QUERY_SOURCE = "cache_stale"
            cached_data, cache_age = cached_stale
            log_warning(
                f"Consulta de fechas usa cache tras fallo de red del endpoint "
                f"(edad {cache_age:.1f}s). Detalle: {summarize_error_text(exc)}"
            )
            return cached_data
        raise
    except Exception as exc:
        if is_transient_network_failure(str(exc)):
            activate_api_cooldown("dates", str(exc), NETWORK_FAST_FAIL_COOLDOWN_SECONDS)
            cached_stale = get_cached_result("dates", request_url, DATE_QUERY_CACHE_STALE_GRACE_SECONDS)
            if cached_stale:
                LAST_DATE_QUERY_SOURCE = "cache_stale"
                cached_data, cache_age = cached_stale
                log_warning(
                    f"Consulta de fechas usa cache tras error transitorio "
                    f"(edad {cache_age:.1f}s). Detalle: {summarize_error_text(exc)}"
                )
                return cached_data
        raise RuntimeError(
            f"Date query failed for facility {facility_id}: {exc}"
        ) from exc


def get_valid_times_for_facility(facility_id, date):
    time_url = build_times_url(facility_id, date)
    validate_ais_url(time_url)
    headers = get_request_headers()
    params = build_request_params([("date", date), ("appointments[expedite]", "false")])
    try:
        data = request_get_json_with_retry(
            time_url,
            headers=headers,
            params=params,
            retries=5,
            timeout=30,
        )
    except AuthSessionError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"Time query failed for facility {facility_id} and date {date}: {exc}"
        ) from exc
    available_times = data.get("available_times") or []
    if not available_times:
        return []
    valid_times = [slot for slot in available_times if is_quarter_hour_slot(slot)]
    invalid_times = [slot for slot in available_times if not is_quarter_hour_slot(slot)]
    if invalid_times:
        log_warning(
            "Horas descartadas por no cumplir intervalos de 15 minutos "
            f"(sede {facility_id}, fecha {date}): {', '.join(invalid_times)}"
        )
    if not valid_times:
        raise RuntimeError(
            f"No hay horas validas en intervalos de 15 minutos para sede {facility_id} y fecha {date}."
        )
    return valid_times


def get_time_for_facility(facility_id, date, pick_last=True):
    valid_times = get_valid_times_for_facility(facility_id, date)
    if not valid_times:
        return None
    selected_time = valid_times[-1] if pick_last else valid_times[0]
    log_info(
        "Hora seleccionada valida (cada 15 min) "
        f"para sede {facility_id}, fecha {date}: {selected_time}"
    )
    return selected_time


def get_asc_facility_id():
    asc_facility = driver.find_element(
        by=By.NAME, value='appointments[asc_appointment][facility_id]'
    )
    if asc_facility.tag_name.lower() == "select":
        return Select(asc_facility).first_selected_option.get_attribute("value")
    return asc_facility.get_attribute("value")


def get_consular_facility_id():
    consular_facility = driver.find_element(
        by=By.NAME, value='appointments[consulate_appointment][facility_id]'
    )
    if consular_facility.tag_name.lower() == "select":
        return Select(consular_facility).first_selected_option.get_attribute("value")
    return consular_facility.get_attribute("value")


def build_cas_date_candidates(asc_dates, consular_date):
    if not asc_dates:
        return []
    target = datetime.strptime(consular_date, "%Y-%m-%d")
    parsed_dates = []
    for item in asc_dates:
        date_str = item.get("date")
        if not date_str:
            continue
        try:
            parsed_dates.append(datetime.strptime(date_str, "%Y-%m-%d"))
        except ValueError:
            continue
    if not parsed_dates:
        return []
    parsed_dates.sort()
    no_later_than_consular = [d for d in parsed_dates if d <= target]
    later_than_consular = [d for d in parsed_dates if d > target]
    ordered = sorted(no_later_than_consular, reverse=True) + sorted(later_than_consular)
    ordered_str = []
    seen = set()
    for item in ordered:
        date_value = item.strftime("%Y-%m-%d")
        if date_value not in seen:
            ordered_str.append(date_value)
            seen.add(date_value)
    return ordered_str


def reschedule_cas(consular_date, consular_time):
    try:
        log_info(f"Waiting {CAS_DELAY_AFTER_CONSULAR} seconds before CAS scheduling.")
        time.sleep(CAS_DELAY_AFTER_CONSULAR)
        consular_facility_id = get_consular_facility_id() or str(FACILITY_ID)

        try:
            asc_facility_id = get_asc_facility_id()
        except Exception:
            return False, "No se encontro el campo de sede CAS."
        if not asc_facility_id:
            return False, "El ID de sede CAS esta vacio."

        asc_dates = get_dates_for_facility(asc_facility_id)
        if not isinstance(asc_dates, list) or not asc_dates:
            return False, "No hay fechas disponibles para CAS."
        cas_date_candidates = build_cas_date_candidates(asc_dates, consular_date)
        limited_cas_dates = cas_date_candidates[:max(1, MAX_CAS_DATES_PER_CYCLE)]
        if not limited_cas_dates:
            return False, "No fue posible seleccionar fecha para CAS."
        log_info(f"CAS candidate dates: {', '.join(limited_cas_dates)}")
        cas_failures = []
        for date_index, asc_date in enumerate(limited_cas_dates, start=1):
            try:
                asc_times = get_valid_times_for_facility(asc_facility_id, asc_date)
            except Exception as exc:
                cas_failures.append(
                    f"[{asc_date}] No fue posible obtener horas CAS: {exc.__class__.__name__}: {exc}"
                )
                continue
            if not asc_times:
                cas_failures.append(f"[{asc_date}] No hay horas CAS disponibles.")
                continue
            log_info(
                f"CAS fecha {date_index}/{len(limited_cas_dates)} -> horas candidatas: {', '.join(asc_times)}"
            )
            for time_index, asc_time in enumerate(asc_times, start=1):
                attempt_msg = (
                    f"CAS intento {time_index}/{len(asc_times)} "
                    f"con fecha {asc_date} y hora {asc_time}"
                )
                log_info(attempt_msg)
                try:
                    submitted_consular_time, submitted_asc_time = submit_reprogramar_form(
                        consular_facility_id=consular_facility_id,
                        consular_date=consular_date,
                        consular_time=consular_time,
                        asc_facility_id=asc_facility_id,
                        asc_date=asc_date,
                        asc_time=asc_time,
                    )
                    headers = get_request_headers()
                    cas_validated, validation_msg = validate_post_reschedule(
                        headers,
                        expected_consular_date=consular_date,
                        expected_cas_date=asc_date,
                    )
                    if cas_validated:
                        return True, (
                            f"Cita CAS reagendada correctamente: {asc_date} {submitted_asc_time or asc_time}\n"
                            f"{validation_msg}"
                        )
                    diag = collect_page_structure_report()
                    cas_failures.append(
                        (
                            f"[{asc_date} {asc_time}] No confirmado por estado actual.\n"
                            f"{validation_msg}\n{diag}"
                        )
                    )
                except Exception as exc:
                    diag = collect_page_structure_report()
                    cas_failures.append(
                        f"[{asc_date} {asc_time}] Excepcion CAS: {exc.__class__.__name__}: {exc}\n{diag}"
                    )
                if (time_index < len(asc_times)) or (date_index < len(limited_cas_dates)):
                    try:
                        ensure_appointment_page_ready(force_navigate=False)
                    except Exception:
                        ensure_appointment_page_ready(force_navigate=True)
        return False, (
                "No fue posible confirmar CAS despues de evaluar fechas/horas candidatas.\n"
                + "\n".join(cas_failures)
        )
    except Exception as exc:
        diag = collect_page_structure_report()
        return False, (
            f"Excepcion en flujo CAS: {exc.__class__.__name__}: {exc}\n"
            f"{diag}"
        )


def finalize_consular_success(consular_facility_id, consular_date, consular_time, validation_msg):
    consular_msg = (
        f"Cita consular reagendada correctamente: {consular_date} {consular_time} "
        f"(sede {consular_facility_id})\n"
        f"{validation_msg}"
    )
    cas_ok, cas_msg = reschedule_cas(consular_date, consular_time)
    if cas_ok:
        return ["SUCCESS", f"{consular_msg}\n{cas_msg}"]
    return ["PARTIAL_SUCCESS", f"{consular_msg}\n{cas_msg}"]


def validate_consular_status_with_rechecks(expected_consular_date):
    validation_msg = "Validacion posterior no disponible."
    for attempt in range(1, max(1, DIRECT_STATUS_RECHECKS) + 1):
        headers = get_request_headers()
        consular_validated, validation_msg = validate_post_reschedule(
            headers,
            expected_consular_date=expected_consular_date,
            expected_cas_date=None,
        )
        if consular_validated:
            return True, validation_msg
        if attempt < max(1, DIRECT_STATUS_RECHECKS):
            time.sleep(max(0.1, DIRECT_STATUS_RECHECK_WAIT_SECONDS))
    return False, validation_msg


def attempt_consular_direct_submit(consular_facility_id, consular_date, consular_time):
    asc_facility_id = get_field_value("appointments[asc_appointment][facility_id]")
    asc_date_value = get_field_value("appointments[asc_appointment][date]")
    asc_time_value = get_field_value("appointments[asc_appointment][time]")
    status_code, feedback = submit_reprogramar_request(
        consular_facility_id=consular_facility_id,
        consular_date=consular_date,
        consular_time=consular_time,
        asc_facility_id=asc_facility_id,
        asc_date=asc_date_value,
        asc_time=asc_time_value,
    )
    if feedback:
        log_warning(f"Direct server submit feedback for consular: {feedback}")
    else:
        log_info(
            "Direct server submit completed for consular "
            f"with HTTP {status_code} and no portal feedback."
        )
    consular_validated, validation_msg = validate_consular_status_with_rechecks(consular_date)
    return consular_validated, validation_msg, status_code, feedback


def reschedule(date):
    try:
        ensure_appointment_page_ready(force_navigate=False)
        consular_facility_id = get_consular_facility_id() or str(FACILITY_ID)
        consular_times = get_valid_times_for_facility(consular_facility_id, date)
        if not consular_times:
            return ["FAIL", f"No hay hora consular disponible para {date}."]
        log_info(f"Consular candidate times for {date}: {', '.join(consular_times)}")
        failures = []
        for index, consular_time in enumerate(consular_times, start=1):
            log_info(
                f"Consular intento {index}/{len(consular_times)} "
                f"con fecha {date} y hora {consular_time}"
            )
            direct_context = ""
            try:
                if DIRECT_SUBMIT_FIRST:
                    direct_validated, validation_msg, direct_status, _direct_feedback = attempt_consular_direct_submit(
                        consular_facility_id=consular_facility_id,
                        consular_date=date,
                        consular_time=consular_time,
                    )
                    if direct_validated:
                        return finalize_consular_success(
                            consular_facility_id=consular_facility_id,
                            consular_date=date,
                            consular_time=consular_time,
                            validation_msg=validation_msg,
                        )
                    direct_context = (
                        f"Direct pre-submit no confirmado (HTTP {direct_status}). {validation_msg}"
                    )
                    log_warning(
                        f"Direct pre-submit not confirmed for {date} {consular_time}. "
                        f"HTTP {direct_status}."
                    )
                    if not DIRECT_SUBMIT_UI_FALLBACK:
                        diag = collect_page_structure_report()
                        failures.append(
                            (
                                f"[{consular_time}] No confirmado por estado actual.\n"
                                f"{validation_msg}\n{diag}"
                            )
                        )
                        continue

                submitted_consular_time, _submitted_asc_time = submit_reprogramar_form(
                    consular_facility_id=consular_facility_id,
                    consular_date=date,
                    consular_time=consular_time,
                    asc_facility_id=None,
                    asc_date=None,
                    asc_time=None,
                )
                consular_validated, validation_msg = validate_consular_status_with_rechecks(date)
                if not consular_validated and not DIRECT_SUBMIT_FIRST:
                    log_warning(
                        "Consular not confirmed after UI submit. "
                        f"Trying direct server submit for {date} {submitted_consular_time or consular_time}."
                    )
                    try:
                        consular_validated, validation_msg, fallback_status, _fallback_feedback = attempt_consular_direct_submit(
                            consular_facility_id=consular_facility_id,
                            consular_date=date,
                            consular_time=submitted_consular_time or consular_time,
                        )
                        direct_context = (
                            f"Direct fallback after UI no confirmado (HTTP {fallback_status}). "
                            f"{validation_msg}"
                        )
                    except Exception as fallback_exc:
                        log_warning(
                            f"Direct server submit fallback failed for consular: {fallback_exc}"
                        )
                if not consular_validated:
                    diag = collect_page_structure_report()
                    failure_msg = (
                        f"[{consular_time}] No confirmado por estado actual.\n"
                        f"{validation_msg}\n{diag}"
                    )
                    if direct_context:
                        failure_msg = f"{failure_msg}\n{direct_context}"
                    failures.append(failure_msg)
                    continue

                return finalize_consular_success(
                    consular_facility_id=consular_facility_id,
                    consular_date=date,
                    consular_time=submitted_consular_time or consular_time,
                    validation_msg=validation_msg,
                )
            except Exception as exc:
                diag = collect_page_structure_report()
                failure_msg = (
                    f"[{consular_time}] Excepcion consular: {exc.__class__.__name__}: {exc}\n{diag}"
                )
                if direct_context:
                    failure_msg = f"{failure_msg}\n{direct_context}"
                failures.append(failure_msg)
            if index < len(consular_times):
                try:
                    ensure_appointment_page_ready(force_navigate=False)
                except Exception:
                    ensure_appointment_page_ready(force_navigate=True)
        return [
            "FAIL",
            (
                    f"No fue posible reagendar cita consular para {date} despues de {len(consular_times)} horas intentadas.\n"
                    + "\n".join(failures)
            ),
        ]
    except Exception as exc:
        diag = collect_page_structure_report()
        return [
            "FAIL",
            (
                f"Excepcion durante reagendado: {exc.__class__.__name__}: {exc}\n"
                f"{diag}"
            ),
        ]


def get_date():
    # Requesting to get the whole available dates for consular
    try:
        consular_facility_id = get_consular_facility_id() or str(FACILITY_ID)
    except Exception:
        consular_facility_id = str(FACILITY_ID)
    return get_dates_for_facility(consular_facility_id)


def get_time(date):
    try:
        consular_facility_id = get_consular_facility_id() or str(FACILITY_ID)
    except Exception:
        consular_facility_id = str(FACILITY_ID)
    consular_time = get_time_for_facility(consular_facility_id, date, pick_last=False)
    if consular_time:
        log_info(f"Got consular time successfully: {date} {consular_time}")
    return consular_time


def is_logged_in():
    content = driver.page_source
    if (content.find("error") != -1):
        return False
    return True


def get_available_dates(dates):
    # Evaluation of different available dates
    def is_in_period(date, PSD, PED):
        new_date = datetime.strptime(date, "%Y-%m-%d")
        result = (PSD <= new_date <= PED)
        return result

    PED = datetime.strptime(PRIOD_END, "%Y-%m-%d")
    PSD = datetime.strptime(PRIOD_START, "%Y-%m-%d")
    candidates = []
    for d in dates:
        date = d.get('date')
        if is_in_period(date, PSD, PED):
            candidates.append(date)
    if candidates:
        return candidates
    log_info(f"No available dates between {PSD.date()} and {PED.date()}.")
    return []


def try_reschedule_candidates(candidate_dates):
    limited_candidates = candidate_dates[:max(1, MAX_DATES_PER_CYCLE)]
    failures = []
    for index, candidate_date in enumerate(limited_candidates, start=1):
        msg = f"Intento {index}/{len(limited_candidates)} de reagendado con fecha: {candidate_date}"
        log_info(msg)
        info_logger(LOG_FILE_NAME, msg)
        result_title, result_msg = reschedule(candidate_date)
        if result_title in {"SUCCESS", "PARTIAL_SUCCESS"}:
            return result_title, result_msg
        failures.append(f"[{candidate_date}] {result_msg}")
        try:
            ensure_appointment_page_ready(force_navigate=False)
        except Exception as exc:
            recover_msg = f"Recuperacion suave fallo: {exc}. Se intenta recarga forzada."
            log_warning(recover_msg)
            info_logger(LOG_FILE_NAME, recover_msg)
            try:
                ensure_appointment_page_ready(force_navigate=True)
            except Exception as forced_exc:
                recover_msg = f"Fallo al reabrir formulario tras intento fallido: {forced_exc}"
                log_warning(recover_msg)
                info_logger(LOG_FILE_NAME, recover_msg)
    failure_summary = (
            f"No fue posible reagendar despues de {len(limited_candidates)} intentos en este ciclo.\n"
            + "\n".join(failures)
    )
    return "FAIL", failure_summary


def info_logger(file_path, log):
    # file_path: e.g. "log.txt"
    safe_log = sanitize_text(log)
    with open(file_path, "a") as file:
        file.write(str(datetime.now().time()) + ":\n" + safe_log + "\n")


def is_pid_running(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def acquire_run_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r", encoding="utf-8") as lock_fp:
                existing_pid = int((lock_fp.read() or "0").strip() or "0")
        except Exception:
            existing_pid = 0
        if existing_pid and is_pid_running(existing_pid):
            raise RuntimeError(f"Ya existe una ejecucion activa (PID {existing_pid}).")
        try:
            os.remove(LOCK_FILE)
        except Exception:
            pass
    with open(LOCK_FILE, "w", encoding="utf-8") as lock_fp:
        lock_fp.write(str(os.getpid()))


def release_run_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass


def cleanup_driver():
    try:
        handles = driver.window_handles
        if handles:
            primary = handles[0]
            for handle in handles[1:]:
                try:
                    driver.switch_to.window(handle)
                    driver.close()
                except Exception:
                    continue
            try:
                driver.switch_to.window(primary)
            except Exception:
                pass
    except Exception:
        pass
    try:
        navigate_ais_page(SIGN_OUT_LINK, attempts=2)
    except Exception:
        pass
    try:
        driver.stop_client()
    except Exception:
        pass
    try:
        driver.quit()
    except Exception:
        pass


def restart_browser_with_pause(reason, wait_seconds):
    global driver
    global USER_AGENT_CACHE
    wait_seconds = max(1.0, wait_seconds)
    restart_msg = (
        f"Se ejecuta reinicio completo del navegador por '{reason}'. "
        f"Espera de {wait_seconds:.0f} segundos antes de reabrir."
    )
    log_warning(restart_msg)
    try:
        info_logger(LOG_FILE_NAME, restart_msg)
    except Exception:
        pass
    cleanup_driver()
    time.sleep(wait_seconds)
    driver = create_webdriver()
    USER_AGENT_CACHE = ""
    log_info("Navegador reabierto tras reinicio completo.")


def full_browser_restart_with_pause(reason):
    restart_browser_with_pause(reason, FULL_BROWSER_RESTART_WAIT_SECONDS)


def prune_restart_events(restart_events, window_seconds):
    now = time.time()
    return [item for item in (restart_events or []) if now - item <= window_seconds]


def should_force_full_browser_restart(consecutive_exception_streak, reason, restart_events=None):
    if consecutive_exception_streak < FULL_BROWSER_RESTART_FAILURE_STREAK:
        return False
    trigger_msg = (
        f"Se alcanzaron {consecutive_exception_streak} excepciones consecutivas. "
        "Se forzara cierre total del proceso de navegador."
    )
    log_warning(trigger_msg)
    try:
        info_logger(LOG_FILE_NAME, trigger_msg)
    except Exception:
        pass
    try:
        full_browser_restart_with_pause(reason)
        if restart_events is not None:
            restart_events.append(time.time())
        return True
    except Exception as exc:
        failure_msg = (
            "No se pudo completar reinicio completo del navegador: "
            f"{exc.__class__.__name__}: {sanitize_text(exc)}"
        )
        log_error(failure_msg)
        try:
            info_logger(LOG_FILE_NAME, failure_msg)
        except Exception:
            pass
        return False


def maybe_apply_long_cooldown(
        reason,
        restart_events=None,
        force=False,
        cooldown_seconds=None,
):
    events = prune_restart_events(restart_events or [], LONG_COOLDOWN_WINDOW_SECONDS)
    if restart_events is not None:
        restart_events[:] = events
    threshold_reached = len(events) >= LONG_COOLDOWN_HARD_RESTART_THRESHOLD
    if not force and not threshold_reached:
        return False
    wait_seconds = max(60.0, cooldown_seconds or LONG_COOLDOWN_SECONDS)
    msg = (
        f"Se activa cooldown largo por '{reason}'. "
        f"Pausa de {wait_seconds:.0f} segundos antes de retomar consultas."
    )
    log_warning(msg)
    try:
        info_logger(LOG_FILE_NAME, msg)
    except Exception:
        pass
    send_notification("BAN", msg)
    try:
        restart_browser_with_pause(f"cooldown largo - {reason}", wait_seconds)
    except Exception as exc:
        failure_msg = (
            "No se pudo ejecutar cooldown largo con reinicio completo: "
            f"{exc.__class__.__name__}: {sanitize_text(exc)}"
        )
        log_error(failure_msg)
        try:
            info_logger(LOG_FILE_NAME, failure_msg)
        except Exception:
            pass
    if restart_events is not None:
        restart_events.clear()
    return True


def run_scheduler():
    global LOG_FILE_NAME
    first_loop = True
    END_MSG_TITLE = "INFO"
    msg = "Proceso finalizado."
    transient_block_streak = 0
    auth_recovery_streak = 0
    transient_network_streak = 0
    ip_rotation_attempts = 0
    unexpected_error_streak = 0
    driver_session_streak = 0
    consecutive_exception_streak = 0
    degraded_mode = False
    degraded_failure_streak = 0
    degraded_success_streak = 0
    full_restart_events = []

    def reset_failure_streaks():
        nonlocal transient_block_streak
        nonlocal auth_recovery_streak
        nonlocal transient_network_streak
        nonlocal ip_rotation_attempts
        nonlocal unexpected_error_streak
        nonlocal driver_session_streak
        nonlocal consecutive_exception_streak
        nonlocal degraded_failure_streak
        nonlocal degraded_success_streak
        transient_block_streak = 0
        auth_recovery_streak = 0
        transient_network_streak = 0
        ip_rotation_attempts = 0
        unexpected_error_streak = 0
        driver_session_streak = 0
        consecutive_exception_streak = 0
        degraded_failure_streak = 0
        degraded_success_streak = 0

    acquire_run_lock()
    try:
        while 1:
            LOG_FILE_NAME = "log_" + str(datetime.now().date()) + ".txt"
            if first_loop:
                t0 = time.time()
                total_time = 0
                Req_count = 0
                try:
                    start_process()
                    unexpected_error_streak = 0
                    driver_session_streak = 0
                    consecutive_exception_streak = 0
                    degraded_failure_streak = 0
                    if degraded_mode:
                        degraded_success_streak += 1
                        if degraded_success_streak >= DEGRADED_MODE_RECOVERY_SUCCESS_STREAK:
                            degraded_mode = False
                            degraded_success_streak = 0
                            log_info("Modo degradado desactivado tras recuperacion estable en inicio.")
                    else:
                        degraded_success_streak = 0
                except Exception as exc:
                    error_trace = traceback.format_exc()
                    msg = (
                        f"Startup failed: {exc.__class__.__name__}: {exc}\n"
                        f"{error_trace}\n"
                    )
                    if is_view_limit_block_failure(msg):
                        degraded_mode = True
                        degraded_failure_streak += 1
                        degraded_success_streak = 0
                        transient_block_streak += 1
                        consecutive_exception_streak += 1
                        maybe_apply_long_cooldown(
                            reason="inicio - limite de vistas",
                            restart_events=full_restart_events,
                            force=True,
                            cooldown_seconds=VIEW_LIMIT_COOLDOWN_SECONDS,
                        )
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    if is_driver_session_failure(msg):
                        driver_session_streak += 1
                        consecutive_exception_streak += 1
                        degraded_failure_streak += 1
                        degraded_success_streak = 0
                        if degraded_failure_streak >= DEGRADED_MODE_FAILURE_STREAK and not degraded_mode:
                            degraded_mode = True
                            log_warning("Modo degradado activado por inestabilidad de sesion WebDriver.")
                        base_wait_seconds = min(
                            120.0,
                            max(5.0, AUTH_RECOVERY_WAIT_SECONDS) * (2 ** min(4, driver_session_streak - 1)),
                        )
                        wait_seconds = compute_adaptive_wait_seconds(
                            base_wait_seconds,
                            degraded_mode=degraded_mode,
                            failure_streak=degraded_failure_streak,
                        )
                        restart_msg = (
                            f"Sesion WebDriver invalida en inicio (intento {driver_session_streak}). "
                            f"Se reinicia sesion y se reintenta en {wait_seconds:.1f} segundos."
                        )
                        log_warning(restart_msg)
                        try:
                            info_logger(LOG_FILE_NAME, restart_msg)
                        except Exception:
                            pass
                        try:
                            reset_webdriver_session(reason="sesion webdriver invalida en inicio")
                        except Exception:
                            pass
                        if driver_session_streak >= LONG_COOLDOWN_NETWORK_STREAK:
                            maybe_apply_long_cooldown(
                                reason="inicio - sesion webdriver inestable",
                                restart_events=full_restart_events,
                                force=True,
                                cooldown_seconds=LONG_COOLDOWN_SECONDS,
                            )
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        if should_force_full_browser_restart(
                                consecutive_exception_streak,
                                "inicio - sesion webdriver invalida",
                                restart_events=full_restart_events,
                        ):
                            consecutive_exception_streak = 0
                            if maybe_apply_long_cooldown(
                                    reason="inicio - reinicios duros consecutivos",
                                    restart_events=full_restart_events,
                            ):
                                reset_failure_streaks()
                                first_loop = True
                                continue
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        time.sleep(wait_seconds)
                        first_loop = True
                        continue
                    if is_page_unresponsive_failure(msg):
                        transient_network_streak += 1
                        consecutive_exception_streak += 1
                        degraded_failure_streak += 1
                        degraded_success_streak = 0
                        if degraded_failure_streak >= DEGRADED_MODE_FAILURE_STREAK and not degraded_mode:
                            degraded_mode = True
                            log_warning("Modo degradado activado por fallos de red consecutivos.")
                        base_wait_seconds = min(
                            300.0,
                            max(5.0, compute_retry_wait_seconds()) * (2 ** (transient_network_streak - 1)),
                        )
                        wait_seconds = compute_adaptive_wait_seconds(
                            base_wait_seconds,
                            degraded_mode=degraded_mode,
                            failure_streak=degraded_failure_streak,
                        )
                        network_msg = (
                            f"Pagina no responde o fallo de red detectado en inicio (intento {transient_network_streak}). "
                            f"Reintento en {wait_seconds:.1f} segundos."
                        )
                        log_warning(network_msg)
                        ip_rotation_attempts = maybe_rotate_ip_on_network_failure(
                            network_streak=max(transient_network_streak, IP_ROTATION_TRIGGER_STREAK),
                            rotation_attempts=ip_rotation_attempts,
                            context_label="inicio - pagina no responde",
                        )
                        try:
                            info_logger(LOG_FILE_NAME, network_msg)
                        except Exception:
                            pass
                        if transient_network_streak >= LONG_COOLDOWN_NETWORK_STREAK:
                            maybe_apply_long_cooldown(
                                reason="inicio - racha de red",
                                restart_events=full_restart_events,
                                force=True,
                                cooldown_seconds=LONG_COOLDOWN_SECONDS,
                            )
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        if should_force_full_browser_restart(
                                consecutive_exception_streak,
                                "inicio - pagina no responde",
                                restart_events=full_restart_events,
                        ):
                            consecutive_exception_streak = 0
                            if maybe_apply_long_cooldown(
                                    reason="inicio - reinicios duros consecutivos",
                                    restart_events=full_restart_events,
                            ):
                                reset_failure_streaks()
                                first_loop = True
                                continue
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        time.sleep(wait_seconds)
                        first_loop = True
                        continue
                    if is_transient_block_failure(msg):
                        transient_block_streak += 1
                        consecutive_exception_streak += 1
                        degraded_failure_streak += 1
                        degraded_success_streak = 0
                        if degraded_failure_streak >= DEGRADED_MODE_FAILURE_STREAK and not degraded_mode:
                            degraded_mode = True
                            log_warning("Modo degradado activado por bloqueos consecutivos.")
                        cooldown_hours = BLOCK_COOLDOWN_TIME * min(8, (2 ** (transient_block_streak - 1)))
                        block_msg = (
                            f"Bloqueo temporal detectado en inicio (intento {transient_block_streak}). "
                            f"Se aplicara pausa de {cooldown_hours:.2f} horas."
                        )
                        log_warning(block_msg)
                        try:
                            info_logger(LOG_FILE_NAME, block_msg)
                        except Exception:
                            pass
                        send_notification("BAN", block_msg)
                        try:
                            navigate_ais_page(SIGN_OUT_LINK, attempts=2)
                        except Exception:
                            pass
                        if transient_block_streak >= LONG_COOLDOWN_BLOCK_STREAK:
                            maybe_apply_long_cooldown(
                                reason="inicio - bloqueos consecutivos",
                                restart_events=full_restart_events,
                                force=True,
                                cooldown_seconds=LONG_COOLDOWN_SECONDS,
                            )
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        if should_force_full_browser_restart(
                                consecutive_exception_streak,
                                "inicio - bloqueo temporal",
                                restart_events=full_restart_events,
                        ):
                            consecutive_exception_streak = 0
                            if maybe_apply_long_cooldown(
                                    reason="inicio - reinicios duros consecutivos",
                                    restart_events=full_restart_events,
                            ):
                                reset_failure_streaks()
                                first_loop = True
                                continue
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        time.sleep(cooldown_hours * hour)
                        first_loop = True
                        continue
                    unexpected_error_streak += 1
                    consecutive_exception_streak += 1
                    degraded_failure_streak += 1
                    degraded_success_streak = 0
                    if degraded_failure_streak >= DEGRADED_MODE_FAILURE_STREAK and not degraded_mode:
                        degraded_mode = True
                        log_warning("Modo degradado activado por excepciones inesperadas.")
                    base_wait_seconds = min(
                        300.0,
                        max(5.0, compute_retry_wait_seconds()) * (2 ** min(6, unexpected_error_streak - 1)),
                    )
                    wait_seconds = compute_adaptive_wait_seconds(
                        base_wait_seconds,
                        degraded_mode=degraded_mode,
                        failure_streak=degraded_failure_streak,
                    )
                    recover_msg = (
                        f"Error inesperado en inicio (intento {unexpected_error_streak}). "
                        f"Se reinicia sesion y se reintenta en {wait_seconds:.1f} segundos."
                    )
                    log_warning(recover_msg)
                    try:
                        info_logger(LOG_FILE_NAME, msg)
                        info_logger(LOG_FILE_NAME, recover_msg)
                    except Exception:
                        pass
                    try:
                        reset_webdriver_session(reason="error inesperado en inicio")
                    except Exception:
                        pass
                    if should_force_full_browser_restart(
                            consecutive_exception_streak,
                            "inicio - excepcion inesperada",
                            restart_events=full_restart_events,
                    ):
                        consecutive_exception_streak = 0
                        if maybe_apply_long_cooldown(
                                reason="inicio - reinicios duros consecutivos",
                                restart_events=full_restart_events,
                        ):
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    first_loop = True
                    time.sleep(wait_seconds)
                    continue
                first_loop = False
            Req_count += 1
            try:
                msg = "-" * 60 + f"\nRequest count: {Req_count}, Log time: {datetime.today()}\n"
                log_info(msg)
                info_logger(LOG_FILE_NAME, msg)
                dates = get_date()
                unexpected_error_streak = 0
                driver_session_streak = 0
                consecutive_exception_streak = 0
                if LAST_DATE_QUERY_SOURCE == "network":
                    transient_network_streak = 0
                    ip_rotation_attempts = 0
                    degraded_failure_streak = 0
                    if degraded_mode:
                        degraded_success_streak += 1
                        if degraded_success_streak >= DEGRADED_MODE_RECOVERY_SUCCESS_STREAK:
                            degraded_mode = False
                            degraded_success_streak = 0
                            log_info("Modo degradado desactivado tras consultas estables.")
                    else:
                        degraded_success_streak = 0
                else:
                    log_warning(
                        "Consulta de fechas servida desde cache. "
                        f"Fuente: {LAST_DATE_QUERY_SOURCE}. Se conserva backoff actual."
                    )
                if has_view_limit_banner():
                    degraded_mode = True
                    maybe_apply_long_cooldown(
                        reason="ciclo - banner de limite de vistas",
                        restart_events=full_restart_events,
                        force=True,
                        cooldown_seconds=VIEW_LIMIT_COOLDOWN_SECONDS,
                    )
                    reset_failure_streaks()
                    first_loop = True
                    continue
                if not dates:
                    transient_block_streak += 1
                    degraded_mode = True
                    degraded_failure_streak += 1
                    degraded_success_streak = 0
                    msg = (
                        f"La lista de fechas esta vacia. Posible bloqueo temporal.\n"
                        f"\tPausa por {BAN_COOLDOWN_TIME} horas.\n"
                    )
                    log_warning(msg)
                    info_logger(LOG_FILE_NAME, msg)
                    send_notification("BAN", msg)
                    navigate_ais_page(SIGN_OUT_LINK, attempts=2)
                    if transient_block_streak >= LONG_COOLDOWN_BLOCK_STREAK:
                        maybe_apply_long_cooldown(
                            reason="ciclo - fechas vacias consecutivas",
                            restart_events=full_restart_events,
                            force=True,
                            cooldown_seconds=LONG_COOLDOWN_SECONDS,
                        )
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    time.sleep(BAN_COOLDOWN_TIME * hour)
                    first_loop = True
                else:
                    transient_block_streak = 0
                    auth_recovery_streak = 0
                    msg = ""
                    for d in dates:
                        msg = msg + "%s" % (d.get('date')) + ", "
                    msg = "Available dates:\n" + msg
                    log_info(msg)
                    info_logger(LOG_FILE_NAME, msg)
                    candidate_dates = get_available_dates(dates)
                    if candidate_dates:
                        msg = (
                                "Fechas candidatas en rango: "
                                + ", ".join(candidate_dates[:max(1, MAX_DATES_PER_CYCLE)])
                        )
                        log_info(msg)
                        info_logger(LOG_FILE_NAME, msg)
                        send_notification("TENTATIVE_DATE", msg)
                        effective_candidate_dates = candidate_dates
                        if degraded_mode:
                            effective_candidate_dates = candidate_dates[:DEGRADED_MODE_MAX_DATES_PER_CYCLE]
                            degrade_msg = (
                                "Modo degradado activo: se limita intento de reagendado a "
                                f"{len(effective_candidate_dates)} fecha(s) para priorizar efectividad."
                            )
                            log_warning(degrade_msg)
                            info_logger(LOG_FILE_NAME, degrade_msg)
                        END_MSG_TITLE, msg = try_reschedule_candidates(effective_candidate_dates)
                        if END_MSG_TITLE in {"SUCCESS", "PARTIAL_SUCCESS"}:
                            break
                        log_warning(msg)
                        info_logger(LOG_FILE_NAME, msg)
                    RETRY_WAIT_TIME = compute_adaptive_wait_seconds(
                        RETRY_TIME,
                        degraded_mode=degraded_mode,
                        failure_streak=degraded_failure_streak,
                    )
                    t1 = time.time()
                    total_time = t1 - t0
                    msg = "\nTiempo de trabajo: ~ {:.2f} minutos".format(total_time / minute)
                    log_info(msg)
                    info_logger(LOG_FILE_NAME, msg)
                    if total_time > WORK_LIMIT_TIME * hour:
                        send_notification(
                            "REST",
                            f"Pausa despues de {WORK_LIMIT_TIME} horas | Ciclos ejecutados: {Req_count}",
                        )
                        navigate_ais_page(SIGN_OUT_LINK, attempts=2)
                        time.sleep(WORK_COOLDOWN_TIME * hour)
                        first_loop = True
                    else:
                        base_wait_seconds = compute_retry_wait_seconds()
                        RETRY_WAIT_TIME = compute_adaptive_wait_seconds(
                            base_wait_seconds,
                            degraded_mode=degraded_mode,
                            failure_streak=degraded_failure_streak,
                        )
                        msg = "Espera antes de reintento: " + str(round(RETRY_WAIT_TIME, 2)) + " segundos"
                        log_info(msg)
                        info_logger(LOG_FILE_NAME, msg)
                        time.sleep(RETRY_WAIT_TIME)
            except Exception as exc:
                error_trace = traceback.format_exc()
                msg = (
                    f"Se detuvo el ciclo por excepcion: {exc.__class__.__name__}: {exc}\n"
                    f"{error_trace}\n"
                )
                if is_view_limit_block_failure(msg):
                    transient_block_streak += 1
                    consecutive_exception_streak += 1
                    degraded_mode = True
                    degraded_failure_streak += 1
                    degraded_success_streak = 0
                    maybe_apply_long_cooldown(
                        reason="ciclo - limite de vistas",
                        restart_events=full_restart_events,
                        force=True,
                        cooldown_seconds=VIEW_LIMIT_COOLDOWN_SECONDS,
                    )
                    reset_failure_streaks()
                    first_loop = True
                    continue
                if is_driver_session_failure(msg):
                    driver_session_streak += 1
                    consecutive_exception_streak += 1
                    degraded_failure_streak += 1
                    degraded_success_streak = 0
                    if degraded_failure_streak >= DEGRADED_MODE_FAILURE_STREAK and not degraded_mode:
                        degraded_mode = True
                        log_warning("Modo degradado activado por inestabilidad de sesion WebDriver.")
                    base_wait_seconds = min(
                        120.0,
                        max(5.0, AUTH_RECOVERY_WAIT_SECONDS) * (2 ** min(4, driver_session_streak - 1)),
                    )
                    wait_seconds = compute_adaptive_wait_seconds(
                        base_wait_seconds,
                        degraded_mode=degraded_mode,
                        failure_streak=degraded_failure_streak,
                    )
                    restart_msg = (
                        f"Sesion WebDriver invalida en ciclo (intento {driver_session_streak}). "
                        f"Se reinicia sesion y se reintenta en {wait_seconds:.1f} segundos."
                    )
                    log_warning(restart_msg)
                    info_logger(LOG_FILE_NAME, restart_msg)
                    try:
                        reset_webdriver_session(reason="sesion webdriver invalida en ciclo")
                    except Exception:
                        pass
                    if driver_session_streak >= LONG_COOLDOWN_NETWORK_STREAK:
                        maybe_apply_long_cooldown(
                            reason="ciclo - sesion webdriver inestable",
                            restart_events=full_restart_events,
                            force=True,
                            cooldown_seconds=LONG_COOLDOWN_SECONDS,
                        )
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    if should_force_full_browser_restart(
                            consecutive_exception_streak,
                            "ciclo - sesion webdriver invalida",
                            restart_events=full_restart_events,
                    ):
                        consecutive_exception_streak = 0
                        if maybe_apply_long_cooldown(
                                reason="ciclo - reinicios duros consecutivos",
                                restart_events=full_restart_events,
                        ):
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    first_loop = True
                    time.sleep(wait_seconds)
                    continue
                if isinstance(exc, SessionRestartRequiredError):
                    transient_network_streak += 1
                    consecutive_exception_streak += 1
                    degraded_failure_streak += 1
                    degraded_success_streak = 0
                    if degraded_failure_streak >= DEGRADED_MODE_FAILURE_STREAK and not degraded_mode:
                        degraded_mode = True
                        log_warning("Modo degradado activado por reinicios de sesion consecutivos.")
                    base_wait_seconds = min(
                        180.0,
                        max(5.0, compute_retry_wait_seconds()) * (2 ** (transient_network_streak - 1)),
                    )
                    wait_seconds = compute_adaptive_wait_seconds(
                        base_wait_seconds,
                        degraded_mode=degraded_mode,
                        failure_streak=degraded_failure_streak,
                    )
                    restart_msg = (
                        "ConnectionError persistente en endpoint de fechas. "
                        f"{'Se conserva la sesion actual' if is_webdriver_session_alive() else 'Se reiniciara la sesion'} "
                        f"(intento {transient_network_streak}) y reintento en {wait_seconds:.1f} segundos."
                    )
                    log_warning(restart_msg)
                    info_logger(LOG_FILE_NAME, restart_msg)
                    ip_rotation_attempts = maybe_rotate_ip_on_network_failure(
                        network_streak=transient_network_streak,
                        rotation_attempts=ip_rotation_attempts,
                        context_label="endpoint de fechas",
                    )
                    preserve_current_session = is_webdriver_session_alive()
                    if not preserve_current_session:
                        try:
                            reset_webdriver_session(reason="endpoint de fechas - sesion no reutilizable")
                        except Exception:
                            pass
                    if transient_network_streak >= LONG_COOLDOWN_NETWORK_STREAK:
                        maybe_apply_long_cooldown(
                            reason="ciclo - SessionRestartRequired recurrente",
                            restart_events=full_restart_events,
                            force=True,
                            cooldown_seconds=LONG_COOLDOWN_SECONDS,
                        )
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    if should_force_full_browser_restart(
                            consecutive_exception_streak,
                            "ciclo - SessionRestartRequired",
                            restart_events=full_restart_events,
                    ):
                        consecutive_exception_streak = 0
                        if maybe_apply_long_cooldown(
                                reason="ciclo - reinicios duros consecutivos",
                                restart_events=full_restart_events,
                        ):
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    first_loop = not preserve_current_session
                    time.sleep(wait_seconds)
                    continue
                if isinstance(exc, AuthSessionError) or is_auth_session_failure(str(exc)):
                    auth_recovery_streak += 1
                    consecutive_exception_streak += 1
                    degraded_failure_streak += 1
                    degraded_success_streak = 0
                    if degraded_failure_streak >= DEGRADED_MODE_FAILURE_STREAK and not degraded_mode:
                        degraded_mode = True
                        log_warning("Modo degradado activado por errores de autenticacion consecutivos.")
                    if auth_recovery_streak <= max(1, AUTH_RECOVERY_MAX_ATTEMPTS):
                        recovery_msg = (
                            "Sesion invalida detectada (HTTP 401). "
                            f"Reautenticacion {auth_recovery_streak}/{AUTH_RECOVERY_MAX_ATTEMPTS}."
                        )
                        log_warning(recovery_msg)
                        info_logger(LOG_FILE_NAME, recovery_msg)
                        try:
                            navigate_ais_page(SIGN_OUT_LINK, attempts=2)
                        except Exception:
                            pass
                        if should_force_full_browser_restart(
                                consecutive_exception_streak,
                                "ciclo - AuthSessionError",
                                restart_events=full_restart_events,
                        ):
                            consecutive_exception_streak = 0
                            if maybe_apply_long_cooldown(
                                    reason="ciclo - reinicios duros consecutivos",
                                    restart_events=full_restart_events,
                            ):
                                reset_failure_streaks()
                                first_loop = True
                                continue
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        auth_wait_seconds = compute_adaptive_wait_seconds(
                            max(1.0, AUTH_RECOVERY_WAIT_SECONDS),
                            degraded_mode=degraded_mode,
                            failure_streak=degraded_failure_streak,
                        )
                        time.sleep(auth_wait_seconds)
                        first_loop = True
                        continue
                    cooldown_hours = max(BLOCK_COOLDOWN_TIME, 0.5)
                    lock_msg = (
                        f"Se excedio el limite de reautenticacion ({AUTH_RECOVERY_MAX_ATTEMPTS}) por HTTP 401. "
                        f"Pausa de {cooldown_hours:.2f} horas y reinicio de sesion."
                    )
                    log_warning(lock_msg)
                    info_logger(LOG_FILE_NAME, lock_msg)
                    try:
                        navigate_ais_page(SIGN_OUT_LINK, attempts=2)
                    except Exception:
                        pass
                    if should_force_full_browser_restart(
                            consecutive_exception_streak,
                            "ciclo - AuthSessionError en limite",
                            restart_events=full_restart_events,
                    ):
                        consecutive_exception_streak = 0
                        if maybe_apply_long_cooldown(
                                reason="ciclo - reinicios duros consecutivos",
                                restart_events=full_restart_events,
                        ):
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    cooldown_wait_seconds = compute_adaptive_wait_seconds(
                        cooldown_hours * hour,
                        degraded_mode=degraded_mode,
                        failure_streak=degraded_failure_streak,
                    )
                    time.sleep(cooldown_wait_seconds)
                    auth_recovery_streak = 0
                    first_loop = True
                    continue
                if is_page_unresponsive_failure(msg):
                    transient_network_streak += 1
                    consecutive_exception_streak += 1
                    degraded_failure_streak += 1
                    degraded_success_streak = 0
                    if degraded_failure_streak >= DEGRADED_MODE_FAILURE_STREAK and not degraded_mode:
                        degraded_mode = True
                        log_warning("Modo degradado activado por fallos de red consecutivos.")
                    base_wait_seconds = min(
                        300.0,
                        max(5.0, compute_retry_wait_seconds()) * (2 ** (transient_network_streak - 1)),
                    )
                    wait_seconds = compute_adaptive_wait_seconds(
                        base_wait_seconds,
                        degraded_mode=degraded_mode,
                        failure_streak=degraded_failure_streak,
                    )
                    network_msg = (
                        f"Pagina no responde o fallo de red detectado en ciclo (intento {transient_network_streak}). "
                        f"Reintento en {wait_seconds:.1f} segundos."
                    )
                    log_warning(network_msg)
                    info_logger(LOG_FILE_NAME, network_msg)
                    ip_rotation_attempts = maybe_rotate_ip_on_network_failure(
                        network_streak=max(transient_network_streak, IP_ROTATION_TRIGGER_STREAK),
                        rotation_attempts=ip_rotation_attempts,
                        context_label="ciclo - pagina no responde",
                    )
                    preserve_current_session = is_webdriver_session_alive()
                    if not preserve_current_session:
                        try:
                            reset_webdriver_session(reason="ciclo - pagina no responde")
                        except Exception:
                            pass
                    if transient_network_streak >= LONG_COOLDOWN_NETWORK_STREAK:
                        maybe_apply_long_cooldown(
                            reason="ciclo - racha de red",
                            restart_events=full_restart_events,
                            force=True,
                            cooldown_seconds=LONG_COOLDOWN_SECONDS,
                        )
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    if should_force_full_browser_restart(
                            consecutive_exception_streak,
                            "ciclo - pagina no responde",
                            restart_events=full_restart_events,
                    ):
                        consecutive_exception_streak = 0
                        if maybe_apply_long_cooldown(
                                reason="ciclo - reinicios duros consecutivos",
                                restart_events=full_restart_events,
                        ):
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    first_loop = not preserve_current_session
                    time.sleep(wait_seconds)
                    continue
                if is_transient_block_failure(msg):
                    transient_block_streak += 1
                    consecutive_exception_streak += 1
                    degraded_failure_streak += 1
                    degraded_success_streak = 0
                    if degraded_failure_streak >= DEGRADED_MODE_FAILURE_STREAK and not degraded_mode:
                        degraded_mode = True
                        log_warning("Modo degradado activado por bloqueos consecutivos.")
                    cooldown_hours = BLOCK_COOLDOWN_TIME * min(8, (2 ** (transient_block_streak - 1)))
                    block_msg = (
                        f"Bloqueo temporal detectado en ciclo (intento {transient_block_streak}). "
                        f"Se aplicara pausa de {cooldown_hours:.2f} horas y reinicio de sesion."
                    )
                    log_warning(block_msg)
                    info_logger(LOG_FILE_NAME, block_msg)
                    send_notification("BAN", block_msg)
                    try:
                        navigate_ais_page(SIGN_OUT_LINK, attempts=2)
                    except Exception:
                        pass
                    if transient_block_streak >= LONG_COOLDOWN_BLOCK_STREAK:
                        maybe_apply_long_cooldown(
                            reason="ciclo - bloqueos consecutivos",
                            restart_events=full_restart_events,
                            force=True,
                            cooldown_seconds=LONG_COOLDOWN_SECONDS,
                        )
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    if should_force_full_browser_restart(
                            consecutive_exception_streak,
                            "ciclo - bloqueo temporal",
                            restart_events=full_restart_events,
                    ):
                        consecutive_exception_streak = 0
                        if maybe_apply_long_cooldown(
                                reason="ciclo - reinicios duros consecutivos",
                                restart_events=full_restart_events,
                        ):
                            reset_failure_streaks()
                            first_loop = True
                            continue
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    cooldown_wait_seconds = compute_adaptive_wait_seconds(
                        cooldown_hours * hour,
                        degraded_mode=degraded_mode,
                        failure_streak=degraded_failure_streak,
                    )
                    time.sleep(cooldown_wait_seconds)
                    first_loop = True
                    continue
                unexpected_error_streak += 1
                consecutive_exception_streak += 1
                degraded_failure_streak += 1
                degraded_success_streak = 0
                if degraded_failure_streak >= DEGRADED_MODE_FAILURE_STREAK and not degraded_mode:
                    degraded_mode = True
                    log_warning("Modo degradado activado por excepciones inesperadas.")
                base_wait_seconds = min(
                    300.0,
                    max(5.0, compute_retry_wait_seconds()) * (2 ** min(6, unexpected_error_streak - 1)),
                )
                wait_seconds = compute_adaptive_wait_seconds(
                    base_wait_seconds,
                    degraded_mode=degraded_mode,
                    failure_streak=degraded_failure_streak,
                )
                recover_msg = (
                    f"Error inesperado en ciclo (intento {unexpected_error_streak}). "
                    f"Se reinicia sesion y se reintenta en {wait_seconds:.1f} segundos."
                )
                log_warning(recover_msg)
                info_logger(LOG_FILE_NAME, msg)
                info_logger(LOG_FILE_NAME, recover_msg)
                try:
                    reset_webdriver_session(reason="error inesperado en ciclo")
                except Exception:
                    pass
                if should_force_full_browser_restart(
                        consecutive_exception_streak,
                        "ciclo - excepcion inesperada",
                        restart_events=full_restart_events,
                ):
                    consecutive_exception_streak = 0
                    if maybe_apply_long_cooldown(
                            reason="ciclo - reinicios duros consecutivos",
                            restart_events=full_restart_events,
                    ):
                        reset_failure_streaks()
                        first_loop = True
                        continue
                    reset_failure_streaks()
                    first_loop = True
                    continue
                first_loop = True
                time.sleep(wait_seconds)
                continue
    except KeyboardInterrupt:
        msg = "Ejecucion detenida manualmente por usuario."
        log_info(msg)
    finally:
        final_summary = msg
        if END_MSG_TITLE == "EXCEPTION":
            final_summary = " | ".join(build_compact_exception_details(msg))
        log_info(final_summary)
        try:
            info_logger(LOG_FILE_NAME, final_summary)
        except Exception:
            pass
        send_notification(END_MSG_TITLE, msg)
        cleanup_driver()
        release_run_lock()


if __name__ == "__main__":
    run_scheduler()

