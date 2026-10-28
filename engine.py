"""
WhatsApp Automation Engine using Selenium WebDriver.
Controls visible Google Chrome window on macOS, handles session persistence,
clicks 'New Chat', searches phone numbers, pastes personalized messages,
and handles human-like delays to prevent WhatsApp anti-spam flags.
"""

import os
import sys
import time
import re
import random
import urllib.parse
import threading
import subprocess
import platform
from typing import List, Dict, Any, Optional, Callable

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


def normalize_message_text(text: str) -> str:
    """
    Normalizes all newline and line break variations (Unicode U+2028/U+2029, \r\n, \r)
    to standard \n so preview and sent messages are 100% identical.
    """
    if not text:
        return ""
    return str(text).replace("\u2028", "\n").replace("\u2029", "\n").replace("\r\n", "\n").replace("\r", "\n").strip()


def clean_phone_number(raw_phone: str, default_country_code: str = "91") -> Optional[str]:
    """
    Cleans raw phone numbers to international WhatsApp format (digits only, no +).
    Handles:
      - '092103 70993' -> '919210370993'
      - '+91 098917 26354' -> '919891726354' (strips trunk 0)
      - '0091 98765 43210' -> '919876543210' (strips international 00)
      - '+91 98765-43210' -> '919876543210'
      - '9876543210' -> '919876543210'
      - '+1 (555) 123-4567' -> '15551234567'
    """
    if not raw_phone:
        return None

    # Strip everything except digits
    digits = re.sub(r"\D", "", str(raw_phone))
    if not digits:
        return None

    cc = re.sub(r"\D", "", str(default_country_code)) or "91"

    # Strip international 00 prefix (e.g. 0091... -> 91...)
    if digits.startswith("00") and len(digits) > 10:
        digits = digits[2:]

    # If starts with '0' followed by country code + 10 digits (e.g. 0919876543210 -> 919876543210)
    if digits.startswith("0" + cc) and len(digits) == len(cc) + 11:
        digits = digits[1:]

    # If starts with country code followed by domestic trunk 0 (e.g. 9109876543210 -> 919876543210)
    if digits.startswith(cc + "0") and len(digits) == len(cc) + 11:
        digits = cc + digits[len(cc) + 1:]

    # If starts with leading 0 (common domestic trunk like 098917...), strip it if rest is 10 digits
    if digits.startswith("0") and len(digits) == 11:
        digits = digits[1:]

    # If it's a 10 digit number (standard mobile number), prepend default country code
    if len(digits) == 10:
        digits = cc + digits

    # Basic length check for valid E.164 phone digits (typically 10 to 15 digits)
    if 10 <= len(digits) <= 15:
        return digits

    return digits if len(digits) >= 8 else None


class WhatsAppEngine:
    def __init__(self, profile_dir: Optional[str] = None):
        self.driver: Optional[webdriver.Chrome] = None
        self.profile_dir = profile_dir or os.path.join(os.path.dirname(os.path.abspath(__file__)), "chrome_profile")
        os.makedirs(self.profile_dir, exist_ok=True)

        # State management
        self.state: str = "IDLE"  # IDLE, LAUNCHING, WAITING_QR, READY, RUNNING, PAUSED, STOPPED
        self.queue: List[Dict[str, Any]] = []
        self.current_index: int = -1
        self.current_contact: Optional[Dict[str, Any]] = None
        self.stats = {
            "total": 0,
            "sent": 0,
            "failed": 0,
            "not_on_whatsapp": 0,
            "pending": 0,
            "start_time": None,
            "elapsed_seconds": 0
        }

        # Configuration
        self.message_template: str = ""
        self.delay_min: int = 7
        self.delay_max: int = 14
        self.dispatch_mode: str = "new_chat"  # 'new_chat', 'direct_url', 'hybrid'
        self.default_country_code: str = "91"

        # Threading & Control (RLock prevents deadlocks across recursive/internal calls)
        self._worker_thread: Optional[threading.Thread] = None
        self._pause_event = threading.Event()
        self._pause_event.set()  # Not paused by default
        self._stop_event = threading.Event()
        self._skip_event = threading.Event()
        self._lock = threading.RLock()

        # Callbacks
        self.event_subscribers: List[Callable[[str, Dict[str, Any]], None]] = []

    def subscribe(self, callback: Callable[[str, Dict[str, Any]], None]):
        """Subscribe to engine events (for SSE / WebSockets)."""
        if callback not in self.event_subscribers:
            self.event_subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[str, Dict[str, Any]], None]):
        if callback in self.event_subscribers:
            self.event_subscribers.remove(callback)

    def emit(self, event_type: str, data: Dict[str, Any]):
        """Notify all subscribers with event data."""
        for cb in list(self.event_subscribers):
            try:
                cb(event_type, data)
            except Exception:
                pass

    def log(self, message: str, level: str = "info"):
        """Emit a log message with timestamp."""
        timestamp = time.strftime("%H:%M:%S")
        payload = {
            "timestamp": timestamp,
            "message": message,
            "level": level
        }
        self.emit("log", payload)
        print(f"[{timestamp}] [{level.upper()}] {message}")

    # ==========================================
    # Browser Management
    # ==========================================
    def launch_browser(self) -> bool:
        """Launches visible Google Chrome on macOS with persistent user profile or attaches to running instance."""
        with self._lock:
            if self.driver:
                try:
                    _ = self.driver.current_url
                    self.log("Chrome is already open and running.", "info")
                    return True
                except Exception:
                    self.driver = None

            # Check if Chrome is already running with an active DevTools port from our profile
            port_file = os.path.join(self.profile_dir, "DevToolsActivePort")
            if os.path.isfile(port_file):
                try:
                    with open(port_file, "r") as pf:
                        active_port = pf.readline().strip()
                    if active_port.isdigit():
                        import urllib.request
                        with urllib.request.urlopen(f"http://localhost:{active_port}/json/version", timeout=1.5) as r:
                            opts = Options()
                            opts.add_experimental_option("debuggerAddress", f"localhost:{active_port}")
                            self.driver = webdriver.Chrome(options=opts)
                            self.state = "READY" if self.is_browser_ready() else "WAITING_QR"
                            self.emit("state_change", {"state": self.state})
                            self.log(f"Connected directly to existing open WhatsApp Chrome session (port {active_port})!", "success")
                            self._check_login_async()
                            return True
                except Exception as attach_err:
                    self.log(f"Could not re-attach to existing instance ({attach_err}), launching fresh...", "debug")

            self.state = "LAUNCHING"
            self.emit("state_change", {"state": self.state})
            self.log(f"Launching visible Google Chrome (Profile: {self.profile_dir})...", "info")

            options = Options()
            options.add_argument(f"--user-data-dir={self.profile_dir}")
            options.add_argument("--remote-debugging-port=0")
            options.add_argument("--no-first-run")
            options.add_argument("--no-default-browser-check")
            options.add_argument("--start-maximized")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
            
            # Anti-detection flags
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)

            try:
                self.driver = webdriver.Chrome(options=options)
                # CDP override for navigator.webdriver
                try:
                    self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                    })
                except Exception:
                    pass

                self.log("Navigating to WhatsApp Web (https://web.whatsapp.com)...", "info")
                self.driver.get("https://web.whatsapp.com")

                # Monitor login state asynchronously
                self._check_login_async()
                return True
            except Exception as e:
                self.state = "STOPPED"
                self.emit("state_change", {"state": self.state})
                self.log(f"Failed to launch Chrome: {str(e)}", "error")
                return False

    def _check_login_async(self):
        """Monitors login state until QR is scanned or chat list is visible."""
        def monitor():
            self.log("Monitoring WhatsApp Web login state...", "info")
            start = time.time()
            notified_qr = False

            while self.driver and time.time() - start < 300:
                try:
                    if not self.driver:
                        break

                    # Check if logged in
                    if self.is_browser_ready():
                        self.state = "READY"
                        self.emit("state_change", {"state": self.state})
                        self.log("WhatsApp Web is LOGGED IN and READY!", "success")
                        return

                    # Check for QR code
                    qr_elements = self.driver.find_elements(By.CSS_SELECTOR, "canvas, div[data-testid='qrcode'], div[data-ref]")
                    if qr_elements and not notified_qr:
                        self.state = "WAITING_QR"
                        self.emit("state_change", {"state": self.state})
                        self.log("WhatsApp Web QR code detected in Chrome. Please scan it with your phone WhatsApp app!", "warning")
                        notified_qr = True

                    time.sleep(1.5)
                except WebDriverException:
                    break
                except Exception:
                    time.sleep(2)

        t = threading.Thread(target=monitor, daemon=True)
        t.start()

    def close_browser(self):
        """Closes the Selenium browser cleanly."""
        with self._lock:
            if self.state in ["RUNNING", "PAUSED"]:
                self.stop()
            if self.driver:
                self.log("Closing Chrome browser...", "info")
                try:
                    self.driver.quit()
                except Exception:
                    pass
                self.driver = None
            self.state = "IDLE"
            self.emit("state_change", {"state": self.state})
            self.log("Browser closed.", "info")

    def is_browser_ready(self) -> bool:
        """Returns True if WhatsApp Web is open and fully logged in."""
        if not self.driver:
            return False
        try:
            selectors = [
                "button[aria-label='New chat']",
                "#pane-side",
                "div[id='side']",
                "div[data-testid='chat-list']",
                "div[aria-label='Chat list']",
                "input[placeholder*='Search' i]"
            ]
            for sel in selectors:
                elems = self.driver.find_elements(By.CSS_SELECTOR, sel)
                if elems and elems[0].is_displayed():
                    return True
            return False
        except Exception:
            return False

    # ==========================================
    # Queue & Setup
    # ==========================================
    def set_queue(self, items: List[Dict[str, Any]], template: str, delay_min: int = 7, delay_max: int = 14, mode: str = "new_chat", default_cc: str = "91"):
        """Populates the sending queue with contacts and message template."""
        with self._lock:
            self.message_template = normalize_message_text(template)
            self.delay_min = max(3, int(delay_min))
            self.delay_max = max(self.delay_min, int(delay_max))
            self.dispatch_mode = mode or "new_chat"
            self.default_country_code = default_cc or "91"

            self.queue = []
            for idx, item in enumerate(items):
                raw_phone = str(item.get("phone", "")).strip()
                clean_phone = clean_phone_number(raw_phone, default_country_code=self.default_country_code)

                self.queue.append({
                    "id": idx + 1,
                    "name": item.get("name") or "Customer",
                    "raw_phone": raw_phone,
                    "clean_phone": clean_phone,
                    "address": item.get("address") or "",
                    "category": item.get("category") or "",
                    "rating": item.get("rating") or "",
                    "reviews": item.get("reviews") or "",
                    "website": item.get("website") or "",
                    "status": "pending",
                    "message": "",
                    "sent_at": None,
                    "error": None
                })

            self.stats["total"] = len(self.queue)
            self.stats["sent"] = 0
            self.stats["failed"] = 0
            self.stats["not_on_whatsapp"] = 0
            self.stats["pending"] = len(self.queue)
            self.current_index = -1
            self.current_contact = None

            self.emit("queue_updated", {"queue": self.queue, "stats": self.stats})
            self.log(f"Queue configured with {len(self.queue)} contacts. Dispatch mode: {self.dispatch_mode.upper()}", "info")

    def retry_failed(self) -> int:
        """Resets all 'failed' contacts back to 'pending' so they can be processed cleanly."""
        with self._lock:
            reset_count = 0
            for c in self.queue:
                if c.get("status") == "failed":
                    c["status"] = "pending"
                    c["error"] = None
                    reset_count += 1
            self.stats["failed"] = 0
            self.stats["pending"] = sum(1 for c in self.queue if c["status"] == "pending")
            self.emit("queue_updated", {"queue": self.queue, "stats": self.stats})
            self.log(f"Reset {reset_count} failed contacts back to pending for retry.", "info")
            return reset_count

    def build_message(self, contact: Dict[str, Any]) -> str:
        """Renders personalized message by replacing tags with exact formatting."""
        raw_template = self.message_template or "Hello {name}!"
        msg = normalize_message_text(raw_template)

        name = contact.get("name") or "Customer"
        phone = contact.get("raw_phone") or contact.get("phone") or contact.get("clean_phone") or ""
        address = contact.get("address") or ""
        category = contact.get("category") or "business"
        rating = str(contact.get("rating") or "")
        reviews = str(contact.get("reviews") or "")
        website = str(contact.get("website") or "")

        replacements = {
            "{name}": name,
            "{business_name}": name,
            "{clinic}": name,
            "{phone}": phone,
            "{address}": address,
            "{city}": address,
            "{category}": category,
            "{rating}": rating,
            "{reviews}": reviews,
            "{website}": website,
        }
        for placeholder, val in replacements.items():
            msg = msg.replace(placeholder, str(val))
        return normalize_message_text(msg)

    # ==========================================
    # WhatsApp Selectors & DOM Interaction
    # ==========================================
    def _find_new_chat_button(self):
        """Finds the 'New chat' button using accurate modern selectors."""
        selectors = [
            (By.CSS_SELECTOR, "button[aria-label='New chat']"),
            (By.CSS_SELECTOR, "button[data-tab='2'][aria-label='New chat']"),
            (By.XPATH, "//button[@aria-label='New chat' or @title='New chat']"),
            (By.XPATH, "//button[contains(@class, 'html-button') and @aria-label='New chat']"),
            (By.CSS_SELECTOR, "span[data-icon='new-chat-outline']"),
            (By.CSS_SELECTOR, "span[data-icon='chat']"),
            (By.XPATH, "//*[@title='New chat' or @aria-label='New chat']")
        ]

        for by, sel in selectors:
            try:
                elems = self.driver.find_elements(by, sel)
                for e in elems:
                    if e.is_displayed():
                        return e
            except Exception:
                continue
        return None

    def _find_search_input_in_new_chat(self):
        """
        Locates the search input inside the 'New chat' drawer.
        MUST BE an <input> element with placeholder containing 'Search',
        and NEVER a contenteditable div (which matches the open chat's message box!).
        """
        selectors = [
            (By.CSS_SELECTOR, "input[placeholder*='Search name, number' i]"),
            (By.CSS_SELECTOR, "input[placeholder*='Search' i]"),
            (By.CSS_SELECTOR, "#side input[data-tab='3']"),
            (By.CSS_SELECTOR, "input[data-tab='3']"),
            (By.XPATH, "//input[contains(@placeholder, 'Search') or contains(@placeholder, 'search')]"),
            (By.CSS_SELECTOR, "div[data-testid='chat-list-search'] input")
        ]

        for by, sel in selectors:
            try:
                elems = self.driver.find_elements(by, sel)
                for e in elems:
                    # Ensure it is displayed and NOT inside #main or footer
                    if e.is_displayed():
                        return e
            except Exception:
                continue
        return None

    def _type_message_into_composer(self, composer, message: str):
        """Types or pastes message into WhatsApp compose box, preserving all newlines, paragraphs, and emojis."""
        message = normalize_message_text(message)

        # On macOS, native clipboard paste (Cmd+V) is 100% faithful to preview:
        # It natively preserves paragraphs, emojis (✅, 🚀), and currency symbols (₹) in WhatsApp's Lexical editor
        pasted = False
        if platform.system() == "Darwin":
            try:
                p = subprocess.Popen(["pbcopy"], stdin=subprocess.PIPE, close_fds=True)
                p.communicate(message.encode("utf-8"))
                time.sleep(0.05)
                composer.send_keys(Keys.COMMAND, "v")
                time.sleep(0.35)
                # Verify composer has content
                if composer.text.strip():
                    pasted = True
            except Exception as e:
                self.log(f"Clipboard paste fallback: {e}", "debug")

        # Fallback to key-by-key typing if clipboard paste did not populate composer
        if not pasted:
            lines = message.split("\n")
            for i, line in enumerate(lines):
                if line:
                    composer.send_keys(line)
                    time.sleep(random.uniform(0.015, 0.03))
                if i < len(lines) - 1:
                    composer.send_keys(Keys.SHIFT + Keys.ENTER)
                    time.sleep(0.04)

    def _dismiss_drawer_or_dialog(self):
        """Safely closes any open drawer, modal, or search box."""
        if not self.driver:
            return
        try:
            # 1. Look for back or close buttons
            close_btns = self.driver.find_elements(
                By.CSS_SELECTOR,
                "button[aria-label='Back'], span[data-icon='back'], button[aria-label='Close'], span[data-icon='x']"
            )
            for b in close_btns:
                try:
                    if b.is_displayed():
                        self.driver.execute_script("arguments[0].click();", b)
                        time.sleep(0.4)
                        return
                except Exception:
                    pass
            # 2. Press Escape key
            try:
                from selenium.webdriver.common.action_chains import ActionChains
                ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                time.sleep(0.3)
            except Exception:
                pass
        except Exception:
            pass

    def _find_message_composer(self):
        """Finds the main message compose input box in the active chat conversation."""
        selectors = [
            (By.CSS_SELECTOR, "#main footer div[contenteditable='true']"),
            (By.CSS_SELECTOR, "footer div[contenteditable='true'][data-tab='10']"),
            (By.CSS_SELECTOR, "footer div[contenteditable='true']"),
            (By.CSS_SELECTOR, "#main div[data-testid='conversation-compose-box-input']"),
            (By.CSS_SELECTOR, "div[data-testid='conversation-compose-box-input']"),
            (By.CSS_SELECTOR, "div[aria-placeholder*='Type a message' i]"),
            (By.XPATH, "//div[@id='main']//footer//div[@contenteditable='true']"),
            (By.XPATH, "//footer//div[@contenteditable='true' and @data-tab='10']")
        ]
        for by, sel in selectors:
            try:
                elems = self.driver.find_elements(by, sel)
                if elems and elems[0].is_displayed():
                    return elems[0]
            except Exception:
                continue
        return None

    def _find_send_button(self):
        """Finds the send button in the footer."""
        selectors = [
            (By.CSS_SELECTOR, "#main footer button[aria-label='Send']"),
            (By.CSS_SELECTOR, "button[aria-label='Send']"),
            (By.CSS_SELECTOR, "span[data-icon='send']"),
            (By.CSS_SELECTOR, "button[data-testid='compose-btn-send']"),
            (By.CSS_SELECTOR, "span[data-icon='send-refreshed']"),
            (By.XPATH, "//footer//button[contains(@aria-label, 'Send')]"),
            (By.XPATH, "//span[@data-icon='send' or @data-icon='send-refreshed']/ancestor::button")
        ]
        for by, sel in selectors:
            try:
                elems = self.driver.find_elements(by, sel)
                if elems and elems[0].is_displayed():
                    return elems[0]
            except Exception:
                continue
        return None

    # ==========================================
    # Send Flow 1: Interactive 'New Chat' on Screen
    # ==========================================
    def _send_contact_via_new_chat(self, contact: Dict[str, Any], message: str) -> bool:
        """
        Interactively clicks 'New chat' button on screen, searches phone number in the
        dedicated drawer input, selects contact, types message and clicks Send.
        """
        driver = self.driver
        phone = contact["clean_phone"]
        name = contact["name"]

        self.log(f"[{contact['id']}/{self.stats['total']}] Locating 'New chat' button on screen for {name} (+{phone})...", "info")

        # 0. Ensure no prior drawer or popup is left open and URL is at main interface
        try:
            curr_url = driver.current_url or ""
            if "/send?" in curr_url:
                chats_btn = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Chats'], span[data-icon='chats']")
                if chats_btn and chats_btn[0].is_displayed():
                    driver.execute_script("arguments[0].click();", chats_btn[0])
                    time.sleep(1.0)
                else:
                    driver.get("https://web.whatsapp.com/")
                    time.sleep(2.0)
        except Exception:
            pass

        self._dismiss_drawer_or_dialog()

        # 1. Click New Chat button
        new_chat_btn = self._find_new_chat_button()
        if not new_chat_btn:
            # Try dismissing any lingering overlay and retry
            self._dismiss_drawer_or_dialog()
            time.sleep(0.5)
            new_chat_btn = self._find_new_chat_button()

        if not new_chat_btn:
            raise Exception("New chat button could not be located in navigation bar.")

        # JavaScript click directly opens the New Chat drawer reliably
        driver.execute_script("arguments[0].click();", new_chat_btn)
        time.sleep(0.8)

        # 2. Find search input strictly in New Chat drawer
        search_input = None
        for _ in range(8):
            search_input = self._find_search_input_in_new_chat()
            if search_input:
                break
            time.sleep(0.4)

        if not search_input:
            raise Exception("Search input inside 'New chat' drawer not found.")

        # Clear search input thoroughly
        search_input.click()
        time.sleep(0.2)
        try:
            search_input.clear()
        except Exception:
            pass
        search_input.send_keys(Keys.COMMAND + "a" if sys.platform == "darwin" else Keys.CONTROL + "a")
        search_input.send_keys(Keys.BACKSPACE)
        time.sleep(0.15)

        # Format number with '+'
        search_query = f"+{phone}" if not phone.startswith("+") else phone
        self.log(f"Entering '{search_query}' in New Chat search drawer...", "info")

        # Paste full number at once to prevent React input from dropping digits mid-typing
        pasted = False
        if platform.system() == "Darwin":
            try:
                p = subprocess.Popen(["pbcopy"], stdin=subprocess.PIPE, close_fds=True)
                p.communicate(search_query.encode("utf-8"))
                time.sleep(0.05)
                search_input.send_keys(Keys.COMMAND, "v")
                time.sleep(0.25)
                val_clean = (search_input.get_attribute("value") or "").replace(" ", "")
                if val_clean == search_query.replace(" ", ""):
                    pasted = True
            except Exception:
                pass

        if not pasted:
            try:
                driver.execute_script("""
                    const input = arguments[0];
                    const val = arguments[1];
                    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                    setter.call(input, val);
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                """, search_input, search_query)
                time.sleep(0.25)
            except Exception:
                search_input.send_keys(search_query)
                time.sleep(0.25)

        # Verify input has complete phone query; if not, re-enter
        curr_val = (search_input.get_attribute("value") or "").replace(" ", "")
        if curr_val != search_query.replace(" ", ""):
            search_input.send_keys(Keys.COMMAND + "a" if sys.platform == "darwin" else Keys.CONTROL + "a")
            search_input.send_keys(search_query)
            time.sleep(0.25)

        # 3. Wait/poll up to 9.5 seconds for search results (handling "Looking outside your contacts...")
        result_cell = None
        phone_core = phone[-7:]
        ignore_keywords = ["new group", "new community", "new channel", "nouveau groupe", "nouvelle communauté"]

        start_wait = time.time()
        while time.time() - start_wait < 9.5:
            if self._stop_event.is_set() or self._skip_event.is_set():
                break

            # Fast JavaScript search for cell, no_results, or still searching
            search_check = driver.execute_script("""
                const phoneCore = arguments[0];
                const ignoreKeywords = arguments[1];
                const bodyText = (document.body.innerText || '').toLowerCase();

                // 1. Check for definitive 'no results' in drawer
                if (bodyText.includes('no results found') || bodyText.includes('no contacts found') || 
                    bodyText.includes('no chats, contacts or messages found')) {
                    return { status: 'no_results' };
                }

                // 2. Check for contact entry under 'Not in your contacts' or matching core phone digits
                const items = document.querySelectorAll(
                    "#side div[role='button'], div[data-testid='cell-frame-container'], div[role='gridcell'], div[role='listitem']"
                );

                for (const item of items) {
                    if (item.offsetParent === null && item.getBoundingClientRect().height === 0) continue;
                    const text = (item.innerText || '').trim();
                    const textLower = text.toLowerCase();

                    if (ignoreKeywords.some(kw => textLower.includes(kw))) continue;

                    const digits = text.replace(/\\D/g, '');
                    if (digits.includes(phoneCore) || digits.endsWith(phoneCore) || text.includes(phoneCore) ||
                        textLower.includes('not in your contacts') || textLower.includes('chat with')) {
                        return { status: 'found' };
                    }
                }

                // 3. Still loading outside contacts
                if (bodyText.includes('looking outside your contacts') || bodyText.includes('searching')) {
                    return { status: 'searching' };
                }

                return { status: 'pending' };
            """, phone_core, ignore_keywords)

            status = search_check.get("status") if search_check else "pending"

            if status == "no_results":
                self.log(f"WhatsApp confirms: '+{phone}' is NOT registered on WhatsApp.", "warning")
                self._dismiss_drawer_or_dialog()
                contact["status"] = "not_on_whatsapp"
                contact["error"] = "Not registered on WhatsApp"
                self.stats["not_on_whatsapp"] += 1
                return False

            if status == "found":
                # Locate element in Python and click it
                list_items = driver.find_elements(
                    By.CSS_SELECTOR,
                    "#side div[role='button'], div[data-testid='cell-frame-container'], div[role='gridcell'], div[role='listitem']"
                )
                for it in list_items:
                    try:
                        t = (it.text or "").strip()
                        t_lower = t.lower()
                        if any(kw in t_lower for kw in ignore_keywords):
                            continue
                        digits = re.sub(r"\D", "", t)
                        if (phone_core in digits or digits.endswith(phone_core) or phone_core in t or
                            "not in your contacts" in t_lower or "chat with" in t_lower):
                            result_cell = it
                            break
                    except Exception:
                        continue
                if result_cell:
                    break

            time.sleep(0.5)

        if result_cell:
            self.log(f"Found contact entry for '+{phone}'. Opening chat conversation...", "info")
            try:
                driver.execute_script("arguments[0].click();", result_cell)
            except Exception:
                result_cell.click()
            time.sleep(2.0)
        else:
            self.log(f"No direct contact cell match in drawer for '+{phone}'. Falling back to direct URL...", "info")
            self._dismiss_drawer_or_dialog()
            return self._send_contact_via_direct_url(contact, message)

        # 4. Locate message composer in active chat pane
        composer = self._find_message_composer()
        if not composer:
            raise Exception("Chat conversation opened, but message composer box not found.")

        # Focus & Type Message
        self.log(f"Typing personalized message for {name}...", "info")
        composer.click()
        time.sleep(0.3)
        self._type_message_into_composer(composer, message)
        time.sleep(random.uniform(0.5, 0.8))

        # 5. Click Send Button or Press Enter
        send_btn = self._find_send_button()
        if send_btn:
            self.log("Clicking WhatsApp Send button...", "info")
            try:
                driver.execute_script("arguments[0].click();", send_btn)
            except Exception:
                send_btn.click()
        else:
            self.log("Pressing Enter to dispatch message...", "info")
            composer.send_keys(Keys.ENTER)

        time.sleep(1.8)
        self.log(f"Message SENT successfully to {name} (+{phone})!", "success")
        contact["status"] = "sent"
        contact["sent_at"] = time.strftime("%H:%M:%S")
        self.stats["sent"] += 1
        return True

    # ==========================================
    # Send Flow 2: Direct URL Fallback / Mode
    # ==========================================
    def _send_contact_via_direct_url(self, contact: Dict[str, Any], message: str) -> bool:
        """Navigates directly to web.whatsapp.com/send?phone=... as a reliable method."""
        driver = self.driver
        phone = contact["clean_phone"]
        name = contact["name"]

        message = normalize_message_text(message)
        encoded_text = urllib.parse.quote(message)
        url = f"https://web.whatsapp.com/send?phone={phone}&text={encoded_text}"
        self.log(f"Navigating to WhatsApp chat for {name} (+{phone})...", "info")
        driver.get(url)

        # Wait for chat to load or invalid number popup (crash-proof JS detection)
        start_wait = time.time()
        chat_loaded = False
        while time.time() - start_wait < 16:
            if self._stop_event.is_set() or self._skip_event.is_set():
                break

            # Fast, 100% crash-proof check for invalid / not-on-WhatsApp modal dialog
            is_invalid_dialog = driver.execute_script("""
                const phrases = [
                    'invalid', "isn't on whatsapp", 'not on whatsapp', 
                    'not registered', "couldn't find", 'url is invalid',
                    'is invalid', 'phone number shared via url'
                ];
                const dialogs = document.querySelectorAll("div[role='dialog'], div[data-animate-modal-popup='true'], [data-testid='popup-contents']");
                for (const d of dialogs) {
                    const text = (d.innerText || '').toLowerCase();
                    for (const p of phrases) {
                        if (text.includes(p)) return true;
                    }
                }
                const bodyText = (document.body.innerText || '').toLowerCase();
                for (const p of ['phone number shared via url is invalid', "phone number isn't on whatsapp", 'url is invalid']) {
                    if (bodyText.includes(p)) return true;
                }
                return false;
            """)

            if is_invalid_dialog:
                self.log(f"Phone number '+{phone}' is not registered on WhatsApp.", "warning")
                # Dismiss modal by clicking OK
                try:
                    driver.execute_script("""
                        const buttons = document.querySelectorAll("div[role='dialog'] button, div[role='dialog'] [role='button'], button");
                        for (const b of buttons) {
                            const text = (b.innerText || '').trim().toLowerCase();
                            if (text === 'ok' || text === 'cancel' || text.includes('ok') || b.getAttribute('aria-label') === 'OK') {
                                b.click();
                                return true;
                            }
                        }
                        return false;
                    """)
                except Exception:
                    pass
                self._dismiss_drawer_or_dialog()
                contact["status"] = "not_on_whatsapp"
                contact["error"] = "Not registered on WhatsApp"
                self.stats["not_on_whatsapp"] += 1
                return False

            # Check if message composer or send button is loaded
            composer = self._find_message_composer()
            send_btn = self._find_send_button()
            if composer or send_btn:
                chat_loaded = True
                break
            time.sleep(0.7)

        if not chat_loaded:
            raise Exception("Timeout loading chat conversation for phone number.")

        time.sleep(1.0)
        composer = self._find_message_composer()
        composer_text = ""
        if composer:
            try:
                composer_text = (composer.text or composer.get_attribute("innerText") or "").strip()
            except Exception:
                pass

        if not composer_text and composer:
            self.log(f"Composer text was empty on URL load; pasting message for {name}...", "info")
            composer.click()
            time.sleep(0.3)
            self._type_message_into_composer(composer, message)
            time.sleep(random.uniform(0.5, 0.8))

        send_btn = self._find_send_button()
        if send_btn:
            self.log("Clicking Send button...", "info")
            try:
                driver.execute_script("arguments[0].click();", send_btn)
            except Exception:
                send_btn.click()
        else:
            composer = self._find_message_composer()
            if composer:
                composer.send_keys(Keys.ENTER)

        time.sleep(1.8)
        self.log(f"Message SENT successfully to {name} (+{phone})!", "success")
        contact["status"] = "sent"
        contact["sent_at"] = time.strftime("%H:%M:%S")
        self.stats["sent"] += 1
        return True

    # ==========================================
    # Main Campaign Loop Worker
    # ==========================================
    def start_campaign(self):
        """Starts processing the queue in a background thread."""
        with self._lock:
            if self.state == "RUNNING":
                self.log("Campaign is already in progress.", "warning")
                return

            if self.state == "PAUSED":
                self.log("Resuming paused campaign...", "info")
                self.resume()
                return

            # Check if a previous worker thread is still finishing
            if self._worker_thread and self._worker_thread.is_alive():
                self.log("Campaign worker thread is still active. Please wait a moment.", "warning")
                return

            if not self.queue:
                self.log("No numbers in queue! Please add numbers first.", "error")
                return

            self._stop_event.clear()
            self._pause_event.set()
            self._skip_event.clear()
            self.state = "RUNNING"
            self.stats["start_time"] = time.time()
            self.emit("state_change", {"state": self.state})

            # Launch worker in background thread immediately so HTTP returns instantly
            self._worker_thread = threading.Thread(target=self._campaign_worker, daemon=True)
            self._worker_thread.start()

    def pause(self):
        """Pauses the sending loop."""
        self._pause_event.clear()
        self.state = "PAUSED"
        self.emit("state_change", {"state": self.state})
        self.log("Campaign PAUSED. Click Resume to continue.", "warning")

    def resume(self):
        """Resumes the sending loop."""
        self._pause_event.set()
        self.state = "RUNNING"
        self.emit("state_change", {"state": self.state})
        self.log("Campaign RESUMED.", "info")

    def stop(self):
        """Stops the campaign immediately."""
        self._stop_event.set()
        self._pause_event.set()
        self._skip_event.clear()
        self.state = "STOPPED"
        self.emit("state_change", {"state": self.state})
        self.log("Campaign STOPPED by user.", "warning")

    def skip_current(self):
        """Skips the contact currently being processed or waiting in cooldown."""
        self._skip_event.set()
        self.log("Skip signal received. Skipping to next number...", "warning")

    def _campaign_worker(self):
        """Iterates through the queue one by one."""
        self.log("Starting WhatsApp dispatch queue worker...", "info")

        # 1. Ensure Chrome is running
        if not self.driver:
            self.log("Launching visible Google Chrome on your screen...", "info")
            if not self.launch_browser():
                with self._lock:
                    self.state = "STOPPED"
                    self.emit("state_change", {"state": self.state})
                return

        # 2. Wait until WhatsApp Web is logged in
        wait_start = time.time()
        notified_wait = False

        while not self.is_browser_ready() and not self._stop_event.is_set():
            if not notified_wait:
                self.log("Waiting for WhatsApp Web login... If QR code appears in Chrome, please scan it with your phone.", "info")
                notified_wait = True

            if time.time() - wait_start > 180:
                self.log("WhatsApp login check timed out (3 minutes). Please ensure WhatsApp Web is logged in.", "error")
                with self._lock:
                    self.state = "STOPPED"
                    self.emit("state_change", {"state": self.state})
                return
            time.sleep(1.5)

        if self._stop_event.is_set():
            with self._lock:
                self.state = "STOPPED"
                self.emit("state_change", {"state": self.state})
            return

        self.log("WhatsApp Web is connected! Starting to send messages one by one...", "success")

        # 3. Process contacts
        for idx, contact in enumerate(self.queue):
            if self._stop_event.is_set():
                break

            # Handle pause
            while not self._pause_event.is_set():
                if self._stop_event.is_set():
                    break
                time.sleep(0.5)

            if self._stop_event.is_set():
                break

            # Skip if already sent or invalid
            if contact["status"] in ["sent", "not_on_whatsapp"]:
                continue

            # Check if skip was triggered for this contact
            if self._skip_event.is_set():
                self._skip_event.clear()
                self.log(f"Skipped contact '{contact['name']}' (+{contact.get('clean_phone') or contact.get('raw_phone')}).", "warning")
                contact["status"] = "skipped"
                contact["error"] = "Skipped by user"
                self.stats["failed"] += 1
                self.stats["pending"] = sum(1 for c in self.queue if c["status"] == "pending")
                self.emit("contact_progress", {"index": idx, "contact": contact, "stats": self.stats})
                continue

            self.current_index = idx
            self.current_contact = contact
            contact["status"] = "sending"
            self.stats["pending"] = sum(1 for c in self.queue if c["status"] == "pending")
            self.emit("contact_progress", {"index": idx, "contact": contact, "stats": self.stats})

            # Check phone validity
            if not contact.get("clean_phone"):
                self.log(f"Skipping invalid phone number for '{contact['name']}': {contact['raw_phone']}", "warning")
                contact["status"] = "failed"
                contact["error"] = "Invalid phone number format"
                self.stats["failed"] += 1
                self.emit("contact_progress", {"index": idx, "contact": contact, "stats": self.stats})
                continue

            message = self.build_message(contact)
            contact["message"] = message

            success = False
            try:
                # Mode 1: New Chat screen automation (with automatic direct URL fallback)
                if self.dispatch_mode == "new_chat":
                    try:
                        success = self._send_contact_via_new_chat(contact, message)
                    except Exception as e:
                        self.log(f"New chat interaction issue: {str(e)}. Automatically falling back to direct URL mode...", "warning")
                        success = self._send_contact_via_direct_url(contact, message)
                elif self.dispatch_mode == "direct_url":
                    success = self._send_contact_via_direct_url(contact, message)
                else:  # hybrid
                    try:
                        success = self._send_contact_via_new_chat(contact, message)
                    except Exception:
                        success = self._send_contact_via_direct_url(contact, message)

            except Exception as ex:
                self.log(f"Error sending message to {contact['name']} (+{contact['clean_phone']}): {str(ex)}", "error")
                contact["status"] = "failed"
                contact["error"] = str(ex)
                self.stats["failed"] += 1
                try:
                    self._dismiss_drawer_or_dialog()
                except Exception:
                    pass

            self.stats["pending"] = sum(1 for c in self.queue if c["status"] == "pending")
            self.emit("contact_progress", {"index": idx, "contact": contact, "stats": self.stats})

            # Delay before next contact (Anti-Spam Human Pacing)
            remaining_unprocessed = any(c["status"] == "pending" for c in self.queue[idx + 1:])
            if remaining_unprocessed and not self._stop_event.is_set():
                delay = random.uniform(self.delay_min, self.delay_max)
                self.log(f"Pacing cooldown: waiting {delay:.1f}s before next contact...", "info")
                delay_start = time.time()
                while time.time() - delay_start < delay:
                    if self._stop_event.is_set():
                        break
                    # If user clicks Skip during cooldown, advance immediately
                    if self._skip_event.is_set():
                        self.log("Cooldown skipped by user. Advancing to next contact...", "info")
                        break
                    while not self._pause_event.is_set() and not self._stop_event.is_set():
                        time.sleep(0.5)

                    time_left = max(0, int(delay - (time.time() - delay_start)))
                    self.emit("cooldown", {"remaining_seconds": time_left, "total_delay": int(delay)})
                    time.sleep(1.0)

        # Finished loop
        with self._lock:
            if self._stop_event.is_set():
                self.state = "STOPPED"
                self.current_contact = None
                self.emit("state_change", {"state": self.state})
                self.log(f"Campaign stopped by user. Progress: Sent {self.stats['sent']} | Not on WA: {self.stats['not_on_whatsapp']} | Failed: {self.stats['failed']}", "warning")
            else:
                self.state = "READY" if self.is_browser_ready() else "IDLE"
                self.current_contact = None
                self.emit("state_change", {"state": self.state})
                self.log(f"Campaign completed! Sent: {self.stats['sent']} | Not on WA: {self.stats['not_on_whatsapp']} | Failed: {self.stats['failed']}", "success")
