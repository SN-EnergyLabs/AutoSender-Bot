# 📱 SN Energy Labs — WhatsApp Auto Sender Bot

A high-performance, standalone automation dashboard and visible Chrome bot for sending personalized WhatsApp messages to leads.

Designed specifically to:
1. **Control Visible Google Chrome on your screen**: Clicks the **New Chat** button, searches unsaved phone numbers, opens the conversation, types pre-entered personalized messages, and sends them one by one.
2. **Persistent WhatsApp Login Session**: Uses a dedicated Chrome profile directory (`chrome_profile`). You only scan the QR code once on first launch; all future runs stay logged in automatically.
3. **1-Click Import from Lead Files**: Scans and loads CSV, Excel, or JSON files.
4. **Smart Anti-Spam Human Pacing**: Randomized delay intervals (e.g. 7–14 seconds) to prevent WhatsApp anti-spam flags.
5. **Real-time Live Dashboard**: Monitor live progress, pause/resume, skip numbers, and export execution reports.

---

## 🚀 Quick Start

1. Install requirements:
```bash
pip install -r requirements.txt
```

2. Run the dashboard:
```bash
python3 run.py
```

The bot will automatically:
1. Find an available local port (e.g. `http://localhost:8085`).
2. Open the dashboard in your default web browser.

---

## 🛠️ Step-by-Step Workflow

### Step 1: Connect Chrome & WhatsApp Web
1. On the top right of the dashboard, click **"Launch Visible Chrome"**.
2. A visible Google Chrome window will appear on your screen and navigate to `https://web.whatsapp.com`.
3. If this is your first time, point your phone at the screen to scan the QR code.
4. Once scanned, the dashboard status pill turns green: **"WhatsApp Ready"**.

### Step 2: Choose Your Numbers
Select one of 3 input options:
- **From Scraped Leads**: Pick any `.csv` file from the dropdown (e.g., `leads_dentist_delhi_latest.csv` or from `business_scraper/exports/`) and click **"Import Into Queue"**.
- **Paste Numbers**: Type or paste numbers line-by-line (e.g. `+91 98765 43210` or `Dr. Sharma, 098917 72753`).
- **Upload CSV**: Drag and drop any custom CSV file.

### Step 3: Craft Your Personalized Message
Compose your message in the template box. You can click any of the variable pills to insert dynamic personalization:
- `{name}` — Business / Customer name (e.g. "Smile Arts Dental Clinic")
- `{category}` — Business category (e.g. "Dental clinic")
- `{address}` — Clinic address / locality
- `{rating}` — Google rating (e.g. "4.9")
- `{phone}` — Formatted phone number

Watch the **Live WhatsApp Message Preview** bubble update instantly as you type.

### Step 4: Configure Pacing & Screen Mode
- **Screen Control Flow**:
  - `🖱️ Click "New Chat" & Search (On-Screen)` [Default]: Interactively clicks the New Chat icon, types the phone number into the search box, selects the contact, and sends the message.
  - `🔗 Direct Chat URL`: Directly opens `web.whatsapp.com/send?phone=...`.
  - `⚡ Smart Hybrid`: Tries the New Chat interaction and automatically falls back to Direct URL if WhatsApp's layout changes.
- **Human Pacing Delay**: Min and Max seconds (e.g. 7 to 14s) to ensure safe, natural spacing between messages.

### Step 5: Start Sending
- Click the big green **"Start Sending Campaign"** button.
- Watch Chrome on your screen as the bot automatically:
  1. Opens New Chat.
  2. Enters the phone number.
  3. Verifies if the contact exists on WhatsApp.
  4. Types the personalized message with natural human keystrokes.
  5. Hits Send.
  6. Updates the table with green checkmarks and cools down before the next number.

---

## ⏸️ Campaign Controls
- **Pause / Resume**: Pause sending at any moment if you need to take over Chrome.
- **Skip**: Skip the current contact if needed.
- **Stop**: Stop the campaign immediately.
- **Export CSV**: Download a complete delivery report (`whatsapp_campaign_results_*.csv`) with sent timestamps and any failed/invalid numbers.

---

## 📁 File Structure
```
whatsapp_bot/
├── run.py                 # One-click launcher script
├── server.py              # FastAPI backend with Server-Sent Events (SSE)
├── engine.py              # Selenium Chrome automation engine
├── chrome_profile/        # Persistent Chrome profile (stores WhatsApp Web login)
├── static/
│   ├── index.html         # Modern light dashboard UI
│   ├── style.css          # Design system & light aesthetic
│   └── app.js             # Live client controller & SSE stream handler
└── README.md              # Documentation
```
