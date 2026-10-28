#!/usr/bin/env python3
"""
Launcher script for WhatsApp Outreach Bot Dashboard.
Starts the FastAPI server with Uvicorn and opens the dashboard in the browser.
"""

import os
import sys
import socket
import webbrowser
import threading
import time

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, CURRENT_DIR)

import uvicorn
from rich.console import Console
from rich.panel import Panel

console = Console()


def find_free_port(start_port: int = 8085) -> int:
    """Finds an open TCP port starting from start_port."""
    port = start_port
    while port < 65535:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            res = sock.connect_ex(("127.0.0.1", port))
            if res != 0:
                return port
        port += 1
    return 8085


def open_browser_delayed(url: str, delay: float = 1.2):
    def _open():
        time.sleep(delay)
        webbrowser.open(url)
    t = threading.Thread(target=_open, daemon=True)
    t.start()


def main():
    port = find_free_port(8085)
    url = f"http://localhost:{port}"

    banner = f"""[bold green]╔═══════════════════════════════════════════════════════════════════════╗[/bold green]
[bold green]║[/bold green]   [bold white]📱 SN ENERGY LABS - WHATSAPP AUTO SENDER[/bold white]                            [bold green]║[/bold green]
[bold green]║[/bold green]   [dim]Visible Screen Control • New Chat Search • Auto Personalized Msg[/dim]   [bold green]║[/bold green]
[bold green]╚═══════════════════════════════════════════════════════════════════════╝[/bold green]

[bold cyan]🚀 Dashboard Running At:[/bold cyan] [link={url}][bold underline]{url}[/bold underline][/link]
[dim]• Visible Chrome will control your screen to search numbers & send messages.[/dim]
[dim]• WhatsApp login session will be saved automatically in chrome_profile.[/dim]
[dim]• Press Ctrl+C in terminal to stop the server anytime.[/dim]
"""
    console.print(banner)

    # Automatically open dashboard in browser
    open_browser_delayed(url)

    # Run Uvicorn server
    uvicorn.run("server:app", host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    main()
