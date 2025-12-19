# CONFIGURATION TEMPLATE
#
# This file is SAFE TO COMMIT.
# Copy this file to `config_local.py` and fill in the values
# using your own browser session data.
#
# Required tools:
# - Google Chrome / Chromium
# - Browser DevTools (F12)
#
# -------------------------------------------------


# 1) STABLE ID
# -------------------------------------------------
# How to get:
# - Open Eventbrite events page
#   (This project uses "All Events in Dublin")
# - Open DevTools (F12)
# - Go to Network → Fetch/XHR
# - Click a request named "destination/search"
# - In Headers → Request URL, copy the value after:
#     ?stable_id=
#
STABLE_ID = "PASTE_STABLE_ID_HERE"


# 2) BASE URL (do not change)
# -------------------------------------------------
BASE_URL = "https://www.eventbrite.com/api/v3/destination/search/"


# 3) HEADERS
# -------------------------------------------------
# How to get:
# - In DevTools → Network → destination/search
# - Copy Request Headers
# - Paste them into ChatGPT (or similar) to convert to Python dict
# - REMOVE any cookies from headers
# - Paste the result below
#
HEADERS = {
    # Paste your headers here
}


# 4) COOKIES
# -------------------------------------------------
# How to get:
# Option A:
# - DevTools → Network → destination/search → Headers → Cookie
#
# Option B:
# - DevTools → Application → Cookies → eventbrite.com
#
# Convert cookies into a Python dictionary and paste below.
#
# These are REQUIRED for live fetching.
#
COOKIES = {
    # Paste your cookies here
}
