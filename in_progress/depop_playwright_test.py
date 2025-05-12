import csv
import os
import time
import json
import urllib.parse
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, slow_mo=100)
    page = browser.new_page(viewport={"width": 1280, "height": 800})

    page.goto("https://www.depop.com/explore/?moduleOrigin=meganav&sort=priceAscending&sizes=6-77.4-US%2C6-77.5-US&colours=grey%2Cblack&priceMin=1&priceMax=10&brands=76%2C326&groups=footwear&productTypes=boots%2Ctrainers&gender=male&conditions=used_excellent%2Cused_like_new&isDiscounted=true")

    time.sleep(100)
    browser.close