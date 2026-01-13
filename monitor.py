import asyncio
import os
import time
import aiohttp
import zipfile
import urllib.parse
from io import BytesIO
from typing import Optional, Tuple, List
from playwright.async_api import async_playwright

# --- CONFIGURATION ---
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL") 

EXAM_CONFIG = {
    "ordinal_sem": "6th",
    "roman_sem": "VI",
    "session": "2025",
    "held_month": "November",
    "held_year": "2025"
}
REG_LIST = [
    "22156148040", "22156148042", "22156148018", "22156148051", "22156148012",
    "22156148001", "22156148002", "22156148003", "22156148004", "22156148005",
    "22156148006", "22156148007", "22156148008", "22156148009", "22156148011",
    "22156148013", "22156148014", "22156148015", "22156148016", "22156148017",
    "22156148019", "22156148020", "22156148021", "22156148022", "22156148023",
    "22156148024", "22156148026", "22156148027", "22156148028", "22156148029",
    "22156148030", "22156148031", "22156148032", "22156148033", "22156148034",
    "22156148035", "22156148036", "22156148037", "22156148038", "22156148039",
    "22156148041", "22156148044", "22156148045", "22156148046", "22156148047",
    "22156148048", "22156148050", "22156148052", "22156148053", "23156148901",
    "23156148902", "23156148903", "23156148904", "22101148001", "22101148002",
    "22101148003", "22101148004", "22101148005", "22101148006", "22101148007",
    "22101148008", "22101148009", "22101148010", "22101148011", "22101148012",
    "22101148013", "22101148014", "22101148015", "22101148016", "22101148019",
    "22101148021", "22101148023", "22101148024", "22101148025", "22101148026",
    "22101148027", "22101148028", "22101148029", "22101148030", "22101148031",
    "22101148033", "22101148034", "22101148035", "22101148036", "22101148038",
    "22101148039", "22101148040", "22101148041", "22101148042", "23101148901",
    "23101148902", "23101148903", "23101148904", "23101148905", "23101148906",
    "23101148908", "23101148909", "23101148910", "23101148911", "23101148913",
    "23101148914", "23101148915", "23101148916", "23101148918", "23101148919",
    "22103148001", "22103148004", "22103148006", "22103148007", "22103148008",
    "23103148901", "23103148902", "23103148903", "23103148904", "23103148905",
    "23103148906", "23103148907", "23103148908", "23103148909", "23103148910",
    "23103148911", "23103148912", "23103148913", "23103148914", "23103148916",
    "23103148917", "23103148918", "23103148920", "23103148921", "23103148922",
    "23103148923", "23103148924", "23103148925", "23103148926", "23103148928",
    "23103148930", "23103148931", "23103148932", "23103148933", "23103148934",
    "22104148001", "22104148002", "22104148003", "22104148004", "22104148005",
    "22104148006", "22104148007", "22104148008", "22104148009", "22104148010",
    "22104148012", "22104148013", "22104148014", "22104148015", "23104148901",
    "23104148902", "23104148903", "23104148904", "23104148905", "23104148906",
    "23104148907", "23104148908", "23102148901", "23102148902", "23102148903",
    "23102148904", "23102148905"
]

# --- SETTINGS ---
CHECK_INTERVAL = 5          
CONTINUOUS_DURATION = 900   
CONCURRENCY_LIMIT = 6       
SCHEDULED_INTERVAL = 600    
DOWN_REMINDER_DELAY = 600   

# FIXED: Set to 7.5 MB to be safe for Standard Discord Servers (Limit is 8MB-10MB)
MAX_ZIP_SIZE_BYTES = 7.5 * 1024 * 1024 

class DiscordMonitor:
    def __init__(self):
        self.last_status: Optional[str] = None
        self.last_scheduled_time: float = 0
        self.last_down_alert_time: float = 0
        self.rate_limit_remaining = 5
        self.rate_limit_reset = 0
        self.check_browser = None
        self.check_page = None

    # --- RATE LIMITED DISCORD MESSENGER ---
    async def send_discord_message(self, content: str) -> bool:
        if not DISCORD_WEBHOOK_URL: return False
        
        now = time.time()
        if self.rate_limit_remaining <= 0 and now < self.rate_limit_reset:
            await asyncio.sleep(self.rate_limit_reset - now)
            
        payload = {"content": content, "username": "BEU Monitor"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(DISCORD_WEBHOOK_URL, json=payload) as resp:
                    self.rate_limit_remaining = int(resp.headers.get("X-RateLimit-Remaining", 5))
                    reset_after = resp.headers.get("X-RateLimit-Reset-After")
                    if reset_after: self.rate_limit_reset = now + float(reset_after)
                    
                    if resp.status == 429: 
                        retry = float(resp.headers.get("retry-after", 1))
                        await asyncio.sleep(retry)
                        return await self.send_discord_message(content)
                    return resp.status in (200, 204)
        except:
            return False

    async def send_file(self, filename: str, data: BytesIO, content: str = "") -> bool:
        """
        Sends a file WITH a message content to ensure context.
        Includes error printing to debug delivery failures.
        """
        if not DISCORD_WEBHOOK_URL: return False
        
        form = aiohttp.FormData()
        # Reset pointer to start of file
        data.seek(0)
        
        # Add the message text if provided
        if content:
            form.add_field("content", content)
            
        form.add_field("username", "BEU Monitor")
        # Add the file
        form.add_field("file", data, filename=filename, content_type="application/zip")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(DISCORD_WEBHOOK_URL, data=form, timeout=600) as resp:
                    
                    # Handle Rate Limits
                    if resp.status == 429:
                        retry = float(resp.headers.get("retry-after", 1))
                        print(f"Rate limited. Waiting {retry}s...")
                        await asyncio.sleep(retry)
                        return await self.send_file(filename, data, content)
                    
                    # Debugging for failures
                    if resp.status not in (200, 204):
                        error_text = await resp.text()
                        print(f"❌ Upload Failed! Status: {resp.status} | Response: {error_text}")
                        return False
                        
                    return True
        except Exception as e:
            print(f"❌ Exception during upload: {e}")
            return False

    # --- SITE LOGIC ---
    def construct_url(self, reg_no):
        params = {
            'name': f"B.Tech. {EXAM_CONFIG['ordinal_sem']} Semester Examination, {EXAM_CONFIG['session']}",
            'semester': EXAM_CONFIG['roman_sem'],
            'session': EXAM_CONFIG['session'],
            'regNo': str(reg_no),
            'exam_held': f"{EXAM_CONFIG['held_month']}/{EXAM_CONFIG['held_year']}"
        }
        return f"https://beu-bih.ac.in/result-three?{urllib.parse.urlencode(params)}"

    async def check_connection(self) -> str:
        test_reg = REG_LIST[0] 
        canary_url = self.construct_url(test_reg)
        try:
            if not self.check_page: return "DOWN"
            await self.check_page.goto(canary_url, timeout=15000)
            try:
                await self.check_page.wait_for_selector(f"text={test_reg}", timeout=5000)
                return "UP"
            except:
                return "DOWN"
        except Exception as e:
            return "DOWN"

    async def fetch_student_pdf(self, context, reg_no, semaphore) -> Tuple[str, Optional[bytes]]:
        async with semaphore:
            page = await context.new_page()
            try:
                await page.goto(self.construct_url(reg_no), timeout=40000)
                try:
                    await page.wait_for_selector(f"text={reg_no}", timeout=10000)
                except:
                    pass 
                pdf = await page.pdf(format="A4", print_background=True)
                await page.close()
                return (reg_no, pdf)
            except Exception as e:
                print(f"Failed {reg_no}: {e}")
                await page.close()
                return (reg_no, None)

    async def download_all_pdfs(self) -> List[Tuple[str, Optional[bytes]]]:
        """Downloads all PDFs and returns them in a list"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0")
            sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
            
            tasks = [self.fetch_student_pdf(context, reg, sem) for reg in REG_LIST]
            results = await asyncio.gather(*tasks)
            await browser.close()
            return results

    async def chunk_and_upload_results(self, results):
        """
        Splits results into multiple ZIPs of ~7.5MB each.
        """
        current_idx = 1
        current_buffer = BytesIO()
        current_zip = zipfile.ZipFile(current_buffer, "w", zipfile.ZIP_DEFLATED)
        
        for reg, pdf in results:
            if pdf:
                # Check size before writing
                if current_buffer.tell() > MAX_ZIP_SIZE_BYTES:
                    # Finalize current chunk
                    current_zip.close()
                    file_size = current_buffer.tell() / (1024*1024)
                    filename = f"Results_Part{current_idx}.zip"
                    
                    # Upload WITH message
                    msg = f"📦 **Batch {current_idx} Ready** ({file_size:.2f} MB)"
                    print(f"Uploading {filename}...")
                    success = await self.send_file(filename, current_buffer, content=msg)
                    
                    if not success:
                        print(f"⚠️ Failed to upload {filename}!")

                    # Reset for next chunk
                    current_idx += 1
                    current_buffer = BytesIO()
                    current_zip = zipfile.ZipFile(current_buffer, "w", zipfile.ZIP_DEFLATED)
                
                # Write to current zip
                current_zip.writestr(f"Result_{reg}.pdf", pdf)
            else:
                current_zip.writestr(f"MISSING_{reg}.txt", "Failed to download.")

        # Upload the final remaining chunk
        if current_buffer.tell() > 0:
            current_zip.close()
            file_size = current_buffer.tell() / (1024*1024)
            filename = f"Results_Part{current_idx}.zip"
            msg = f"📦 **Final Batch {current_idx} Ready** ({file_size:.2f} MB)"
            print(f"Uploading {filename}...")
            await self.send_file(filename, current_buffer, content=msg)

    async def continuous_status(self, end_time):
        print("Entering Continuous Status Loop...")
        while time.time() < end_time:
            left = int(end_time - time.time())
            if left <= 0: break
            await self.send_discord_message(f"✅ Website still UP ({left}s left)")
            await asyncio.sleep(CHECK_INTERVAL)

    # --- MAIN LOOP ---
    async def run(self):
        print(f"Monitor Started. Run Duration: {CONTINUOUS_DURATION}s")
        await self.send_discord_message("🔍 **Monitor Started** (Playwright Check + 7.5MB Split)")
        
        async with async_playwright() as p:
            self.check_browser = await p.chromium.launch(headless=True)
            self.check_page = await self.check_browser.new_context()
            self.check_page = await self.check_page.new_page()

            start_time = time.time()
            end_time = start_time + CONTINUOUS_DURATION
            
            try:
                while time.time() < end_time:
                    status = await self.check_connection()
                    now = time.time()

                    if status == "UP":
                        if self.last_status != "UP":
                            await self.send_discord_message("🚨 **SITE IS LIVE!** Starting Bulk Download...")
                            
                            # 1. Download ALL results first
                            results = await self.download_all_pdfs()
                            count = sum(1 for _, pdf in results if pdf)
                            await self.send_discord_message(f"📥 Downloaded {count} PDFs. Compressing...")

                            # 2. Chunk and Upload (Handles 8MB limit)
                            await self.chunk_and_upload_results(results)
                            
                            await self.send_discord_message("✅ **All Batches Uploaded Successfully**")
                            await self.continuous_status(end_time)
                            return 

                    elif status == "DOWN":
                        if self.last_status == "UP":
                            await self.send_discord_message("🔴 Website went **DOWN**")
                            self.last_down_alert_time = now
                        elif self.last_status is None:
                            await self.send_discord_message("🔴 Website is currently **DOWN**")
                            self.last_down_alert_time = now
                        elif (now - self.last_down_alert_time) > DOWN_REMINDER_DELAY:
                            await self.send_discord_message("🔴 Reminder: Website is still **DOWN**")
                            self.last_down_alert_time = now
                    
                    self.last_status = status
                    await asyncio.sleep(CHECK_INTERVAL)
            
            finally:
                if self.check_browser:
                    await self.check_browser.close()

if __name__ == "__main__":
    asyncio.run(DiscordMonitor().run())
