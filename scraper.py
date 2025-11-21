import praw
import pandas as pd
from datetime import datetime, timedelta
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== CONFIG ==========
DAYS_THRESHOLD = 2.2
MAX_RETRIES = 3
RETRY_DELAY = 1
MAX_THREADS = 10        # Increase or decrease based on your CPU


# ========== PRAW SETUP ==========
reddit = praw.Reddit(
    client_id="BG_Gmad1Nw1x7YB_IdnQCg",
    client_secret="x2_0hd9jsvnLgw5CO-Mmc84xNwUbhQ",
    user_agent="Mackayn-scraper/1.0 by u/Sure-Author-7442"
)


# ========== CHECK REPLIES (OPTIMIZED) ==========
def has_recent_reply(comment, threshold):
    """
    Recursively checks if a comment or its replies
    have activity newer than threshold.
    """
    try:
        for reply in comment.replies:
            if reply.author and reply.author.name.lower() == "automoderator":
                continue

            if reply.created_utc >= threshold:
                return True

            if has_recent_reply(reply, threshold):
                return True

    except Exception:
        pass

    return False


# Format author output
def fmt(author, status):
    return f"{author}({status})" if author else None


# ========== PROCESS SINGLE URL ==========
def process_url(url):
    for attempt in range(MAX_RETRIES):
        try:
            submission = reddit.submission(url=url)
            submission.comments.replace_more(limit=None)

            threshold = datetime.utcnow().timestamp() - DAYS_THRESHOLD * 86400

            # ===== Get TOP 3 parent comments (skip AutoModerator) =====
            parents = [
                c for c in submission.comments
                if c.author and c.author.name.lower() != "automoderator"
            ][:3]

            rows = []

            for parent in parents:

                parent_status = "YES" if has_recent_reply(parent, threshold) else "NO"
                parent_out = fmt(parent.author.name, parent_status)

                # ===== Top 3 replies (skip AutoModerator) =====
                valid_replies = [
                    r for r in parent.replies
                    if r.author and r.author.name.lower() != "automoderator"
                ]

                child_list = []
                for reply in valid_replies[:3]:
                    status = "YES" if has_recent_reply(reply, threshold) else "NO"
                    child_list.append(fmt(reply.author.name, status))

                while len(child_list) < 3:
                    child_list.append(None)

                rows.append({
                    "Parent": parent_out,
                    "Child1": child_list[0],
                    "Child2": child_list[1],
                    "Child3": child_list[2],
                    "URL": url
                })

            return rows

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return [{
                    "Parent": f"Error: {e}",
                    "Child1": None,
                    "Child2": None,
                    "Child3": None,
                    "URL": url
                }]

            time.sleep(RETRY_DELAY)


# ========== MULTI-THREAD CSV PROCESSING ==========
def process_csv(input_file, output_file):
    df = pd.read_csv(input_file)
    url_list = df["url"].tolist()

    final = []

    print(f"⚙ Processing {len(url_list)} URLs using {MAX_THREADS} threads...")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_url, url): url for url in url_list}

        for future in as_completed(futures):
            rows = future.result()
            final.extend(rows)

    pd.DataFrame(final).to_csv(output_file, index=False)
    print("✅ Output saved to:", output_file)


# ========== RUN ==========
if __name__ == "__main__":
    process_csv(
        "21st urls - Sheet1.csv",
        "output.csv"
    )
