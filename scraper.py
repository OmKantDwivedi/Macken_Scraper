import praw
import pandas as pd
from datetime import datetime, timedelta
import time

# ========== CONFIG ========
DAYS_THRESHOLD = 2
MAX_RETRIES = 3
RETRY_DELAY = 1

# ========== PRAW SETUP ==========
reddit = praw.Reddit(
    client_id="BG_Gmad1Nw1x7YB_IdnQCg",
    client_secret="x2_0hd9jsvnLgw5CO-Mmc84xNwUbhQ",
    user_agent="Mackayn-scraper/1.0 by u/Sure-Author-7442"
)

# ========== RECURSIVE CHECK ==========
def has_recent_reply(comment, threshold):
    try:
        for reply in comment.replies:
            if str(reply.author).lower() == "automoderator":
                continue

            reply_time = datetime.utcfromtimestamp(reply.created_utc)
            if reply_time >= threshold:
                return True

            if has_recent_reply(reply, threshold):
                return True
    except:
        pass
    return False

# Format helper
def fmt(author, status):
    if author is None:
        return None
    return f"{author}({status})"


# ========== PROCESS SINGLE REDDIT URL ==========
def process_url(url):
    for attempt in range(MAX_RETRIES):
        try:
            submission = reddit.submission(url=url)
            submission.comments.replace_more(limit=None)

            threshold = datetime.utcnow() - timedelta(days=DAYS_THRESHOLD)

            # ===== Get TOP 3 parent comments (skip AutoModerator) =====
            parents = []
            for c in submission.comments:
                if str(c.author).lower() == "automoderator":
                    continue
                parents.append(c)
                if len(parents) == 3:
                    break

            rows = []

            for parent in parents:
                parent_status = "YES" if has_recent_reply(parent, threshold) else "NO"
                parent_out = fmt(str(parent.author), parent_status)

                # ===== Top 3 replies (skip AutoModerator) =====
                child_list = []
                for reply in parent.replies:
                    if str(reply.author).lower() == "automoderator":
                        continue

                    status = "YES" if has_recent_reply(reply, threshold) else "NO"
                    child_list.append(fmt(str(reply.author), status))

                    if len(child_list) == 3:
                        break

                # Pad to always have 3 replies
                while len(child_list) < 3:
                    child_list.append(None)

                rows.append({
                    "Parent": parent_out,
                    "Child1": child_list[0],
                    "Child2": child_list[1],
                    "Child3": child_list[2],
                })

            return rows

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                return [{"Parent": f"Error:{e}", "Child1": None, "Child2": None, "Child3": None}]
            time.sleep(RETRY_DELAY)


# ========== CSV PROCESSING ==========
def process_csv(input_file, output_file):
    df = pd.read_csv(input_file)
    url_list = df["url"].tolist()

    final = []
    for url in url_list:
        final.extend(process_url(url))

    pd.DataFrame(final).to_csv(output_file, index=False)
    print("✅ Output saved to:", output_file)


# ========== RUN ==========
if __name__ == "__main__":
    process_csv("Copy of MaxBounty - 10OCT25 - Sheet1.csv", "output.csv")
