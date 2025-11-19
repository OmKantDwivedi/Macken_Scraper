import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta


# ================= CONFIG =================
DAYS_THRESHOLD = 2
MAX_CONCURRENT_REQUESTS = 50     # Higher = faster


# Convert X days ago to timestamp
def threshold_timestamp():
    return datetime.utcnow().timestamp() - DAYS_THRESHOLD * 86400


# ================= FETCH REDDIT JSON =================
async def fetch_json(session, url):
    if not url.endswith("/"):
        url += "/"

    api_url = url + ".json"

    try:
        async with session.get(api_url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except:
        return None


# ================= BUILD TREE =================
def build_tree(comment_listing):
    comment_map = {}
    children_map = {}

    def walk(comments):
        for c in comments:
            if c.get("kind") != "t1":
                continue

            data = c["data"]
            cid = data["id"]
            pid = data.get("parent_id")

            comment_map[cid] = data
            children_map.setdefault(pid, []).append(data)

            if data.get("replies") and isinstance(data["replies"], dict):
                walk(data["replies"]["data"]["children"])

    walk(comment_listing)
    return comment_map, children_map


# ================= RECURSIVE CHECK =================
def has_recent_reply(cid, comment_map, children_map, threshold):
    for reply in children_map.get(f"t1_{cid}", []):
        if reply["author"].lower() != "automoderator":
            if reply["created_utc"] >= threshold:
                return True

            if has_recent_reply(reply["id"], comment_map, children_map, threshold):
                return True

    return False


# ================= PROCESS SINGLE POST =================
async def process_post(session, url):
    data = await fetch_json(session, url)

    if not data or len(data) < 2:
        return [{
            "Parent": "No comments found",
            "Child1": None, "Child2": None, "Child3": None,
            "URL": url
        }]

    comments = data[1]["data"]["children"]
    comment_map, children_map = build_tree(comments)
    threshold = threshold_timestamp()

    # Top-level = parent comments (parent_id starts with t3_)
    parents = [
        c for c in comment_map.values()
        if c.get("parent_id", "").startswith("t3_")
        and c.get("author", "").lower() != "automoderator"
    ][:3]

    if not parents:
        return [{
            "Parent": "No comments found",
            "Child1": None, "Child2": None, "Child3": None,
            "URL": url
        }]

    rows = []

    for parent in parents:
        parent_status = "YES" if has_recent_reply(
            parent["id"], comment_map, children_map, threshold
        ) else "NO"

        parent_out = f"{parent['author']}({parent_status})"

        # Children (Top 3)
        child_comments = [
            r for r in children_map.get(f"t1_{parent['id']}", [])
            if r["author"].lower() != "automoderator"
        ][:3]

        child_out = []
        for child in child_comments:
            status = "YES" if has_recent_reply(
                child["id"], comment_map, children_map, threshold
            ) else "NO"
            child_out.append(f"{child['author']}({status})")

        while len(child_out) < 3:
            child_out.append(None)

        rows.append({
            "Parent": parent_out,
            "Child1": child_out[0],
            "Child2": child_out[1],
            "Child3": child_out[2],
            "URL": url
        })

    return rows


# ================= PROCESS ALL LINKS =================
async def process_all(url_list, output_file):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [asyncio.create_task(process_post(session, url)) for url in url_list]
        results = await asyncio.gather(*tasks)

    final_rows = []
    for r in results:
        final_rows.extend(r)

    pd.DataFrame(final_rows).to_csv(output_file, index=False)
    print("✅ Output saved to:", output_file)


# ================= ENTRY POINT =================
def run_csv(input_file, output_file):
    df = pd.read_csv(input_file)
    urls = df["url"].tolist()

    print(f"⚙ Processing {len(urls)} URLs using native Reddit JSON...")

    asyncio.run(process_all(urls, output_file))


if __name__ == "__main__":
    run_csv(
        "Copy of MaxBounty - 10OCT25 - Sheet1.csv",
        "output_json_async.csv"
    )
