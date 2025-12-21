import zipfile
import io
import re

def _download_bytes(url: str) -> bytes:
    """
    Download bytes from url. Handles Google Drive confirm tokens if needed.
    """
    s = requests.Session()
    r = s.get(url, timeout=180)
    r.raise_for_status()

    # If Drive returns an HTML confirmation page, look for confirm token
    ctype = r.headers.get("Content-Type", "")
    if "text/html" in ctype.lower():
        m = re.search(r"confirm=([0-9A-Za-z_]+)", r.text)
        if m:
            token = m.group(1)
            sep = "&" if "?" in url else "?"
            url2 = f"{url}{sep}confirm={token}"
            r2 = s.get(url2, timeout=180)
            r2.raise_for_status()
            return r2.content

    return r.content

def ensure_db_present():
    db_path = Path(DB_LOCAL_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # If already present and non-trivial size, keep it
    if db_path.exists() and db_path.stat().st_size > 10_000_000:
        return

    if not DB_URL:
        raise RuntimeError("ILLUSTRIA_DB_URL is not set and DB file is missing.")

    payload = _download_bytes(DB_URL)

    if DB_URL.lower().endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(payload)) as z:
            db_names = [n for n in z.namelist() if n.lower().endswith(".db")]
            if not db_names:
                raise RuntimeError("ZIP did not contain a .db file")
            with z.open(db_names[0]) as src, open(db_path, "wb") as dst:
                dst.write(src.read())
    else:
        with open(db_path, "wb") as f:
            f.write(payload)
