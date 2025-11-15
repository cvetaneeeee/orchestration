import re
import requests
from urllib.parse import urljoin
from typing import List, Tuple, Optional, Dict

# Adjust headers if you need cookies/authorization (usually not for app.js)
DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def fetch_scripts_text(page_url: str, script_urls: List[str]) -> List[Tuple[str,str]]:
    """
    Download scripts. Returns list of (absolute_url, text).
    Ignores scripts that fail to download.
    """
    texts = []
    for src in script_urls:
        full = urljoin(page_url, src)
        try:
            r = requests.get(full, headers=DEFAULT_HEADERS, timeout=15)
            if r.ok:
                texts.append((full, r.text))
        except Exception:
            continue
    return texts

def parse_bu_table(js_text: str) -> Tuple[Optional[List[str]], Optional[int]]:
    """
    Find the bu() array and the numeric offset used in vu (n = n - OFFSET).
    Returns (string_table, offset) or (None, None) if not found.
    """
    # Try to find function bu() { return [ ... ]; }
    m = re.search(r'function\s+bu\s*\(\)\s*\{\s*return\s*\[([^\]]+)\]', js_text, flags=re.S)
    if not m:
        # try variant: var bu = function(){ return [ ... ]; }
        m = re.search(r'var\s+bu\s*=\s*function\s*\(\)\s*\{\s*return\s*\[([^\]]+)\]', js_text, flags=re.S)
    if not m:
        # try: const arr = [ ... ]; function bu(){ return arr; }
        m2 = re.search(r'(?P<name>[_$a-zA-Z0-9]+)\s*=\s*\[([^\]]+)\]\s*;\s*function\s+bu\(\)\s*\{\s*return\s*\1\s*\}', js_text, flags=re.S)
        if m2:
            arr_content = m2.group(2)
        else:
            return None, None
    else:
        arr_content = m.group(1)

    # Extract quoted strings robustly
    strings = re.findall(r"""['"]((?:\\.|[^'"])*)['"]""", arr_content)
    if not strings:
        return None, None

    # Find the offset (n = n - OFFSET) inside vu definition
    off_m = re.search(r'return\s+n\s*=\s*n\s*-\s*(\d+)', js_text)
    if not off_m:
        off_m = re.search(r'return\s+n\s*=\s*n-([0-9]+)', js_text)
    offset = int(off_m.group(1)) if off_m else None
    return strings, offset

def resolve_qn(table: List[str], offset: int, idx: int) -> Optional[str]:
    """Resolve qn(idx) using table and offset."""
    try:
        i = int(idx)
    except Exception:
        return None
    pos = i - offset
    if 0 <= pos < len(table):
        return table[pos]
    return None

def assemble_from_parts(parts: List[str], table: List[str], offset: int) -> str:
    """
    Given a list of parts like 'qn(440)' or '"literal"', resolve them and join.
    parts is raw tokens e.g. ["e(440)", '"!p$7aD_"', "e(452)"]
    """
    out = []
    for p in parts:
        p = p.strip()
        # qn or e calls: qn(123) or e(123)
        m = re.search(r'(?:qn|e|t)\s*\(\s*(\d+)\s*\)', p)
        if m:
            val = resolve_qn(table, offset, int(m.group(1)))
            out.append(val or "")
            continue
        # string literal
        lit = re.match(r"""['"]((?:\\.|[^'"])*)['"]""", p)
        if lit:
            out.append(lit.group(1))
            continue
        # fallback: append raw token
        out.append(p)
    return "".join(out)

def extract_uwt_qwt_from_scripts(page_url: str, script_urls: List[str]) -> Dict[str, Optional[str]]:
    """
    Main helper. Returns dict {'Uwt': str|None, 'Qwt': str|None, 'notes': str}.
    """
    scripts = fetch_scripts_text(page_url, script_urls)
    if not scripts:
        return {"Uwt": None, "Qwt": None, "notes": "no scripts fetched"}

    table = None
    offset = None
    source = None
    for src, text in scripts:
        t, o = parse_bu_table(text)
        if t:
            table, offset, source = t, o, src
            break

    if not table:
        return {"Uwt": None, "Qwt": None, "notes": "could not find bu() table in scripts"}

    # Try to find the exact Uwt builder expression used in your snippet
    Uwt_val = None
    Qwt_val = None

    # 1) Look for let Uwt = ( () => { const e = qn; return [ ... ].join("") } )()
    uwt_pattern = re.compile(r'let\s+Uwt\s*=\s*\(\s*\(\)\s*=>\s*\{\s*const\s+e\s*=\s*qn\s*;\s*return\s*\[([^\]]+)\]\.join\([^\)]*\)\s*}', flags=re.S)
    qwt_pattern = re.compile(r'let\s+Qwt\s*=\s*\(\s*\(\)\s*=>\s*\{\s*const\s+e\s*=\s*qn\s*;\s*return\s*\[([^\]]+)\][^\n;]*\}\s*\)\s*\(\s*\)\s*;', flags=re.S)

    for src, text in scripts:
        m = uwt_pattern.search(text)
        if m:
            parts_raw = m.group(1)
            elems = [p.strip() for p in re.split(r'\s*,\s*', parts_raw)]
            Uwt_val = assemble_from_parts(elems, table, offset)
            break

    # fallback: direct Uwt = qn(458) style
    if not Uwt_val:
        for src, text in scripts:
            m = re.search(r'Uwt\s*=\s*qn\(\s*(\d+)\s*\)', text)
            if m:
                Uwt_val = resolve_qn(table, offset, int(m.group(1)))
                if Uwt_val:
                    break

    # Qwt: check explicit literal override first (seen in snippet)
    if not Qwt_val:
        for src, text in scripts:
            m = re.search(r'Qwt\s*=\s*["\']([0-9a-fA-F]{16,64})["\']', text)
            if m:
                Qwt_val = m.group(1)
                break

    # fallback: try builder pattern
    if not Qwt_val:
        for src, text in scripts:
            m = re.search(r'let\s+Qwt\s*=\s*\(\s*\(\)\s*=>\s*\{\s*const\s+e\s*=\s*qn\s*;\s*return\s*\[([^\]]+)\]\s*(?:\[[^\]]+\]\([^\)]*\))?', text, flags=re.S)
            if m:
                parts_raw = m.group(1)
                elems = [p.strip() for p in re.split(r'\s*,\s*', parts_raw)]
                Qwt_val = assemble_from_parts(elems, table, offset)
                break

    notes = f"source={source} offset={offset} table_len={len(table)}"
    return {"Uwt": Uwt_val, "Qwt": Qwt_val, "notes": notes}
