import asyncio
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright

# JS hook (wraps subtle.importKey and subtle.deriveKey to capture password and salt)
_CRYPTO_HOOK_JS = r"""
(() => {
  if (window.__cryptoHookInstalled) return;
  try {
    function bytesToString(buf) {
      try {
        if (!buf) return null;
        if (typeof buf === "string") return buf;
        if (buf instanceof ArrayBuffer) return new TextDecoder().decode(new Uint8Array(buf));
        if (ArrayBuffer.isView(buf)) {
          const view = buf.buffer ? new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength) : new Uint8Array(buf);
          return new TextDecoder().decode(view);
        }
        return null;
      } catch (e) { return null; }
    }
    function bytesToHex(buf) {
      try {
        let u8;
        if (!buf) return null;
        if (buf instanceof ArrayBuffer) u8 = new Uint8Array(buf);
        else if (ArrayBuffer.isView(buf)) u8 = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
        else return null;
        return Array.from(u8).map(b => b.toString(16).padStart(2, "0")).join("");
      } catch (e) { return null; }
    }

    const subtle = (window.crypto && window.crypto.subtle) ? window.crypto.subtle : null;
    if (!subtle) { window.__cryptoHookInstalled = true; return; }

    const origImportKey = subtle.importKey.bind(subtle);
    subtle.importKey = async function(format, keyData, algorithm, extractable, keyUsages) {
      try {
        if (format === "raw" && keyData) {
          const pw = bytesToString(keyData);
          if (pw) {
            window.__cryptoKeys = window.__cryptoKeys || {};
            if (!window.__cryptoKeys.password) window.__cryptoKeys.password = pw;
          }
        }
      } catch (e) { /* swallow */ }
      return origImportKey(format, keyData, algorithm, extractable, keyUsages);
    };

    const origDeriveKey = subtle.deriveKey ? subtle.deriveKey.bind(subtle) : null;
    if (origDeriveKey) {
      subtle.deriveKey = async function(derivationAlgorithm, baseKey, derivedKeyType, extractable, keyUsages) {
        try {
          const alg = derivationAlgorithm || {};
          const name = (alg.name || alg.algorithm || "").toString().toLowerCase();
          if (name.includes("pbkdf2") && alg.salt) {
            const hex = bytesToHex(alg.salt);
            window.__cryptoKeys = window.__cryptoKeys || {};
            if (!window.__cryptoKeys.salt) window.__cryptoKeys.salt = hex;
            if (!window.__cryptoKeys.iterations && alg.iterations) window.__cryptoKeys.iterations = alg.iterations;
            if (!window.__cryptoKeys.hash && alg.hash) {
              if (typeof alg.hash === "string") window.__cryptoKeys.hash = alg.hash;
              else if (alg.hash && alg.hash.name) window.__cryptoKeys.hash = alg.hash.name;
            }
          }
        } catch (e) { /* swallow */ }
        return origDeriveKey(derivationAlgorithm, baseKey, derivedKeyType, extractable, keyUsages);
      };
    }

    window.__cryptoHookInstalled = true;
  } catch (err) {
    window.__cryptoHookInstalled = true;
  }
})();
"""

async def extract_encryption_keys(url: str, timeout_ms: int = 15000) -> Dict[str, Optional[Any]]:
    """
    Navigate to `url`, hook WebCrypto calls and try to capture the password (Uwt) and salt (Qwt).
    Returns a dict: {
        "Uwt": <password string or None>,
        "Qwt": <salt hex string or None>,
        "iterations": <int or None>,
        "hash": <str or None>,
        "notes": <diagnostic string>
    }
    """
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        # Install hook before any script executes in this context
        await context.add_init_script(_CRYPTO_HOOK_JS)
        page = await context.new_page()

        notes = []
        try:
            await page.goto(url, wait_until="domcontentloaded")

            # First: try to wait for the hook to populate window.__cryptoKeys
            try:
                await page.wait_for_function(
                    "window.__cryptoKeys && (window.__cryptoKeys.password || window.__cryptoKeys.salt)",
                    timeout=timeout_ms
                )
                crypto_keys = await page.evaluate("() => window.__cryptoKeys || null")
                notes.append("captured-via-hook-wait")
            except Exception:
                # timed out — try a short sleep then read whatever exists
                await page.wait_for_timeout(3000)
                crypto_keys = await page.evaluate("() => window.__cryptoKeys || null")
                notes.append("timed-out-wait; attempted fallback read")

            # Normalize results
            result = {
                "Uwt": None,
                "Qwt": None,
                "iterations": None,
                "hash": None,
                "notes": ";".join(notes)
            }
            if crypto_keys:
                # password may be in crypto_keys.password as plaintext
                pw_val = crypto_keys.get("password") if isinstance(crypto_keys, dict) else None
                salt_val = crypto_keys.get("salt") if isinstance(crypto_keys, dict) else None
                iters = crypto_keys.get("iterations") if isinstance(crypto_keys, dict) else None
                hash_name = crypto_keys.get("hash") if isinstance(crypto_keys, dict) else None

                # sometimes password may be bytes-like string; keep as-is
                result["Uwt"] = pw_val
                result["Qwt"] = salt_val
                result["iterations"] = int(iters) if iters else None
                result["hash"] = hash_name

                result["notes"] += f";found_keys"
            else:
                result["notes"] += ";no_keys_captured"

            return result

        finally:
            await page.close()
            await context.close()
            await browser.close()

# Example runner
if __name__ == "__main__":
    async def main():
        url = "https://www.oddsportal.com/football/england/premier-league-2024-2025/bournemouth-leicester-44RKa9ke/"
        keys = await extract_encryption_keys(url)
        print("Result:", keys)

    asyncio.run(main())
