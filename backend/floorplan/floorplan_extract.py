#!/usr/bin/env python3
"""Floorplan booth extraction utility.

Goal: detect booth rectangles in expo floorplans, OCR the booth labels,
merge contiguous squares into larger booths, and export a confidence-scored CSV.

Usage:
  python3 floorplan_extract.py --image floorplan.png --output booths.csv
  python3 floorplan_extract.py --url https://example.com/floorplan.png --output booths.csv

Dependencies (optional but recommended):
  - pillow
  - numpy
  - opencv-python
  - pytesseract
  - requests
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import re
import warnings
from collections import deque
from dataclasses import dataclass
from html.parser import HTMLParser
from statistics import median
from typing import Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

try:
    from PIL import Image
except Exception as e:  # pragma: no cover
    raise SystemExit("Pillow is required") from e


CONFIDENCE_HIGH = 0.70
CONFIDENCE_MEDIUM = 0.45


def _configure_tesseract() -> bool:
    """Configure tesseract executable path if available.

    Looks for:
    1. ~/.local/bin/tesseract-wrapper (wrapper with LD_LIBRARY_PATH)
    2. ~/.local/bin/tesseract (local install with manual LD_LIBRARY_PATH)
    3. System tesseract in PATH (only if it actually works)

    Returns True if tesseract is available and functional.
    """
    import os
    import subprocess
    from pathlib import Path

    def _tesseract_works(tess_cmd: str) -> bool:
        """Test if tesseract executable actually works."""
        try:
            result = subprocess.run(
                [tess_cmd, "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False

    # Priority 1: Wrapper script (has LD_LIBRARY_PATH baked in)
    wrapper = Path.home() / ".local" / "bin" / "tesseract-wrapper"
    if wrapper.exists() and wrapper.is_file():
        try:
            if _tesseract_works(str(wrapper)):
                import pytesseract
                pytesseract.pytesseract.tesseract_cmd = str(wrapper)
                return True
        except Exception:
            pass

    # Priority 2: Local tesseract with manual library path setup
    local_tess = Path.home() / ".local" / "bin" / "tesseract"
    if local_tess.exists() and local_tess.is_file():
        lib_path = Path.home() / ".local" / "lib"
        if lib_path.exists():
            os.environ["LD_LIBRARY_PATH"] = str(lib_path) + ":" + os.environ.get("LD_LIBRARY_PATH", "")
            os.environ["TESSDATA_PREFIX"] = str(Path.home() / ".local" / "share" / "tessdata")
        try:
            if _tesseract_works(str(local_tess)):
                import pytesseract
                pytesseract.pytesseract.tesseract_cmd = str(local_tess)
                return True
        except Exception:
            pass

    # Priority 3: System tesseract (only if it actually works)
    try:
        import pytesseract
        import shutil
        system_tess = shutil.which("tesseract")
        if system_tess and _tesseract_works(system_tess):
            pytesseract.pytesseract.tesseract_cmd = system_tess
            return True
    except Exception:
        pass

    return False


_TESSERACT_AVAILABLE = _configure_tesseract()


@dataclass
class OCRToken:
    text: str
    confidence: float


@dataclass
class RegionOCR:
    booth_numbers: str
    organisation_name: str
    tokens: List[OCRToken]
    raw_text: str


@dataclass
class Calibration:
    base_unit: int = 10
    square_width_px: Optional[float] = None
    square_height_px: Optional[float] = None


@dataclass
class BoothCandidate:
    booth_numbers: str
    organisation_name: str
    booth_width: Optional[int]
    booth_height: Optional[int]
    confidence: str


@dataclass
class CandidateRecord:
    region: Tuple[int, int, int, int]
    ocr: RegionOCR
    booth_width: Optional[int]
    booth_height: Optional[int]
    confidence_score: float
    confidence: str


@dataclass
class GroupedBooth:
    regions: List[Tuple[int, int, int, int]]
    booth_numbers: List[str]
    organisation_names: List[str]


class _MapHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.meta_images: List[str] = []
        self.images: List[str] = []
        self.iframes: List[str] = []
        self.links: List[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        attr_map = {k.lower(): v for k, v in attrs if k and v}
        tag = tag.lower()
        if tag == "meta":
            name = (attr_map.get("property") or attr_map.get("name") or "").lower()
            content = attr_map.get("content")
            if content and name in {"og:image", "twitter:image", "twitter:image:src"}:
                self.meta_images.append(content)
        elif tag == "img":
            src = attr_map.get("src") or attr_map.get("data-src")
            if src:
                self.images.append(src)
        elif tag == "iframe":
            src = attr_map.get("src")
            if src:
                self.iframes.append(src)
        elif tag == "a":
            href = attr_map.get("href")
            if href:
                self.links.append(href)


def load_image(path: Optional[str] = None, url: Optional[str] = None) -> Image.Image:
    if path:
        return Image.open(path).convert("RGB")
    if url:
        if requests is None:
            raise SystemExit("requests is required for URL input")
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        return Image.open(io.BytesIO(resp.content)).convert("RGB")
    raise SystemExit("Provide --image or --url")


def _looks_like_image_url(url: str) -> bool:
    lowered = url.lower().split("?", 1)[0]
    return lowered.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif"))


def _looks_like_pdf_url(url: str) -> bool:
    lowered = url.lower().split("?", 1)[0]
    return lowered.endswith(".pdf")


def _resolve_map_snapshot_url(map_url: str) -> str:
    if requests is None:
        raise SystemExit("requests is required for --map-url input")

    resp = requests.get(map_url, timeout=60)
    resp.raise_for_status()
    content_type = (resp.headers.get("content-type") or "").lower()

    if content_type.startswith("image/") or _looks_like_image_url(map_url):
        return map_url
    if "pdf" in content_type or _looks_like_pdf_url(map_url):
        return map_url

    html = resp.text
    parser = _MapHTMLParser()
    parser.feed(html)

    candidates: List[str] = []
    candidates.extend(parser.meta_images)
    candidates.extend(parser.images)
    candidates.extend(parser.iframes)
    candidates.extend(parser.links)

    preferred: List[str] = []
    fallback: List[str] = []
    for candidate in candidates:
        absolute = urljoin(map_url, candidate)
        if _looks_like_image_url(absolute):
            preferred.append(absolute)
        elif any(token in absolute.lower() for token in ("screenshot", "snapshot", "preview", "export", "thumbnail", "static")):
            fallback.append(absolute)
        elif _looks_like_pdf_url(absolute):
            fallback.append(absolute)

    ordered = preferred + fallback
    if ordered:
        return ordered[0]

    raise SystemExit(
        "Could not resolve a screenshot/static asset from --map-url. Provide a direct image/PDF URL or capture a screenshot first."
    )


def _load_map_as_image(map_url: str, page_number: int = 1) -> Image.Image:
    resolved = _resolve_map_snapshot_url(map_url)
    if _looks_like_pdf_url(resolved):
        if requests is None:
            raise SystemExit("requests is required for --map-url input")
        resp = requests.get(resolved, timeout=60)
        resp.raise_for_status()
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name
        try:
            return _load_pdf_as_image(tmp_path, page_number=page_number)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    return load_image(url=resolved)


def _load_pdf_as_image(path: str, page_number: int = 1) -> Image.Image:
    """Load one PDF page as an RGB image.

    Page numbers are 1-based for CLI usability.
    Tries Pillow's PDF support first, then optional rendering libraries if present.
    """
    if page_number < 1:
        raise SystemExit("--pdf-page must be 1 or greater")

    page_index = page_number - 1

    try:
        pdf = Image.open(path)
        pdf.seek(page_index)
        return pdf.convert("RGB")
    except EOFError:
        raise SystemExit(f"PDF page {page_number} not found in {path}")
    except Exception:
        pass

    try:
        import fitz  # type: ignore

        doc = fitz.open(path)
        if doc.page_count == 0:
            raise SystemExit(f"PDF has no pages: {path}")
        if page_index >= doc.page_count:
            raise SystemExit(f"PDF page {page_number} not found in {path}; page count is {doc.page_count}")
        page = doc.load_page(page_index)
        pix = page.get_pixmap(matrix=fitz.Matrix(2.0, 2.0), alpha=False)
        return Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    except SystemExit:
        raise
    except Exception:
        pass

    try:
        import pypdfium2 as pdfium  # type: ignore

        pdf = pdfium.PdfDocument(path)
        if len(pdf) == 0:
            raise SystemExit(f"PDF has no pages: {path}")
        if page_index >= len(pdf):
            raise SystemExit(f"PDF page {page_number} not found in {path}; page count is {len(pdf)}")
        page = pdf[page_index]
        bitmap = page.render(scale=2.0)
        return bitmap.to_pil().convert("RGB")
    except SystemExit:
        raise
    except Exception:
        pass

    try:
        import pdfplumber  # type: ignore

        with pdfplumber.open(path) as pdf:
            if not pdf.pages:
                raise SystemExit(f"PDF has no pages: {path}")
            if page_index >= len(pdf.pages):
                raise SystemExit(f"PDF page {page_number} not found in {path}; page count is {len(pdf.pages)}")
            page = pdf.pages[page_index]
            im = page.to_image(resolution=200).original
            return im.convert("RGB")
    except SystemExit:
        raise
    except Exception:
        pass

    raise SystemExit(
        "PDF input requires a rendering backend. Install PyMuPDF (recommended), Pillow PDF support, pdfplumber, or pypdfium2.")


def safe_resize(img: Image.Image, max_dim: int = 3000) -> Image.Image:
    w, h = img.size
    scale = min(1.0, max_dim / max(w, h))
    if scale >= 1.0:
        return img
    return img.resize((int(w * scale), int(h * scale)))


def _cv2():
    try:
        import cv2  # type: ignore
        import numpy as np  # type: ignore
        return cv2, np
    except Exception:
        return None, None


def _component_regions(mask) -> List[Tuple[int, int, int, int]]:
    cv2, np = _cv2()
    if cv2 is None:
        return []

    num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(mask, 8)
    regions: List[Tuple[int, int, int, int]] = []
    h, w = mask.shape[:2]
    img_area = h * w

    for i in range(1, num_labels):
        x, y, rw, rh, area = stats[i]
        if area < img_area * 0.00002 or area > img_area * 0.25:
            continue
        if min(rw, rh) < 10:
            continue
        aspect = max(rw / max(1, rh), rh / max(1, rw))
        if aspect > 18:
            continue
        regions.append((int(x), int(y), int(x + rw), int(y + rh)))

    return _merge_nearby(regions)


def _component_regions_pure(mask) -> List[Tuple[int, int, int, int]]:
    import numpy as np

    mask = (mask > 0).astype(np.uint8)
    h, w = mask.shape[:2]
    visited = np.zeros((h, w), dtype=bool)
    regions: List[Tuple[int, int, int, int]] = []
    img_area = h * w

    for y in range(h):
        row = mask[y]
        xs = np.where((row == 1) & (~visited[y]))[0]
        for x in xs:
            if visited[y, x]:
                continue
            q = deque([(x, y)])
            visited[y, x] = True
            min_x = max_x = x
            min_y = max_y = y
            area = 0
            while q:
                cx, cy = q.popleft()
                area += 1
                if cx < min_x:
                    min_x = cx
                if cy < min_y:
                    min_y = cy
                if cx > max_x:
                    max_x = cx
                if cy > max_y:
                    max_y = cy
                for nx, ny in ((cx - 1, cy), (cx + 1, cy), (cx, cy - 1), (cx, cy + 1)):
                    if 0 <= nx < w and 0 <= ny < h and not visited[ny, nx] and mask[ny, nx]:
                        visited[ny, nx] = True
                        q.append((nx, ny))

            bbox_area = (max_x - min_x + 1) * (max_y - min_y + 1)
            if area < img_area * 0.00002 or area > img_area * 0.25:
                continue
            if min(max_x - min_x + 1, max_y - min_y + 1) < 10:
                continue
            if bbox_area <= 0:
                continue
            aspect = max((max_x - min_x + 1) / max(1, (max_y - min_y + 1)), (max_y - min_y + 1) / max(1, (max_x - min_x + 1)))
            if aspect > 18:
                continue
            regions.append((min_x, min_y, max_x + 1, max_y + 1))

    return _merge_nearby(regions)


def _merge_nearby(regions: Sequence[Tuple[int, int, int, int]]) -> List[Tuple[int, int, int, int]]:
    merged: List[Tuple[int, int, int, int]] = []
    for r in regions:
        x1, y1, x2, y2 = r
        placed = False
        for i, m in enumerate(merged):
            mx1, my1, mx2, my2 = m
            if not (x2 < mx1 - 5 or mx2 < x1 - 5 or y2 < my1 - 5 or my2 < y1 - 5):
                merged[i] = (min(x1, mx1), min(y1, my1), max(x2, mx2), max(y2, my2))
                placed = True
                break
        if not placed:
            merged.append(r)
    merged.sort(key=lambda r: (r[1], r[0]))
    return merged


def detect_grid_candidates(img: Image.Image) -> List[Tuple[int, int, int, int]]:
    """Detect booth-like rectangular regions using a conservative OpenCV pass."""
    cv2, np = _cv2()
    if cv2 is None:
        import numpy as np  # type: ignore
        fallback = img.copy()
        fallback.thumbnail((1600, 1600))
        arr = np.array(fallback)
        sat = np.max(arr, axis=2) - np.min(arr, axis=2)
        color_mask = ((sat > 25) & (np.min(arr, axis=2) < 250)).astype(np.uint8)
        color_mask = np.where(color_mask, 1, 0)
        return _component_regions_pure(color_mask)

    arr = np.array(img)
    gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    thr = cv2.adaptiveThreshold(
        blur, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 31, 11
    )

    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (25, 1))
    v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 25))
    horiz = cv2.morphologyEx(thr, cv2.MORPH_OPEN, h_kernel)
    vert = cv2.morphologyEx(thr, cv2.MORPH_OPEN, v_kernel)
    merged = cv2.bitwise_or(horiz, vert)
    merged = cv2.morphologyEx(merged, cv2.MORPH_CLOSE, cv2.getStructuringElement(cv2.MORPH_RECT, (5, 5)))

    contours, _ = cv2.findContours(merged, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    regions: List[Tuple[int, int, int, int]] = []
    img_area = img.width * img.height

    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        area = w * h
        if area < img_area * 0.00002 or area > img_area * 0.15:
            continue
        if min(w, h) < 18:
            continue
        aspect = max(w / max(1, h), h / max(1, w))
        if aspect > 12:
            continue
        roi = merged[y : y + h, x : x + w]
        fill = cv2.countNonZero(roi) / float(area)
        if fill < 0.02:
            continue
        regions.append((x, y, x + w, y + h))

    line_regions = _merge_nearby(regions)

    sat = np.max(arr, axis=2) - np.min(arr, axis=2)
    color_mask = ((sat > 25) & (np.min(arr, axis=2) < 250)).astype(np.uint8) * 255
    color_mask = cv2.morphologyEx(
        color_mask,
        cv2.MORPH_CLOSE,
        cv2.getStructuringElement(cv2.MORPH_RECT, (7, 7)),
    )
    color_mask = cv2.morphologyEx(
        color_mask,
        cv2.MORPH_OPEN,
        cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3)),
    )
    color_regions = _component_regions(color_mask)

    if len(color_regions) > max(len(line_regions) * 1.25, len(line_regions) + 20):
        return color_regions
    if line_regions:
        return line_regions
    return color_regions


def _normalize_text(s: str) -> str:
    return " ".join(s.replace("\n", " ").split())


def _normalize_accents(s: str) -> str:
    """Normalize accented characters to ASCII equivalents.
    
    Common OCR errors: é→e, è→e, à→a, etc.
    Also handles common OCR misreads like:
    - Gé6 → G6 (accented char in booth number)
    - 0→O confusion in context
    """
    import unicodedata
    # Normalize to decomposed form, then remove combining marks
    normalized = unicodedata.normalize('NFD', s)
    ascii_only = ''.join(c for c in normalized if not unicodedata.combining(c))
    return ascii_only


def _fix_ocr_booth_artifacts(text: str) -> str:
    """Fix common OCR artifacts in booth numbers.
    
    Patterns:
    - Gé6 → G6 (remove accented vowel between letter and digit)
    - Ge6 → G6 (remove stray vowel between letter prefix and digits)
    - G!6 → G1 (common misread)
    """
    # Pattern: Letter followed by accented/stray vowel then digits
    # This catches Gé6, Ge6, Gè6, etc.
    def fix_prefix_vowel(match):
        prefix = match.group(1)
        digits = match.group(3)
        return f"{prefix}{digits}"
    
    # Fix patterns like Gé6, Ge6, Gè6 -> G6
    # Pattern: (letter)(accented vowel or e/i/o/u)(digits)
    text = re.sub(r'([A-Za-z])([aeiouéèêëàâäùûüôöîï])(\d+)', fix_prefix_vowel, text, flags=re.IGNORECASE)
    
    return text


def _split_merged_booth_numbers(text: str) -> str:
    """Split merged booth numbers like '525624' into '525 624'.
    
    Common patterns:
    - 6 digits → 3+3 (e.g., 525624 → 525 624)
    - 5 digits → 3+2 (e.g., 52562 → 525 625) - rare, prefer 3+3
    - 8 digits → 4+4 (e.g., 23733623 → 2373 3623) - but more likely 3+3+...
    
    Strategy: Look for 6+ consecutive digits and split into groups of 3-4.
    """
    result = text
    
    # Pattern for 6+ consecutive digits (likely merged booth numbers)
    def split_digits(match):
        digits = match.group(0)
        n = len(digits)
        
        # For 6 digits, split as 3+3
        if n == 6:
            return f"{digits[:3]} {digits[3:]}"
        # For 7 digits, split as 3+4 or 4+3 (prefer 3+4)
        elif n == 7:
            return f"{digits[:3]} {digits[3:]}"
        # For 8 digits, split as 4+4
        elif n == 8:
            return f"{digits[:4]} {digits[4:]}"
        # For 9+ digits, split into groups of 3
        elif n >= 9:
            parts = [digits[i:i+3] for i in range(0, n, 3)]
            return ' '.join(parts)
        return digits
    
    # Apply splitting to sequences of 6+ digits
    result = re.sub(r'\d{6,}', split_digits, result)
    return result


def _infer_booth_prefixes(booth_numbers: List[str]) -> List[str]:
    """Infer missing prefixes in booth number sequences.
    
    OCR often confuses similar characters:
    - G ↔ 6 ↔ o (lowercase o)
    - O ↔ 0 (zero)
    - S ↔ 5 ↔ s
    
    This function analyzes a sequence of booth numbers and infers the correct
    prefix for numbers that appear to be missing it.
    
    Example:
        Input:  ['G5', 'G7', 'G9', '6', 'O8', 'O0']
        Output: ['G5', 'G7', 'G9', 'G6', 'G8', 'G10']
    
    Rules:
    - Only infer when dominant prefix appears in >50% of booths
    - Only infer for numbers without ANY prefix
    - Numbers must be within proximity of existing prefixed numbers
    """
    if not booth_numbers or len(booth_numbers) < 2:
        return booth_numbers
    
    # Parse booth numbers into (prefix, number, suffix) tuples
    parsed = []
    for booth in booth_numbers:
        m = re.match(r'^([A-Z])?(\d+)([A-Z])?$', booth.upper())
        if m:
            parsed.append((m.group(1) or '', int(m.group(2)), m.group(3) or ''))
        else:
            parsed.append(None)
    
    # Count prefix occurrences
    prefix_counts = {}
    for p in parsed:
        if p and p[0]:
            prefix_counts[p[0]] = prefix_counts.get(p[0], 0) + 1
    
    # Find dominant prefix
    dominant_prefix = None
    if prefix_counts:
        dominant_prefix = max(prefix_counts.keys(), key=lambda k: prefix_counts[k])
    
    if not dominant_prefix:
        return booth_numbers
    
    # Check if dominant prefix is truly dominant (>50% of booths with any prefix)
    total_with_prefix = sum(prefix_counts.values())
    if prefix_counts[dominant_prefix] / max(1, total_with_prefix) < 0.5:
        return booth_numbers
    
    # Also need dominant prefix to be in at least 2 booths
    if prefix_counts[dominant_prefix] < 2:
        return booth_numbers
    
    # Get the range of numbers with the dominant prefix
    numbers_with_prefix = [p[1] for p in parsed if p and p[0] == dominant_prefix]
    if not numbers_with_prefix:
        return booth_numbers
    
    min_num = min(numbers_with_prefix)
    max_num = max(numbers_with_prefix)
    
    # Apply corrections - only for booths without ANY prefix
    # AND the number is within the range of existing prefixed numbers
    result = []
    for i, (booth, p) in enumerate(zip(booth_numbers, parsed)):
        if p is None:
            result.append(booth)
            continue
        
        prefix, num, suffix = p
        
        # Only infer prefix for numbers that have NO prefix
        # AND the number is within or adjacent to the range of prefixed numbers
        if not prefix:
            # Check if this number is within or adjacent to the range
            if min_num - 5 <= num <= max_num + 5:
                result.append(f"{dominant_prefix}{num}{suffix}")
            else:
                result.append(booth)
            continue
        
        result.append(booth)
    
    return result


def _preprocess_for_ocr(crop: Image.Image) -> Image.Image:
    """Preprocess image for better OCR results.
    
    Steps:
    1. Convert to grayscale
    2. Enhance contrast
    3. Apply sharpening
    """
    from PIL import ImageEnhance, ImageFilter
    
    # Convert to grayscale
    gray = crop.convert('L')
    
    # Enhance contrast (helps with faded text)
    enhancer = ImageEnhance.Contrast(gray)
    enhanced = enhancer.enhance(2.0)
    
    # Sharpen to make text clearer
    sharpened = enhanced.filter(ImageFilter.SHARPEN)
    
    return sharpened


def _ocr_with_data(crop: Image.Image) -> List[OCRToken]:
    """OCR a crop using pytesseract.

    Returns list of OCRToken.
    Returns empty list if tesseract is not available or fails.
    """
    global _TESSERACT_AVAILABLE

    if not _TESSERACT_AVAILABLE:
        return []

    try:
        import pytesseract  # type: ignore
    except Exception:
        warnings.warn("pytesseract is not installed; OCR will be skipped.")
        _TESSERACT_AVAILABLE = False
        return []

    try:
        # Preprocess the image for better OCR
        preprocessed = _preprocess_for_ocr(crop)
        data = pytesseract.image_to_data(preprocessed, config="--psm 6", output_type=pytesseract.Output.DICT)
        out: List[OCRToken] = []
        for txt, conf in zip(data.get("text", []), data.get("conf", [])):
            txt = _normalize_text(str(txt))
            try:
                c = float(conf)
            except Exception:
                c = -1.0
            if txt:
                out.append(OCRToken(txt, c))
        return out
    except Exception as e:
        warnings.warn(f"OCR failed: {e}")
        return []


NOISE_TOKENS = {
    "AISLE", "AISLES", "ENTRY", "ENTRANCE", "EXIT", "FOOD", "COURT", "LOUNGE",
    "LOBBY", "STAGE", "THEATER", "THEATRE", "REGISTRATION", "RESTROOM", "RESTROOMS",
    "INFO", "INFORMATION", "MEETING", "ROOM", "HALL", "BALLROOM", "PAVILION",
    "PLAZA", "CAFE", "BAR", "OPEN", "SEATING", "GENERAL", "SESSION", "DESK",
}


def _clean_ocr_token(token: str) -> str:
    cleaned = _normalize_text(token).strip().strip(",;:/()[]{}")
    cleaned = cleaned.replace("|", " ")
    # Also handle mismatched parentheses (OCR artifacts)
    cleaned = cleaned.replace('(', ' ').replace(')', ' ').replace('[', ' ').replace(']', ' ')
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _is_noise_token(token: str) -> bool:
    upper = re.sub(r"[^A-Z]", "", token.upper())
    return bool(upper) and upper in NOISE_TOKENS


def _is_garbage_org_name(name: str) -> bool:
    """Detect if an org name is likely OCR garbage.
    
    Patterns that indicate garbage:
    - Single character with punctuation: 'q', 'e', 'm'
    - Random char combinations: 'N i a RN', 'q = P'
    - Mixed with special chars: '@7', '=TM'
    - Very short with symbols: 'e A', 'B E B E E'
    """
    if not name:
        return True
    
    # Remove common prefixes/suffixes that might be legitimate
    cleaned = name.strip()
    
    # Single character or empty after cleaning
    if len(cleaned) <= 1:
        return True
    
    # Count character types
    letters = sum(1 for c in cleaned if c.isalpha())
    digits = sum(1 for c in cleaned if c.isdigit())
    symbols = sum(1 for c in cleaned if not c.isalnum() and c != ' ')
    spaces = sum(1 for c in cleaned if c == ' ')
    
    # Too many symbols relative to letters
    if symbols > 0 and letters <= 2:
        return True
    
    # Mostly digits with few letters (likely misread booth number)
    if digits > letters:
        return True
    
    # Very short with special symbols
    if len(cleaned) <= 3 and symbols > 0:
        return True
    
    # Single letter repeated or alternating: 'A A A', 'B E B E'
    words = cleaned.split()
    if len(words) >= 3:
        unique_words = set(w.upper() for w in words if len(w) == 1)
        if len(unique_words) <= 2 and all(len(w) == 1 for w in words):
            return True
    
    # Contains @ symbol (OCR artifact from email or social)
    if '@' in cleaned:
        return True
    
    return False


def _clean_org_tokens(tokens: Sequence[str]) -> str:
    cleaned: List[str] = []
    for tok in tokens:
        tok = _clean_ocr_token(tok)
        if not tok or _is_noise_token(tok):
            continue
        if re.fullmatch(r"[-|/\\]+", tok):
            continue
        # Skip tokens that look like booth numbers
        upper = tok.upper()
        if re.match(r'^[A-Z]?\d{1,4}[A-Z]?$', upper):
            continue
        cleaned.append(tok)
    result = " ".join(cleaned).strip()
    
    # Apply garbage filter to final result
    if _is_garbage_org_name(result):
        return ""
    
    return result


def _extract_all_booth_numbers(text: str) -> List[str]:
    """Extract all booth number patterns from text, including OCR artifacts.
    
    Handles:
    - Normal patterns: G5, 123, A17, 456B
    - OCR artifacts: Gé6 → G6 (accented chars)
    - Merged numbers: 525624 → 525, 624
    - Mixed with text: "Booth G5 G7" → G5, G7
    - Parentheses artifacts: 2127(2226 → 2127, 2226
    - Pipe separators: 237|336 → 237, 336
    """
    # Normalize accents first
    normalized = _normalize_accents(text)
    # Fix OCR booth artifacts (Gé6 → G6)
    normalized = _fix_ocr_booth_artifacts(normalized)
    # Split merged digit sequences
    normalized = _split_merged_booth_numbers(normalized)
    # Handle parentheses and pipe artifacts - split on them
    normalized = normalized.replace('|', ' ').replace('(', ' ').replace(')', ' ').replace('[', ' ').replace(']', ' ')
    
    booth_pattern = r"[A-Za-z]?\d{1,4}[A-Za-z]?"
    matches = re.findall(booth_pattern, normalized)
    
    # Filter and normalize to uppercase
    valid_booths = []
    for m in matches:
        # Convert to uppercase and validate
        upper = m.upper()
        # Re-validate to filter false positives (like single letters)
        if re.match(r'^[A-Z]?\d{1,4}[A-Z]?$', upper):
            valid_booths.append(upper)
    
    return valid_booths


def split_booth_text(text: str) -> Tuple[str, str]:
    """Split OCR text into booth numbers and organization name.

    Booth number patterns recognized:
    - Single numbers: 123, 1234
    - Letter prefix: A123, G5, S100
    - Letter suffix: 123A, 456B
    - Multiple booths: G5 | G7 | G9, 715|812|713
    - Ranges: 123-130, A1-A10
    - OCR artifacts: Gé6 → G6
    - Merged numbers: 525624 → 525 624
    - Parentheses artifacts: 2127(2226 → 2127, 2226
    """
    if not text:
        return "", ""
    text = _normalize_text(text)
    
    # Normalize accents (é → e, etc.)
    text_normalized = _normalize_accents(text)
    
    # Fix OCR booth artifacts (Gé6 → G6)
    text_normalized = _fix_ocr_booth_artifacts(text_normalized)
    
    # Split merged booth numbers (525624 → 525 624)
    text_normalized = _split_merged_booth_numbers(text_normalized)
    
    # Handle parentheses and pipe artifacts - replace with space
    text_normalized = text_normalized.replace('|', ' ').replace('(', ' ').replace(')', ' ').replace('[', ' ').replace(']', ' ')
    # Normalize whitespace after artifact removal
    text_normalized = re.sub(r'\\s+', ' ', text_normalized).strip()

    booth_pattern = r"[A-Z]?\d{1,4}[A-Z]?"

    tokens = re.split(r"\s+", text_normalized)
    booth_tokens: List[str] = []
    org_tokens: List[str] = []
    booth_phase = True
    
    # Track positions of booth numbers for extracting org name
    booth_positions = set()

    for i, tok in enumerate(tokens):
        cleaned = _clean_ocr_token(tok).strip("|-")
        if not cleaned:
            continue
        if _is_noise_token(cleaned):
            booth_phase = False
            continue

        upper_tok = cleaned.upper()

        if booth_phase:
            parts = re.split(r'[|,\\/]', upper_tok)
            all_booths = True
            matched_parts: List[str] = []

            for part in parts:
                part = part.strip().strip("-")
                if not part:
                    continue
                if re.match(f"^{booth_pattern}$", part):
                    matched_parts.append(part)
                else:
                    all_booths = False
                    break

            if all_booths and matched_parts:
                booth_tokens.extend(matched_parts)
                booth_positions.add(i)
                continue

            if re.match(f"^{booth_pattern}$", upper_tok):
                booth_tokens.append(upper_tok)
                booth_positions.add(i)
                continue

            booth_phase = False

        org_tokens.append(cleaned)

    # Also scan entire text for booth numbers that might be mixed with org names
    # This catches cases like "Native Nations Federal User Showcase 33"
    all_booths = _extract_all_booth_numbers(text_normalized)
    for booth in all_booths:
        if booth not in booth_tokens:
            booth_tokens.append(booth)
    
    # Infer missing prefixes from OCR confusion (G/6/o, S/5/s, etc.)
    booth_tokens = _infer_booth_prefixes(booth_tokens)
    
    # Deduplicate while preserving order
    seen = set()
    unique_booths = []
    for b in booth_tokens:
        if b not in seen:
            seen.add(b)
            unique_booths.append(b)
    
    # Remove booth numbers from org tokens
    booth_set = set(unique_booths)
    filtered_org_tokens = [tok for tok in org_tokens if tok.upper() not in booth_set]
    
    # DEBUG
    import sys

    return " ".join(unique_booths).strip(), _clean_org_tokens(filtered_org_tokens)


def ocr_regions(img: Image.Image, regions: Iterable[Tuple[int, int, int, int]]) -> List[RegionOCR]:
    results: List[RegionOCR] = []
    for region in regions:
        crop = img.crop(region)
        tokens = _ocr_with_data(crop)
        text = " ".join(t.text for t in tokens)
        booth, org = split_booth_text(text)
        results.append(RegionOCR(booth, org, tokens, text))
    return results


def infer_dimensions(
    region: Tuple[int, int, int, int],
    calibration: Optional[Calibration] = None,
    typical_w: Optional[float] = None,
    typical_h: Optional[float] = None,
) -> Tuple[Optional[int], Optional[int]]:
    x1, y1, x2, y2 = region
    w_px = max(1, x2 - x1)
    h_px = max(1, y2 - y1)
    if w_px <= 0 or h_px <= 0:
        return None, None

    calibration = calibration or Calibration()
    square_w = calibration.square_width_px or typical_w or min(w_px, h_px)
    square_h = calibration.square_height_px or typical_h or min(w_px, h_px)
    square_w = max(1.0, float(square_w))
    square_h = max(1.0, float(square_h))

    width_units = max(1, round((w_px / square_w) * calibration.base_unit))
    height_units = max(1, round((h_px / square_h) * calibration.base_unit))
    return width_units, height_units


def _mean_ocr_confidence(tokens: Sequence[OCRToken]) -> float:
    vals = [t.confidence for t in tokens if t.confidence >= 0]
    if not vals:
        return 0.0
    return max(0.0, min(1.0, sum(vals) / (100.0 * len(vals))))


def _token_confidence_for(tokens: Sequence[OCRToken], predicate) -> float:
    vals = [t.confidence for t in tokens if predicate(t.text) and t.confidence >= 0]
    if not vals:
        return 0.0
    return max(0.0, min(1.0, sum(vals) / (100.0 * len(vals))))


def _looks_like_org_token(text: str) -> bool:
    cleaned = re.sub(r"[^A-Za-z0-9&+\-]", "", text)
    return bool(cleaned and re.search(r"[A-Za-z]", cleaned))


def _booth_sequence(tokens: Sequence[str]) -> Tuple[str, ...]:
    ordered: List[str] = []
    seen = set()
    for tok in tokens:
        tok = tok.strip().upper()
        if tok and tok not in seen:
            ordered.append(tok)
            seen.add(tok)
    return tuple(ordered)


def _parse_booth_token(token: str) -> Optional[Tuple[str, int, str]]:
    m = re.fullmatch(r"([A-Z]?)(\d{1,4})([A-Z]?)", token.strip().upper())
    if not m:
        return None
    return m.group(1), int(m.group(2)), m.group(3)


def _booth_pattern_score(booth_numbers: str) -> float:
    if not booth_numbers:
        return 0.0
    parts = [p for p in re.split(r"[\s|,;/]+", booth_numbers.upper()) if p]
    if not parts:
        return 0.0
    valid = sum(1 for p in parts if _parse_booth_token(p) is not None)
    base = valid / len(parts)
    if len(parts) > 1:
        parsed = [_parse_booth_token(p) for p in parts]
        if all(parsed):
            prefixes = {p[0] for p in parsed}
            suffixes = {p[2] for p in parsed}
            nums = sorted(p[1] for p in parsed)
            gaps = [b - a for a, b in zip(nums, nums[1:])]
            if len(prefixes) == 1 and len(suffixes) == 1 and gaps:
                if all(0 < g <= 2 for g in gaps):
                    base = min(1.0, base + 0.15)
                elif all(0 < g <= 5 for g in gaps):
                    base = min(1.0, base + 0.05)
    return min(1.0, base)


def _dimension_quality(region: Tuple[int, int, int, int], typical_w: float, typical_h: float) -> float:
    x1, y1, x2, y2 = region
    width = max(1.0, float(x2 - x1))
    height = max(1.0, float(y2 - y1))
    dw = abs(width - typical_w) / max(typical_w, 1.0)
    dh = abs(height - typical_h) / max(typical_h, 1.0)
    score = 1.0 - min(1.0, (dw + dh) / 2.0)
    aspect = max(width, height) / max(1.0, min(width, height))
    if aspect > 4.0:
        score *= 0.65
    elif aspect > 2.5:
        score *= 0.82
    return max(0.0, min(1.0, score))


def _confidence_label(score: float) -> str:
    if score >= CONFIDENCE_HIGH:
        return "high"
    if score >= CONFIDENCE_MEDIUM:
        return "medium"
    return "low"


def score_candidate(region: Tuple[int, int, int, int], ocr: RegionOCR, typical_w: float, typical_h: float) -> Tuple[float, str]:
    ocr_mean = _mean_ocr_confidence(ocr.tokens)
    booth_conf = _token_confidence_for(ocr.tokens, lambda t: _parse_booth_token(re.sub(r"[^A-Za-z0-9]", "", t.upper())) is not None)
    org_conf = _token_confidence_for(ocr.tokens, _looks_like_org_token)
    pattern_score = _booth_pattern_score(ocr.booth_numbers)
    dimension_score = _dimension_quality(region, typical_w, typical_h)

    # Adjusted weights: prioritize booth pattern and OCR confidence
    # Give more weight to booth pattern (most reliable indicator)
    score = (
        0.25 * ocr_mean +
        0.35 * max(booth_conf, pattern_score) +
        0.20 * org_conf +
        0.20 * dimension_score
    )

    # Strong bonus for clear booth number patterns
    if ocr.booth_numbers and pattern_score >= 0.95:
        score += 0.12
    elif ocr.booth_numbers and pattern_score >= 0.80:
        score += 0.08
    elif ocr.booth_numbers:
        score += 0.04
    
    # Bonus for having organization name
    if ocr.organisation_name and len(ocr.organisation_name) >= 4:
        score += 0.05
    
    # Bonus for having both booth numbers and org name (complete record)
    if ocr.booth_numbers and ocr.organisation_name and len(ocr.organisation_name) >= 4:
        score += 0.05
    
    # Penalties adjusted to be less harsh
    if not ocr.booth_numbers and not ocr.organisation_name:
        score *= 0.30
    elif not ocr.booth_numbers and ocr.organisation_name:
        score *= 0.75  # Less harsh penalty for org-only
    elif ocr.booth_numbers and not ocr.organisation_name:
        score *= 0.95  # Minimal penalty for booth-only

    score = max(0.0, min(1.0, score))
    return score, _confidence_label(score)


def _region_size_stats(regions: Sequence[Tuple[int, int, int, int]]) -> Tuple[float, float]:
    if not regions:
        return 1.0, 1.0
    widths = [max(1, x2 - x1) for x1, y1, x2, y2 in regions]
    heights = [max(1, y2 - y1) for x1, y1, x2, y2 in regions]
    return float(median(widths)), float(median(heights))


def _region_gap_threshold(regions: Sequence[Tuple[int, int, int, int]]) -> float:
    typical_w, typical_h = _region_size_stats(regions)
    return max(12.0, min(60.0, 0.35 * min(typical_w, typical_h)))


def _aligned_and_adjacent(a: Tuple[int, int, int, int], b: Tuple[int, int, int, int], gap_threshold: float) -> bool:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    a_w = max(1, ax2 - ax1)
    a_h = max(1, ay2 - ay1)
    b_w = max(1, bx2 - bx1)
    b_h = max(1, by2 - by1)
    avg_h = (a_h + b_h) / 2.0
    avg_w = (a_w + b_w) / 2.0

    vertical_overlap = max(0, min(ay2, by2) - max(ay1, by1))
    horizontal_overlap = max(0, min(ax2, bx2) - max(ax1, bx1))
    horizontal_gap = max(0, max(ax1, bx1) - min(ax2, bx2))
    vertical_gap = max(0, max(ay1, by1) - min(ay2, by2))

    same_row = vertical_overlap >= 0.5 * avg_h and horizontal_gap <= gap_threshold
    same_col = horizontal_overlap >= 0.5 * avg_w and vertical_gap <= gap_threshold
    return same_row or same_col


def _same_booth_family(a_numbers: Sequence[str], b_numbers: Sequence[str]) -> bool:
    a_parsed = [_parse_booth_token(n) for n in a_numbers]
    b_parsed = [_parse_booth_token(n) for n in b_numbers]
    if not a_parsed or not b_parsed or not all(a_parsed) or not all(b_parsed):
        return False
    a_prefixes = {p[0] for p in a_parsed}
    b_prefixes = {p[0] for p in b_parsed}
    a_suffixes = {p[2] for p in a_parsed}
    b_suffixes = {p[2] for p in b_parsed}
    if len(a_prefixes) != 1 or len(b_prefixes) != 1 or a_prefixes != b_prefixes:
        return False
    if len(a_suffixes) != 1 or len(b_suffixes) != 1 or a_suffixes != b_suffixes:
        return False
    a_nums = sorted(p[1] for p in a_parsed)
    b_nums = sorted(p[1] for p in b_parsed)
    min_gap = min(abs(x - y) for x in a_nums for y in b_nums)
    max_gap = max(abs(x - y) for x in a_nums for y in b_nums)
    return min_gap <= 2 and max_gap <= 6


def _combine_group_records(
    records: Sequence[CandidateRecord],
    typical_w: float,
    typical_h: float,
    calibration: Optional[Calibration] = None,
) -> CandidateRecord:
    regions = [r.region for r in records]
    x1 = min(r[0] for r in regions)
    y1 = min(r[1] for r in regions)
    x2 = max(r[2] for r in regions)
    y2 = max(r[3] for r in regions)
    region = (x1, y1, x2, y2)

    booth_numbers = _booth_sequence(n for rec in records for n in rec.ocr.booth_numbers.split())
    org_names = [rec.ocr.organisation_name for rec in records if rec.ocr.organisation_name]
    org_name = max(org_names, key=len) if org_names else ""
    tokens = [tok for rec in records for tok in rec.ocr.tokens]
    raw_text = " ".join(part for part in (rec.ocr.raw_text for rec in records) if part).strip()
    ocr = RegionOCR(" ".join(booth_numbers), org_name, tokens, raw_text)
    width, height = infer_dimensions(region, calibration=calibration, typical_w=typical_w, typical_h=typical_h)
    score, label = score_candidate(region, ocr, typical_w, typical_h)
    score = max(score, max(rec.confidence_score for rec in records))
    label = _confidence_label(score)
    return CandidateRecord(region, ocr, width, height, score, label)


def group_adjacent_booths(records: Sequence[CandidateRecord], calibration: Optional[Calibration] = None) -> List[CandidateRecord]:
    if not records:
        return []

    typical_w, typical_h = _region_size_stats([r.region for r in records])
    gap_threshold = _region_gap_threshold([r.region for r in records])
    used = set()
    grouped: List[CandidateRecord] = []

    for i, rec in enumerate(records):
        if i in used:
            continue
        base_numbers = [n for n in rec.ocr.booth_numbers.split() if n]
        if not base_numbers:
            grouped.append(rec)
            used.add(i)
            continue

        cluster = [i]
        used.add(i)
        changed = True
        while changed:
            changed = False
            for j, other in enumerate(records):
                if j in used:
                    continue
                other_numbers = [n for n in other.ocr.booth_numbers.split() if n]
                if not other_numbers:
                    continue
                if not any(
                    _same_booth_family(
                        [n for n in records[idx].ocr.booth_numbers.split() if n],
                        other_numbers,
                    ) and _aligned_and_adjacent(records[idx].region, other.region, gap_threshold)
                    for idx in cluster
                ):
                    continue
                cluster.append(j)
                used.add(j)
                changed = True

        if len(cluster) == 1:
            grouped.append(rec)
        else:
            if len(cluster) > 6:
                cluster = cluster[:6]
            grouped.append(_combine_group_records([records[idx] for idx in cluster], typical_w, typical_h, calibration=calibration))

    grouped.sort(key=lambda r: (r.region[1], r.region[0]))
    return grouped


def build_candidates(img: Image.Image, calibration: Optional[Calibration] = None) -> List[BoothCandidate]:
    regions = detect_grid_candidates(img)
    ocr_results = ocr_regions(img, regions) if regions else []
    typical_w, typical_h = _region_size_stats(regions)
    records: List[CandidateRecord] = []

    for idx, region in enumerate(regions):
        ocr = ocr_results[idx] if idx < len(ocr_results) else RegionOCR("", "", [], "")
        width, height = infer_dimensions(region, calibration=calibration, typical_w=typical_w, typical_h=typical_h)
        score, label = score_candidate(region, ocr, typical_w, typical_h)
        records.append(CandidateRecord(region, ocr, width, height, score, label))

    grouped = group_adjacent_booths(records, calibration=calibration)
    out: List[BoothCandidate] = []
    for rec in grouped:
        out.append(
            BoothCandidate(
                rec.ocr.booth_numbers,
                rec.ocr.organisation_name,
                rec.booth_width,
                rec.booth_height,
                rec.confidence,
            )
        )
    return out


def write_csv(path: str, booths: List[BoothCandidate]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Booth Number(s)", "Organisation Name", "Booth Width", "Booth Height", "Confidence"])
        for b in booths:
            w.writerow([b.booth_numbers, b.organisation_name, b.booth_width or "", b.booth_height or "", b.confidence])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image")
    parser.add_argument("--pdf")
    parser.add_argument("--url")
    parser.add_argument("--map-url", help="Interactive map page URL. Resolves a screenshot/static image asset first, then runs normal extraction.")
    parser.add_argument("--output", default="booths.csv")
    parser.add_argument("--max-dim", type=int, default=3000)
    parser.add_argument("--pdf-page", type=int, default=1)
    parser.add_argument("--base-unit", type=int, default=10, help="Base booth unit size to emit, default 10")
    parser.add_argument("--square-width-px", type=float, help="Manual calibration: pixel width of one base booth square")
    parser.add_argument("--square-height-px", type=float, help="Manual calibration: pixel height of one base booth square")
    args = parser.parse_args()

    if sum(bool(v) for v in (args.image, args.pdf, args.url, args.map_url)) != 1:
        raise SystemExit("Provide exactly one of --image, --pdf, --url, or --map-url")

    if args.pdf:
        if not os.path.exists(args.pdf):
            raise SystemExit(f"PDF not found: {args.pdf}")
        img = _load_pdf_as_image(args.pdf, page_number=args.pdf_page)
    elif args.map_url:
        img = _load_map_as_image(args.map_url, page_number=args.pdf_page)
    else:
        img = load_image(args.image, args.url)
    img = safe_resize(img, args.max_dim)
    calibration = Calibration(
        base_unit=max(1, args.base_unit),
        square_width_px=args.square_width_px,
        square_height_px=args.square_height_px or args.square_width_px,
    )
    booths = build_candidates(img, calibration=calibration)
    write_csv(args.output, booths)
    print(f"Wrote {len(booths)} rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
