"""Document ingestion service — text extraction from PDF, DOCX, and TXT files.

Supported formats:
  .txt   → decoded directly (UTF-8 with fallback to Latin-1)
  .docx  → python-docx paragraph extraction
  .pdf   → pdfplumber text + table extraction
             → scanned PDFs detected and flagged (OCR not supported)

Nothing is modified — raw text is extracted and returned as-is.
"""

from __future__ import annotations

import io
from pathlib import PurePosixPath

from app.core.config import settings
from app.schemas.document_ingestion import ExtractedTable


class DocumentIngestionError(Exception):
    """Raised when document validation or extraction fails."""


# ---------------------------------------------------------------------------
# File validation
# ---------------------------------------------------------------------------

def validate_document(filename: str, size_bytes: int) -> str:
    """Return the lowercased file extension if valid, else raise."""
    ext = PurePosixPath(filename).suffix.lower()
    if ext not in settings.allowed_document_extensions:
        raise DocumentIngestionError(
            f"Unsupported document type '{ext}'. "
            f"Allowed: {', '.join(settings.allowed_document_extensions)}"
        )
    max_bytes = settings.max_upload_size_mb * 1024 * 1024
    if size_bytes > max_bytes:
        raise DocumentIngestionError(
            f"File too large ({size_bytes / 1024 / 1024:.1f} MB). "
            f"Maximum: {settings.max_upload_size_mb} MB."
        )
    return ext


# ---------------------------------------------------------------------------
# TXT extraction
# ---------------------------------------------------------------------------

def extract_txt(content: bytes) -> str:
    """Decode plain text with UTF-8 fallback to Latin-1."""
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError:
        return content.decode("latin-1")


# ---------------------------------------------------------------------------
# DOCX extraction
# ---------------------------------------------------------------------------

def extract_docx(content: bytes) -> str:
    """Extract all paragraph text from a Word document."""
    try:
        from docx import Document  # python-docx
    except ImportError:
        raise DocumentIngestionError(
            "python-docx is not installed. Run: pip install python-docx"
        )

    buf = io.BytesIO(content)
    doc = Document(buf)
    paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
    return "\n".join(paragraphs)


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------

def extract_pdf(
    content: bytes,
) -> tuple[str, list[ExtractedTable], int, bool]:
    """Extract text and tables from a PDF.

    Returns:
        (full_text, extracted_tables, page_count, is_scanned)

    is_scanned is True when no extractable text is found on any page,
    meaning the PDF is likely a scanned image.
    OCR is NOT performed — the document is flagged and returned as-is.
    """
    try:
        import pdfplumber
    except ImportError:
        raise DocumentIngestionError(
            "pdfplumber is not installed. Run: pip install pdfplumber"
        )

    buf = io.BytesIO(content)
    text_parts: list[str] = []
    tables: list[ExtractedTable] = []

    with pdfplumber.open(buf) as pdf:
        page_count = len(pdf.pages)
        for page_num, page in enumerate(pdf.pages, start=1):
            # Text extraction
            page_text = page.extract_text() or ""
            if page_text.strip():
                text_parts.append(page_text)

            # Table extraction
            for raw_table in page.extract_tables() or []:
                if not raw_table:
                    continue
                headers = [str(cell or "") for cell in raw_table[0]]
                data_rows = raw_table[1:]
                preview = [
                    [str(cell or "") for cell in row]
                    for row in data_rows[:3]
                ]
                tables.append(ExtractedTable(
                    page=page_num,
                    row_count=len(data_rows),
                    column_count=len(headers),
                    headers=headers,
                    preview_rows=preview,
                ))

    full_text = "\n\n".join(text_parts).strip()
    is_scanned = not full_text  # no extractable text = image-only PDF

    return full_text, tables, page_count, is_scanned


# ---------------------------------------------------------------------------
# High-level orchestrator
# ---------------------------------------------------------------------------

def process_document(
    file_content: bytes,
    filename: str,
) -> tuple[str, list[ExtractedTable], int | None, bool, list[str]]:
    """Extract content from a document file.

    Returns:
        (extracted_text, tables, page_count, is_scanned, warnings)

    Tables and page_count are only populated for PDFs.
    is_scanned is True if the PDF has no machine-readable text.
    """
    warnings: list[str] = []
    ext = validate_document(filename, len(file_content))

    if ext == ".txt":
        text = extract_txt(file_content)
        return text, [], None, False, warnings

    if ext == ".docx":
        text = extract_docx(file_content)
        if not text.strip():
            warnings.append("No text content found in the Word document.")
        return text, [], None, False, warnings

    if ext == ".pdf":
        text, tables, page_count, is_scanned = extract_pdf(file_content)
        if is_scanned:
            warnings.append(
                "This PDF appears to be a scanned or image-only document. "
                "Automatic text extraction is not possible. "
                "Please upload a text-selectable PDF, or convert to .docx / .txt."
            )
        if tables:
            warnings.append(
                f"Found {len(tables)} table(s) across {page_count} page(s). "
                "Tabular data within PDFs is extracted as preview only — "
                "for full analysis upload as CSV or Excel."
            )
        return text, tables, page_count, is_scanned, warnings

    raise DocumentIngestionError(f"Unhandled extension: {ext}")


# ---------------------------------------------------------------------------
# PDF → image rendering (for Gemini multimodal)
# ---------------------------------------------------------------------------

def render_pdf_pages(content: bytes, dpi: int = 150) -> list[bytes]:
    """Render each PDF page to a PNG image using pymupdf (fitz).

    Args:
        content: Raw PDF bytes.
        dpi: Render resolution. 150 dpi is sufficient for Gemini; higher = more tokens.

    Returns:
        List of PNG image bytes, one per page.

    Raises:
        DocumentIngestionError if pymupdf is not installed.
    """
    try:
        import fitz  # pymupdf
    except ImportError:
        raise DocumentIngestionError(
            "pymupdf is not installed. Run: pip install pymupdf"
        )

    doc = fitz.open(stream=content, filetype="pdf")
    scale = dpi / 72.0  # pymupdf default is 72 dpi
    mat = fitz.Matrix(scale, scale)

    images: list[bytes] = []
    for page in doc:
        pix = page.get_pixmap(matrix=mat)
        images.append(pix.tobytes("png"))

    doc.close()
    return images
