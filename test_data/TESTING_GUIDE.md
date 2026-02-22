# FoundationIQ — Gradio Test Cheat Sheet

Use this file alongside the Gradio UI at http://localhost:7860

---

## TAB 1 — Module 1a · Tabular Ingestion

Upload file:      test_data/sales_data.csv

Workflow description (paste the whole block):
"""
Every morning the kitchen manager checks stock manually and writes it in a paper logbook.
The owner then calls each supplier individually over WhatsApp to place orders verbally.
Reservations are taken by phone and recorded in a paper diary — double-bookings happen often.
Waiters take orders on handwritten notepads and walk them to the kitchen board.
The cashier manually calculates bills with a calculator and writes on blank receipt templates.
At closing, the owner manually enters the day's sales totals into an Excel spreadsheet.
At month-end, paper invoices are handed to an external accountant for reconciliation in Tally.
"""

Industry:         Restaurant
Employees:        18
Tools (comma-separated):
  Excel, WhatsApp, Google Pay, PhonePe, Tally

---

## TAB 2 — Module 1b · Document Ingestion

Upload file:      test_data/restaurant_sop.txt
Document type:    sop

---

## TAB 3 — Module 2 · Data Quality

session_id:       <copy the session_id returned by Module 1a>

Expected results:
  - Completeness will be penalised (missing Customer Name, unit_price, quantity, payment_method)
  - Deduplication will be penalised (rows 1001 & 1011 are identical; 1002 & 1012 are identical)
  - Consistency will be penalised (mixed column name styles: "Customer Name", "Product Name" vs lowercase)
  - Structural Integrity penalised (row 1020 has "January 14 2024" — unparsed date format)
  - Overall AI readiness: expect Moderate or Low

---

## TAB 4 — Module 3 · Industry Benchmarking

session_id:       <copy the session_id returned by Module 1a>

Product / Service name:   Spice Garden Dine-In Experience
Your price (INR):         280
Currency:                 INR
Features (comma-separated):
  dine-in, delivery, online ordering, GST billing, parking, AC seating, veg menu, non-veg menu
Category:                 restaurant

Expected results:
  - Market position compared to 11 restaurant competitors in dataset
  - Gemini strategy recommendation on pricing and differentiation
  - Feature match score based on keyword overlap

---

## INTENTIONAL DATA ISSUES IN sales_data.csv

Issue                           Rows / Columns
-----------------------------------------------------
Missing Customer Name           1006, 1014, 1022
Missing unit_price              1007
Missing total_amount            1015
Missing quantity                1019
Missing payment_method          1009, 1022
Duplicate rows                  1011 = 1001 · 1012 = 1002
Whitespace in Customer Name     1005 " Deepak Verma ", 1016 " Ravi Tiwari"
Unparsed date                   1020 "January 14 2024" (not ISO format)
Inconsistent column names       "Customer Name", "Product Name", "Order Date" (title case vs lowercase)
