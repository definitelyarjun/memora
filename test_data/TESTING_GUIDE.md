# FoundationIQ 3.0 — Gradio Test Cheat Sheet (SkillSphere India)

Use this file alongside the Gradio UI at http://localhost:7860

---

## TAB 1 — Module 1 · Startup Ingestion

Fill in the onboarding form with the following values (or POST `test_data/onboarding_payload.json`):

  Company Name:              SkillSphere India
  Startup Sub-type:          SaaS
  MRR — 3 months ago (INR): 1200000
  MRR — 2 months ago (INR): 1350000
  MRR — last month (INR):   1500000
  Growth goal (%):           15
  Months willing to wait:    3
  Tech Stack (comma-sep):    Razorpay, Zoho CRM, Google Workspace, Slack, Mailchimp

Upload CSVs:
  Org Chart:        test_data/org_chart.csv
  Expenses:         test_data/expenses.csv
  Sales Inquiries:  test_data/sales_inquiries.csv

Expected results:
  - MRR trend: Growing (12L → 13.5L → 15L, ~11.8% MoM)
  - Tech stack maturity: Moderate (no BI / data warehouse tooling)
  - DPDP risk on sales_inquiries.csv: CRITICAL (raw +91 phone numbers + personal emails)
  - Data quality score: ~85% (missing Payment_Date & Amount_INR on 3 rows)

---

## TAB 2 — Module 2 · Data Quality & DPDP Scanner

session_id:  <copy the session_id returned by Module 1>

Expected results:
  - sales_inquiries.csv: DPDP CRITICAL — Customer_Email and Customer_Phone contain raw PII
  - Completeness penalised: Payment_Date and Amount_INR missing on INQ003, INQ006, INQ009
  - INQ006 also missing Customer_Phone (additional completeness hit)
  - org_chart.csv: clean — no PII columns detected
  - expenses.csv: clean — no PII columns detected

---

## TAB 3 — Module 3 · Industry Benchmarking

session_id:  <copy the session_id returned by Module 1>

Product / Service name:  SkillSphere LMS (B2B SaaS)
Your price (INR):        4999
Currency:                INR
Features (comma-separated):
  AI personalisation, SCORM support, white-labelling, mobile app, analytics dashboard,
  SSO, Zoho CRM integration, Razorpay billing, multi-tenant, custom certificates
Category:                edtech

Expected results:
  - Competitiveness score relative to B2B EdTech/SaaS peers
  - Gemini strategy recommendation on pricing and positioning
  - Key insights on growth levers for ₹15L MRR → ₹17.25L target

---

## INTENTIONAL DATA ISSUES IN sales_inquiries.csv

Issue                              Rows
-----------------------------------------------------
PII — personal email addresses     All rows (Customer_Email)
PII — Indian mobile numbers (+91)  INQ001–INQ005, INQ007–INQ010
Missing Payment_Date               INQ003, INQ006, INQ009
Missing Amount_INR                 INQ003, INQ006, INQ009
Missing Customer_Phone             INQ006
Bottleneck (TAT > 48 hrs)         INQ002 (118h), INQ005 (98h), INQ008 (95h)

---

## INTENTIONAL DATA ISSUES IN org_chart.csv

Issue                              Notes
-----------------------------------------------------
High automation candidates         EMP006, EMP007 (SDR) — repetitive outreach
                                   EMP008, EMP009 (Support) — FAQ / ticket handling
                                   EMP010 (HR Admin) — payroll & leave management
Salary savings if automated:       ₹1,30,000/month (EMP006+EMP007+EMP010)
