#!/usr/bin/env python3
import os
from config import ClientConfig
from auditors.social_auditor import SocialMediaAuditor
from auditors.content_auditor import ContentAuditor
from analyzers.ai_analyzer import AIAnalyzer
from reports.pdf_generator import PDFReportGenerator

config = ClientConfig(
    client_name="Test Audit",
    client_industry="E-commerce",
    website_url="https://example.com",
    instagram_handle="@test",
    monthly_ad_budget=1000,
    team_size=2,
    primary_goal="Sales",
    target_audience="Adults",
    agency_name="Test Agency",
)

audit_data = {}
audit_data["social"] = SocialMediaAuditor(config).run()
audit_data["content"] = ContentAuditor(config, audit_data).run()
audit_data["ai_insights"] = AIAnalyzer().analyze(config, audit_data)

os.makedirs("reports", exist_ok=True)
PDFReportGenerator(config, audit_data).generate("reports/test.pdf")
print("SUCCESS! PDF created at reports/test.pdf")
