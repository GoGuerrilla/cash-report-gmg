
import json
import os
from datetime import datetime
from auditors.website_auditor import WebsiteAuditor
from auditors.social_auditor import SocialMediaAuditor
from auditors.seo_auditor import SEOAuditor
from auditors.content_auditor import ContentAuditor
from analyzers.ai_analyzer import AIAnalyzer
from reports.pdf_generator import PDFReportGenerator
from config import ClientConfig

def run_audit(config):
    print("MARKETING AUDIT: " + config.client_name)
    audit_data = {}
    if config.website_url:
        print("Auditing website...")
        audit_data["website"] = WebsiteAuditor(config.website_url).run()
    if config.website_url:
        print("Running SEO analysis...")
        audit_data["seo"] = SEOAuditor(config.website_url).run()
    print("Auditing social media...")
    audit_data["social"] = SocialMediaAuditor(config).run()
    print("Analyzing content...")
    audit_data["content"] = ContentAuditor(config, audit_data).run()
    print("Running AI analysis...")
    audit_data["ai_insights"] = AIAnalyzer(config.anthropic_api_key).analyze(config, audit_data)
    print("Generating PDF...")
    output_path = "reports/Marketing_Audit.pdf"
    os.makedirs("reports", exist_ok=True)
    PDFReportGenerator(config, audit_data).generate(output_path)
    print("Done! Report saved: " + output_path)
    return output_path

if __name__ == "__main__":
    config = ClientConfig(
        client_name="Acme Marketing Co",
        client_industry="E-commerce",
        website_url="https://example.com",
        facebook_page_url="https://www.facebook.com/example",
        instagram_handle="@example",
        linkedin_url="https://www.linkedin.com/company/example",
        monthly_ad_budget=2000,
        team_size=3,
        primary_goal="Lead generation",
        target_audience="Small business owners",
        top_competitors=["competitor1.com"],
        agency_name="Your Agency Name",
    )
    run_audit(config)



