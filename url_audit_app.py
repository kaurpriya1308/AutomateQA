import streamlit as st
import json
import re
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="URL Audit Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for better text wrapping and styling
st.markdown("""
    <style>
    .main {
        padding: 2rem;
    }
    .stTextArea textarea {
        font-family: 'Courier New', monospace;
        font-size: 12px;
    }
    .url-text {
        word-wrap: break-word;
        word-break: break-all;
        white-space: pre-wrap;
        font-family: 'Courier New', monospace;
        font-size: 11px;
        background-color: #f0f2f6;
        padding: 8px;
        border-radius: 4px;
        margin: 5px 0;
    }
    .issue-box {
        border-left: 4px solid #ff4b4b;
        padding: 15px;
        margin: 10px 0;
        background-color: #fff5f5;
        border-radius: 4px;
    }
    .success-box {
        border-left: 4px solid #00c853;
        padding: 15px;
        margin: 10px 0;
        background-color: #f1f8f4;
        border-radius: 4px;
    }
    .summary-card {
        padding: 20px;
        border-radius: 8px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)


class URLAuditor:
    """URL Audit logic class"""
    
    @staticmethod
    def clean_json_input(json_text):
        """Clean JSON input by removing common issues"""
        json_text = json_text.strip()
        lines = json_text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = re.sub(r'^[\s\-]+', '', line)
            if line.strip():
                cleaned_lines.append(line)
        
        json_text = '\n'.join(cleaned_lines)
        
        if not json_text.startswith('{') and not json_text.startswith('['):
            match = re.search(r'[{\[]', json_text)
            if match:
                json_text = json_text[match.start():]
        
        if '{' in json_text:
            start = json_text.index('{')
            brace_count = 0
            end = start
            for i in range(start, len(json_text)):
                if json_text[i] == '{':
                    brace_count += 1
                elif json_text[i] == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end = i + 1
                        break
            json_text = json_text[start:end]
        
        return json_text

    @staticmethod
    def parse_json(json_text):
        """Parse JSON with multiple fallback methods"""
        cleaned_json = URLAuditor.clean_json_input(json_text)
        parse_errors = []
        
        # Approach 1: Direct parse
        try:
            return json.loads(cleaned_json), None
        except json.JSONDecodeError as e:
            parse_errors.append(f"Direct parse: {e.msg} at line {e.lineno}")
        
        # Approach 2: Non-strict parse
        try:
            return json.loads(cleaned_json, strict=False), None
        except json.JSONDecodeError as e:
            parse_errors.append(f"Non-strict parse: {e.msg} at line {e.lineno}")
        
        # Approach 3: Fix common issues
        try:
            fixed_json = cleaned_json.replace("'", '"')
            fixed_json = re.sub(r',\s*([}\]])', r'\1', fixed_json)
            return json.loads(fixed_json), None
        except json.JSONDecodeError as e:
            parse_errors.append(f"Fixed parse: {e.msg} at line {e.lineno}")
        
        return None, parse_errors

    @staticmethod
    def check_miny(urls):
        """Check MINY template"""
        issues = []
        miny_pattern = r"\$\{y\}|\$\{ym1\}|\$\{yp1\}|\$\{y2\}|\$\{ym2\}"
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_miny_bracket = bool(re.search(r"\{miny", url))
            has_miny_syntax = bool(re.search(r"\$\{miny=\:\d{4}\}", url))
            has_miny_template = bool(re.search(miny_pattern, url))
            
            if has_miny_bracket and (not has_miny_syntax or not has_miny_template):
                issues.append({
                    "type": "MINY Template Incorrect",
                    "url_index": idx,
                    "url": url
                })
        return issues

    @staticmethod
    def check_epp(urls):
        """Check EPP template"""
        issues = []
        epp_pattern = r"\$\{p\}|\$\{pm1\}|\$\{pp1\}|\$\{stm1\}|\$\{st\}"
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_epp_bracket = bool(re.search(r"\{epp", url))
            has_epp_syntax = bool(re.search(r"\$\{epp=\:\d{1,2}\}", url))
            has_epp_template = bool(re.search(epp_pattern, url))
            
            if has_epp_bracket and (not has_epp_syntax or not has_epp_template):
                issues.append({
                    "type": "EPP Template Incorrect",
                    "url_index": idx,
                    "url": url
                })
        return issues

    @staticmethod
    def check_xpath(urls):
        """Check XPATH template"""
        issues = []
        xpath_pattern = r"\$\{xpath=\:\<\{//.*\};\{\@.*\}\>\}|\$\{xpath=\:\<\{//.*\};\{\@.*\};\{.*\};;\>\}|\$\{xpath=\:\<\{//.*\};\{.*\};;\>\}|\$\{xpath=\:\<\{//.*\};\{\@.*\};\{.*\};\>\}|\$\{xpath=\:\<\{//.*\};\{.*\};\>\}.*xml|\$\{xpath=\:\<\{//.*\};\{\@.*\};;\>\}|\$\{xpath=\:\<\{//.*\};\{.\};;\>\}|\$\{xpath=\:\<\{//.*\};\{\@.*\};\{.*\};;;\>\}|\$\{xpath=\:\<\{//.*\};\{\@.*\};;;\>\}|\$\{xpath=\:\<\{//.*\};\{.\};\>\}|\$\{xpath=\:\<\{//.*\};\{\@.*\}\>\}|\$\{xpath=\:\<\{//.*\};\{\@.*\};\>\}|\$\{xpath=\:\<\{//.*\};\{.\}\>\}|\$\{xpath=\:\<\{//tr\};\{td\[4\]\};\{td\[2\]\};\{td\[1\]\}\>\}|\$\{xpath=\:\<\{//.*\};{.*\};\{.*\};\{.*}.*\>\}|\$\{xpath=\:\<\{//.*\};{.*\};\{.*\};;.*\>\}"
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_xpath = bool(re.search(r"\{xpath", url))
            has_valid_xpath = bool(re.search(xpath_pattern, url))
            
            if has_xpath and not has_valid_xpath:
                issues.append({
                    "type": "XPATH Template Incorrect",
                    "url_index": idx,
                    "url": url
                })
        return issues

    @staticmethod
    def check_onclick(urls):
        """Check ONCLICK template"""
        issues = []
        onclick_pattern = r'\$\{onclick_var=\:\".*\"\}'
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_onclick = bool(re.search(r"\{onclick", url))
            has_valid_onclick = bool(re.search(onclick_pattern, url))
            
            if has_onclick and not has_valid_onclick:
                issues.append({
                    "type": "ONCLICK Template Incorrect",
                    "url_index": idx,
                    "url": url
                })
        return issues

    @staticmethod
    def check_jsarg(urls):
        """Check JSARG template"""
        issues = []
        jsarg_pattern = r'\$\{jsarg=\:\d\}'
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_jsarg = bool(re.search(r"jsarg", url))
            has_valid_jsarg = bool(re.search(jsarg_pattern, url))
            
            if has_jsarg and not has_valid_jsarg:
                issues.append({
                    "type": "JSARG Template Incorrect",
                    "url_index": idx,
                    "url": url
                })
        return issues

    @staticmethod
    def check_json(urls):
        """Check JSON template"""
        issues = []
        json_pattern = r"\$\{json=\:\<\{cp\:\:|\$\{json=\:\<\".*\";\".*\";\".*\";\".*\"\>\}|\$\{json=\:\<\".*\";\".*\"\>\}|\$\{json=\:\<\".*\"; \".*\"\>\}|\$\{json=\:\<\".*\";\".*\";\>\}|\$\{json=\:\<\{tr\:\:|\$\{json=\:\<\".*\";\".*\";\".*\";\>\}|\$\{json=\:\<\".*\";\".*\";;\>\}|\$\{json=\:\<\".*\";\".*\";;;\>\}|GetFinancialReportListResult|GetPresentationListResult|GetEventListResult|\$\{json=\:\<\".*\";\".*\";\".*\";\".*\";\".*\";\".*\"\|\>\}|\$\{json=\:\<\".*\";\".*\";\".*\";\".*\";\".*\";\".*\"\|\".*;\".*\";.*\>\}"
        mid_pattern = r'json\:xhr\:|json\:uepost\:xhr\:|json\:jspost\:xhr\:|json\:curl\:xhr\:|json\:curl\:|appid|json\:\$\{url\}|json\:xhr\:uepost\:'
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
                
            has_json_eq = bool(re.search(r"\{json=", url))
            has_valid_json = bool(re.search(json_pattern, url))
            has_mid = bool(re.search(mid_pattern, url))
            
            has_json_data = bool(re.search(r"\{json_", url))
            has_valid_json_data = bool(re.search(r"\$\{json_data_load=\:1\}|\$\{json_data_load=\:True\}", url))
            
            has_js = bool(re.search(r"\{js_", url))
            has_valid_js = bool(re.search(r"\$\{js_json=\:1\}", url))
            
            if has_json_eq and (not has_valid_json or not has_mid):
                issues.append({
                    "type": "JSON Template Incorrect",
                    "url_index": idx,
                    "url": url
                })
            elif has_json_data and not has_valid_json_data:
                issues.append({
                    "type": "JSON Data Load Template Incorrect",
                    "url_index": idx,
                    "url": url
                })
            elif has_js and not has_valid_js:
                issues.append({
                    "type": "JS JSON Template Incorrect",
                    "url_index": idx,
                    "url": url
                })
        return issues

    @staticmethod
    def check_baseurl(urls):
        """Check BASEURL template"""
        issues = []
        baseurl_pattern = r"\$\{baseurl=\:\".*\"\}|\$\{full_baseurl=\:True\}"
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_baseurl = bool(re.search(r"\{baseurl", url))
            has_valid_baseurl = bool(re.search(baseurl_pattern, url))
            
            if has_baseurl and not has_valid_baseurl:
                issues.append({
                    "type": "BASEURL Template Incorrect",
                    "url_index": idx,
                    "url": url
                })
        return issues

    @staticmethod
    def check_windowflag(urls):
        """Check Window Flag template"""
        issues = []
        window_pattern = r"\$\{window_flag_regex=\:\".*\"\}|\$\{window_flag=\:True\}"
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_window = bool(re.search(r"\{window", url))
            has_valid_window = bool(re.search(window_pattern, url))
            
            if has_window and not has_valid_window:
                issues.append({
                    "type": "Window Flag Template Incorrect",
                    "url_index": idx,
                    "url": url
                })
        return issues

    @staticmethod
    def check_regex(urls):
        """Check Regex format"""
        issues = []
        regex_pattern = r"^ev|^df|^cp|^if"
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str) or len(url) < 4:
                continue
            
            has_regex = bool(re.search(regex_pattern, url))
            if has_regex:
                has_uppercase = bool(re.search(r"[A-Z]", url))
                has_escaped_uppercase = bool(re.search(r"\\[A-Z]|A\-Z", url))
                
                if len(url) >= 11 and has_uppercase and not has_escaped_uppercase:
                    issues.append({
                        "type": "Regex Incorrect - Uppercase not escaped",
                        "url_index": idx,
                        "url": url
                    })
                elif len(url) >= 11 and url[2] != ":":
                    issues.append({
                        "type": "Regex Incorrect - Missing colon at position 2",
                        "url_index": idx,
                        "url": url
                    })
        return issues

    @staticmethod
    def check_http(urls):
        """Check HTTP URL format"""
        issues = []
        keywords = [r'^df', r'^if', r'^ev', r'^cp']
        pattern = '|'.join(keywords)
    
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str) or len(url) <= 5:
                continue
            
            is_special = bool(re.search(pattern, url, re.IGNORECASE))
            if is_special:
                continue
            
            has_http = "http" in url.lower()
            has_multiple_http = bool(re.search(r"http.*http", url, re.IGNORECASE))
            
            # If multiple http found, check if first one is in baseurl template
            if has_multiple_http:
                # Pattern to match ${baseurl} with http inside it
                baseurl_pattern = r'\$\{baseurl\=\:\"http'
                
                # Remove the baseurl template and check if there's still multiple http
                url_without_baseurl = re.sub(baseurl_pattern, '', url, count=1, flags=re.IGNORECASE)
                
                # Re-check for multiple http after removing baseurl
                has_multiple_http = bool(re.search(r"http.*http", url_without_baseurl, re.IGNORECASE))
            
            has_newline = bool(re.search(r"\n", url))
            
            if not has_http:
                issues.append({
                    "type": "Missing HTTP/HTTPS",
                    "url_index": idx,
                    "url": url
                })
            elif has_multiple_http:
                issues.append({
                    "type": "Multiple HTTP in URL",
                    "url_index": idx,
                    "url": url
                })
            elif has_newline:
                issues.append({
                    "type": "Newline character in URL",
                    "url_index": idx,
                    "url": url
                })
        
        return issues

    @staticmethod
    def check_brackets(urls):
        """Check bracket matching"""
        issues = []
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            
            open_count = url.count("{")
            close_count = url.count("}")
            
            if open_count != close_count:
                issues.append({
                    "type": "Mismatched Brackets {}",
                    "url_index": idx,
                    "url": url,
                    "details": f"Open: {open_count}, Close: {close_count}"
                })
        return issues

    @staticmethod
    def check_duplicates(urls):
        """Check for duplicate URLs"""
        issues = []
        valid_urls = []
        url_map = {}
        
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            
            url_clean = url.strip()
            
            if len(url_clean) <= 3:
                continue
            
            if url_clean.lower() in ['nan', 'none', 'null', 'n/a', '']:
                continue
            
            valid_urls.append((idx, url_clean))
            
            if url_clean in url_map:
                url_map[url_clean].append(idx)
            else:
                url_map[url_clean] = [idx]
        
        for url, indices in url_map.items():
            if len(indices) > 1:
                issues.append({
                    "type": "Duplicate URL",
                    "url_indices": indices,
                    "url": url,
                    "occurrences": len(indices)
                })
        
        return issues

    @staticmethod
    def check_metadata(data):
        """Check metadata fields"""
        issues = []
        
        if data.get("verified") and not data.get("case_type"):
            issues.append({
                "type": "Metadata Error",
                "field": "case_type",
                "message": "Case is verified but case_type is missing"
            })
        
        has_curl = any("curl:" in str(url) for url in data.get("after_save_pageurls", []) if isinstance(url, str))
        if has_curl and data.get("case_type") != "cookie_case":
            issues.append({
                "type": "Metadata Error",
                "field": "case_type",
                "message": "URLs contain 'curl:' but case_type is not 'cookie_case'"
            })
        
        has_s3 = any("s3.amazonaws.com" in str(url) for url in data.get("after_save_pageurls", []) if isinstance(url, str))
        if has_s3 and data.get("case_type") != "manual_solution_webpage_generated":
            issues.append({
                "type": "Metadata Error",
                "field": "case_type",
                "message": "S3 URL found but case_type is not 'manual_solution_webpage_generated'"
            })
        
        return issues

    @classmethod
    def audit_urls(cls, data):
        """Run all audit checks"""
        urls = data.get("after_save_pageurls", [])
        
        if not urls:
            return {"status": "No URLs found", "total_urls": 0, "issues_found": 0, "issues": []}
        
        issues = []
        issues.extend(cls.check_miny(urls))
        issues.extend(cls.check_epp(urls))
        issues.extend(cls.check_xpath(urls))
        issues.extend(cls.check_onclick(urls))
        issues.extend(cls.check_jsarg(urls))
        issues.extend(cls.check_json(urls))
        issues.extend(cls.check_baseurl(urls))
        issues.extend(cls.check_windowflag(urls))
        issues.extend(cls.check_regex(urls))
        issues.extend(cls.check_http(urls))
        issues.extend(cls.check_brackets(urls))
        issues.extend(cls.check_duplicates(urls))
        issues.extend(cls.check_metadata(data))
        
        return {
            "status": "Complete",
            "total_urls": len(urls),
            "issues_found": len(issues),
            "issues": issues
        }


def display_url_wrapped(url, max_length=80):
    """Display URL with text wrapping"""
    return f'<div class="url-text">{url}</div>'

def main():
    # Header
    st.title("🔍 URL Audit Tool")
    st.markdown("---")
    
    # Instructions
    with st.expander("ℹ️ Instructions", expanded=False):
        st.markdown("""
        ### How to use this tool:
        1. **Paste your JSON data** in the text area below
        2. Click **Run Audit** button
        3. Review the results
        
        ### JSON Format Expected:
        ```json
        {
            "id": "12345",
            "as_company_id": "TICKER",
            "after_save_pageurls": ["url1", "url2", ...],
            "verified": true,
            "case_type": "cookie_case"
        }
        ```
        """)
    
    # Initialize session state
    if 'audit_results' not in st.session_state:
        st.session_state.audit_results = None
    if 'audit_data' not in st.session_state:
        st.session_state.audit_data = None
    if 'json_input' not in st.session_state:  # Add this
        st.session_state.json_input = ""
    
    # JSON Input Section
    st.subheader("📝 JSON Input")
    json_input = st.text_area(
        "Paste your JSON data here:",
        height=300,
        placeholder='{\n  "id": "12345",\n  "as_company_id": "TICKER",\n  "after_save_pageurls": [...]\n}',
        key="json_input",
        value=st.session_state.json_input  # Add this
    )
    
    # Buttons
    col1, col2, col3, col4 = st.columns([2, 2, 2, 6])
    
    with col1:
        run_button = st.button("🚀 Run Audit", type="primary", use_container_width=True)
    
    with col2:
        clear_button = st.button("🗑️ Clear", use_container_width=True)
    
    with col3:
        if st.session_state.audit_results:
            download_button = st.download_button(
                label="📥 Download Report",
                data=json.dumps(st.session_state.audit_results, indent=2),
                file_name=f"audit_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                use_container_width=True
            )
    
    # Clear functionality
    if clear_button:
        st.session_state.audit_results = None
        st.session_state.audit_data = None
        st.session_state.json_input = ""  # Clear the text area
        st.rerun()
    
    # Run Audit
    if run_button:
        if not json_input.strip():
            st.warning("⚠️ Please paste JSON data first!")
        else:
            with st.spinner("🔄 Processing audit..."):
                # Parse JSON
                data, parse_errors = URLAuditor.parse_json(json_input)
                
                if data is None:
                    st.error("❌ Failed to parse JSON")
                    with st.expander("View Parse Errors"):
                        for error in parse_errors:
                            st.text(error)
                    st.info("💡 Please ensure your JSON is properly formatted.")
                else:
                    # Validate required fields
                    if "after_save_pageurls" not in data:
                        st.error("❌ JSON must contain 'after_save_pageurls' field!")
                    else:
                        # Run audit
                        audit_results = URLAuditor.audit_urls(data)
                        st.session_state.audit_results = audit_results
                        st.session_state.audit_data = data
                        st.success("✅ Audit completed successfully!")
    
    # Display Results
    if st.session_state.audit_results:
        st.markdown("---")
        
        data = st.session_state.audit_data
        results = st.session_state.audit_results
        
        # Header Information
        ticker = data.get("as_company_id", "Unknown")
        company_id = data.get("id", "Unknown")
        
        st.header(f"📊 Audit Results for {ticker} (ID: {company_id})")
        
        # Summary Cards
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total URLs", results["total_urls"])
        
        with col2:
            st.metric("Issues Found", results["issues_found"])
        
        with col3:
            status = "✅ PASS" if results["issues_found"] == 0 else "❌ FAIL"
            st.metric("Status", status)
        
        st.markdown("---")
        
        # Results Display
        if results["issues_found"] == 0:
            st.markdown("""
                <div class="success-box">
                    <h3>✓ No Issues Found!</h3>
                    <p>All URLs passed validation. All templates and formats are correct.</p>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.subheader(f"⚠️ Issues Found: {results['issues_found']}")
            
            # Group issues by type
            issues_by_type = {}
            for issue in results["issues"]:
                issue_type = issue["type"]
                if issue_type not in issues_by_type:
                    issues_by_type[issue_type] = []
                issues_by_type[issue_type].append(issue)
            
            # Display issues grouped by type
            for issue_type, issues in issues_by_type.items():
                with st.expander(f"**{issue_type}** ({len(issues)} issues)", expanded=True):
                    for i, issue in enumerate(issues, 1):
                        st.markdown(f"**Issue #{i}:**")
                        
                        if 'url_index' in issue:
                            st.write(f"📍 **URL Index:** {issue['url_index']}")
                        elif 'url_indices' in issue:
                            st.write(f"📍 **URL Indices:** {issue['url_indices']}")
                            st.write(f"🔢 **Total Occurrences:** {issue['occurrences']}")
                        
                        if 'field' in issue:
                            st.write(f"🏷️ **Field:** `{issue['field']}`")
                            st.write(f"💬 **Message:** {issue['message']}")
                        
                        if 'url' in issue:
                            st.markdown("**🔗 URL:**")
                            st.markdown(display_url_wrapped(issue['url']), unsafe_allow_html=True)
                        
                        if 'details' in issue:
                            st.info(f"ℹ️ {issue['details']}")
                        
                        if i < len(issues):
                            st.markdown("---")
            
            # Summary Table
            st.markdown("### 📋 Issue Summary")
            issue_summary = []
            for issue_type, issues in issues_by_type.items():
                issue_summary.append({
                    "Issue Type": issue_type,
                    "Count": len(issues)
                })
            
            st.table(issue_summary)
    
    # Footer
    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; color: #666; padding: 20px;'>
            <p>URL Audit Tool v1.0 | Built with Streamlit</p>
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
