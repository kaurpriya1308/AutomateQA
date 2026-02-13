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

    # Template patterns used to detect if URLs contain any template/regex patterns
    TEMPLATE_INDICATORS = [
        r'\$\{',          # Any template variable like ${miny=...}, ${epp=...}, etc.
        r'\{miny',        # MINY template
        r'\{epp',         # EPP template
        r'\{xpath',       # XPATH template
        r'\{onclick',     # ONCLICK template
        r'\{json[=_]',    # JSON template
        r'\{js_',         # JS template
        r'\{jsarg',       # JSARG template
        r'\{baseurl',     # BASEURL template
        r'\{window',      # Window flag template
        r'^cp:',          # CP regex pattern
        r'^ev:',          # EV regex pattern
        r'^df:',          # DF regex pattern
        r'^if:',          # IF regex pattern
        r'wd:',           # WD pattern
        r'curl:',         # Curl pattern
        r'json:xhr:',     # JSON XHR pattern
        r'json:curl:',    # JSON curl pattern
        r'appid',         # App ID pattern
    ]

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

        # Approach 3: Fix common issues like missing commas, trailing commas, single quotes
        try:
            fixed_json = cleaned_json

            # Fix missing commas between lines ending with " and next line starting with "
            fixed_json = re.sub(r'"\s*\n\s*"', '",\n"', fixed_json)

            # Replace single quotes with double quotes
            fixed_json = fixed_json.replace("'", '"')

            # Remove trailing commas before } or ]
            fixed_json = re.sub(r',\s*([}\]])', r'\1', fixed_json)

            return json.loads(fixed_json), None
        except json.JSONDecodeError as e:
            parse_errors.append(f"Fixed parse: {e.msg} at line {e.lineno}")

        return None, parse_errors

    @staticmethod
    def urls_contain_templates(urls):
        """Check if any URL in the list contains template/regex patterns"""
        for url in urls:
            if not isinstance(url, str):
                continue
            for pattern in URLAuditor.TEMPLATE_INDICATORS:
                if re.search(pattern, url, re.IGNORECASE):
                    return True
        return False

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
        xpath_pattern = (
            r"\$\{xpath=\:\<\{//.*\};\{\@.*\}\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};\{.*\};;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{.*\};;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};\{.*\};\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{.*\};\>\}.*xml"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{.\};;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};\{.*\};;;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};;;\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{.\};\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\}\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{\@.*\};\>\}"
            r"|\$\{xpath=\:\<\{//.*\};\{.\}\>\}"
            r"|\$\{xpath=\:\<\{//tr\};\{td\[4\]\};\{td\[2\]\};\{td\[1\]\}\>\}"
            r"|\$\{xpath=\:\<\{//.*\};{.*\};\{.*\};\{.*}.*\>\}"
            r"|\$\{xpath=\:\<\{//.*\};{.*\};\{.*\};;.*\>\}"
        )

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
    def check_json_template(urls):
        """Check JSON template"""
        issues = []
        json_pattern = (
            r"\$\{json=\:\<\{cp\:\:"
            r"|\$\{json=\:\<\".*\";\".*\";\".*\";\".*\"\>\}"
            r"|\$\{json=\:\<\".*\";\".*\"\>\}"
            r"|\$\{json=\:\<\".*\"; \".*\"\>\}"
            r"|\$\{json=\:\<\".*\";\".*\";\>\}"
            r"|\$\{json=\:\<\{tr\:\:"
            r"|\$\{json=\:\<\".*\";\".*\";\".*\";\>\}"
            r"|\$\{json=\:\<\".*\";\".*\";;\>\}"
            r"|\$\{json=\:\<\".*\";\".*\";;;\>\}"
            r"|GetFinancialReportListResult"
            r"|GetPresentationListResult"
            r"|GetEventListResult"
            r"|\$\{json=\:\<\".*\";\".*\";\".*\";\".*\";\".*\";\".*\"\|\>\}"
            r"|\$\{json=\:\<\".*\";\".*\";\".*\";\".*\";\".*\";\".*\"\|\".*;\".*\";.*\>\}"
        )
        mid_pattern = (
            r'json\:xhr\:|json\:uepost\:xhr\:|json\:jspost\:xhr\:'
            r'|json\:curl\:xhr\:|json\:curl\:|appid|json\:\$\{url\}|json\:xhr\:uepost\:'
        )

        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue

            has_json_eq = bool(re.search(r"\{json=", url))
            has_valid_json = bool(re.search(json_pattern, url))
            has_mid = bool(re.search(mid_pattern, url))

            has_json_data = bool(re.search(r"\{json_", url))
            has_valid_json_data = bool(
                re.search(r"\$\{json_data_load=\:1\}|\$\{json_data_load=\:True\}", url)
            )

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
                baseurl_pat = r'\$\{baseurl\=\:\"http'
                url_without_baseurl = re.sub(baseurl_pat, '', url, count=1, flags=re.IGNORECASE)
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
        url_map = {}

        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue

            url_clean = url.strip()

            if len(url_clean) <= 3:
                continue

            if url_clean.lower() in ['nan', 'none', 'null', 'n/a', '']:
                continue

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
        """
        Check metadata fields.
        Maps JSON fields to original CSV columns:
          - 'status' in JSON = 'Agent status' in CSV
          - 'case_type' in JSON = 'Case Type' in CSV
          - 'project' in JSON = 'Reason/Project' in CSV
          - 'research_status' in JSON = 'Researcher Status' in CSV
          - 'issue_area' in JSON = 'Issue Area' in CSV
          - 'final_status' in JSON = 'Final Status' in CSV
          - 'irsp_provider' in JSON = 'After save IRSP' in CSV
          - 'after_save_pageurls' in JSON = After save presentation urls in CSV
        """
        issues = []

        agent_status = str(data.get("status", "") or "").strip().lower()
        case_type = str(data.get("case_type", "") or "").strip().lower()
        project = str(data.get("project", "") or "").strip()
        research_status = str(data.get("research_status", "") or "").strip().lower()
        issue_area = str(data.get("issue_area", "") or "").strip()
        final_status_val = str(data.get("final_status", "") or "").strip()
        irsp_provider = str(data.get("irsp_provider", "") or "").strip()
        after_urls = data.get("after_save_pageurls", [])

        # --- CTAS check: Agent status is verified/manual/escalated but case_type is missing ---
        # Original: verified$|manual|escalated_to_technology_team on Agent status
        is_active_status = bool(re.search(
            r"verified$|manual|escalated_to_technology_team",
            agent_status
        ))

        if is_active_status and not case_type:
            issues.append({
                "type": "Metadata Error",
                "field": "case_type",
                "message": "No Case Type with Verified/Manual/Escalated Agent status"
            })

        # --- Cookie check: URLs contain curl: but case_type != cookie_case ---
        has_curl = any(
            "curl:" in str(url) for url in after_urls if isinstance(url, str)
        )
        if has_curl and case_type != "cookie_case":
            issues.append({
                "type": "Metadata Error",
                "field": "case_type",
                "message": "URLs contain 'curl:' but case_type is not 'cookie_case'"
            })

        # --- S3/Manual webpage check ---
        has_s3 = any(
            "s3.amazonaws.com" in str(url) for url in after_urls if isinstance(url, str)
        )
        if has_s3 and case_type != "manual_solution_webpage_generated":
            issues.append({
                "type": "Metadata Error",
                "field": "case_type",
                "message": "S3 URL found but case_type is not 'manual_solution_webpage_generated'"
            })

        # --- nfproject check: not_fixed status but project not QA ---
        if research_status == "not_fixed" and project != "QA":
            issues.append({
                "type": "Metadata Error",
                "field": "research_status / project",
                "message": "Not fixed status but project not QA"
            })

        # --- ncia check: Notes and Issue Area missing for non-covered case ---
        if (not issue_area
                and agent_status not in ["internal_review", "miscellaneous"]
                and project not in ["New Ticker", "QA"]):
            issues.append({
                "type": "Metadata Error",
                "field": "issue_area",
                "message": "Issue Area missing for non-covered case"
            })

        # --- ascheck: Case verified but no URL added / Q4Web with non-active status ---
        has_active_agent = bool(re.search(
            r"verified|manual|escalated|website_is_down|internal_review",
            agent_status
        ))

        if not after_urls and has_active_agent:
            issues.append({
                "type": "Metadata Error",
                "field": "after_save_pageurls",
                "message": "Case verified/escalated but no URL added"
            })

        if irsp_provider.lower() == "q4web" and not has_active_agent:
            issues.append({
                "type": "Metadata Error",
                "field": "irsp_provider",
                "message": "Q4Web added with non-active Agent Status"
            })

        # --- wdcheck: WD in URLs but case_type is direct ---
        wd_urls = [
            url for url in after_urls
            if isinstance(url, str) and bool(re.search(r"wd:", url))
        ]
        if wd_urls and case_type == "direct":
            issues.append({
                "type": "Metadata Error",
                "field": "case_type",
                "message": (
                    "WD added in URLs but case type marked as direct | URLs: "
                    + " || ".join(wd_urls[:3])
                )
            })

        # =====================================================
        # AMENDMENT 1: 'Direct' case_type should only be used
        # when there are NO templates in after_save_pageurls.
        # Error if 'direct' is used with templates present.
        # =====================================================
        if case_type == "direct" and after_urls:
            if URLAuditor.urls_contain_templates(after_urls):
                issues.append({
                    "type": "Metadata Error",
                    "field": "case_type",
                    "message": (
                        "'Direct' case_type used but templates/regex patterns "
                        "found in after_save_pageurls. 'Direct' should only be "
                        "used when there are no templates."
                    )
                })

        # =====================================================
        # AMENDMENT 2: issue_area and final_status must not be
        # blank. Raise error if either is empty.
        # Only check when agent status indicates work was done.
        # =====================================================
        if is_active_status:
            if not issue_area:
                issues.append({
                    "type": "Metadata Error",
                    "field": "issue_area",
                    "message": "Issue Area is blank. It must not be empty."
                })
            if not final_status_val:
                issues.append({
                    "type": "Metadata Error",
                    "field": "final_status",
                    "message": "Final Status is blank. It must not be empty."
                })

        # =====================================================
        # AMENDMENT 3: If there is 'cp:' in after_save_pageurls,
        # then irsp_provider value should be blank. If there is
        # no irsp_provider value, that is also okay.
        # =====================================================
        has_cp = any(
            isinstance(url, str) and url.strip().startswith("cp:")
            for url in after_urls
        )
        if has_cp and irsp_provider:
            issues.append({
                "type": "Metadata Error",
                "field": "irsp_provider",
                "message": (
                    f"'cp:' pattern found in after_save_pageurls but "
                    f"irsp_provider is '{irsp_provider}'. It should be blank "
                    f"when 'cp:' URLs are present."
                )
            })

        # =====================================================
        # AMENDMENT 4: If any of the first three URLs of
        # after_save_pageurls starts with 'text:', then
        # irsp_provider should be 'Q4Web'.
        # =====================================================
        first_three = after_urls[:3] if len(after_urls) >= 3 else after_urls
        has_text_prefix = any(
            isinstance(url, str) and url.strip().lower().startswith("text:")
            for url in first_three
        )
        if has_text_prefix and irsp_provider != "Q4Web":
            issues.append({
                "type": "Metadata Error",
                "field": "irsp_provider",
                "message": (
                    f"One of the first three after_save_pageurls starts with "
                    f"'text:' but irsp_provider is '{irsp_provider}'. "
                    f"It should be 'Q4Web'."
                )
            })

        return issues

    @classmethod
    def audit_urls(cls, data):
        """Run all audit checks"""
        urls = data.get("after_save_pageurls", [])

        issues = []

        # Template/URL checks (only if URLs exist)
        if urls:
            issues.extend(cls.check_miny(urls))
            issues.extend(cls.check_epp(urls))
            issues.extend(cls.check_xpath(urls))
            issues.extend(cls.check_onclick(urls))
            issues.extend(cls.check_jsarg(urls))
            issues.extend(cls.check_json_template(urls))
            issues.extend(cls.check_baseurl(urls))
            issues.extend(cls.check_windowflag(urls))
            issues.extend(cls.check_regex(urls))
            issues.extend(cls.check_http(urls))
            issues.extend(cls.check_brackets(urls))
            issues.extend(cls.check_duplicates(urls))

        # Metadata checks (always run)
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

        ### Key Field Mappings (JSON → CSV):
        | JSON Field | CSV Column |
        |---|---|
        | `status` | Agent status |
        | `case_type` | Case Type |
        | `project` | Reason/Project |
        | `issue_area` | Issue Area |
        | `final_status` | Final Status |
        | `irsp_provider` | After save IRSP |
        | `after_save_pageurls` | After save presentation URLs |
        | `research_status` | Researcher Status |

        ### Validation Rules:
        - **Direct case_type**: Only allowed when NO templates exist in after URLs
        - **Issue Area & Final Status**: Must NOT be blank for verified/manual/escalated cases
        - **CP URLs + IRSP**: If `cp:` in after URLs, `irsp_provider` must be blank
        - **Text URLs + IRSP**: If first 3 URLs start with `text:`, `irsp_provider` must be `Q4Web`
        """)

    # Initialize session state
    if 'audit_results' not in st.session_state:
        st.session_state.audit_results = None
    if 'audit_data' not in st.session_state:
        st.session_state.audit_data = None
    if 'clear_trigger' not in st.session_state:
        st.session_state.clear_trigger = False

    # JSON Input Section
    st.subheader("📝 JSON Input")

    # Set default value for text area
    default_value = "" if st.session_state.clear_trigger else st.session_state.get('last_input', '')

    json_input = st.text_area(
        "Paste your JSON data here:",
        height=300,
        placeholder='{\n  "id": "12345",\n  "as_company_id": "TICKER",\n  "status": "verified",\n  "after_save_pageurls": [...]\n}',
        value=default_value,
        key="json_text_area"
    )

    # Store current input
    if json_input and not st.session_state.clear_trigger:
        st.session_state.last_input = json_input

    # Reset clear trigger
    if st.session_state.clear_trigger:
        st.session_state.clear_trigger = False

    # Buttons
    col1, col2, col3, col4 = st.columns([2, 2, 2, 6])

    with col1:
        run_button = st.button("🚀 Run Audit", type="primary", use_container_width=True)

    with col2:
        clear_button = st.button("🗑️ Clear", use_container_width=True)

    with col3:
        if st.session_state.audit_results:
            st.download_button(
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
        st.session_state.last_input = ""
        st.session_state.clear_trigger = True
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
        ticker = data.get("ticker", data.get("as_company_id", "Unknown"))
        company_id = data.get("as_company_id", data.get("id", "Unknown"))
        agent_status = data.get("status", "N/A")

        st.header(f"📊 Audit Results for {ticker} ({company_id})")
        st.caption(f"Agent Status (from 'status' field): **{agent_status}**")

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

        # Quick info about key fields
        with st.expander("📋 Parsed Field Values", expanded=False):
            info_col1, info_col2 = st.columns(2)
            with info_col1:
                st.write(f"**Agent Status (status):** {data.get('status', 'N/A')}")
                st.write(f"**Case Type:** {data.get('case_type', 'N/A')}")
                st.write(f"**Project:** {data.get('project', 'N/A')}")
                st.write(f"**Issue Area:** {data.get('issue_area', 'N/A')}")
            with info_col2:
                st.write(f"**Final Status:** {data.get('final_status', 'N/A')}")
                st.write(f"**IRSP Provider:** {data.get('irsp_provider', 'N/A')}")
                st.write(f"**Research Status:** {data.get('research_status', 'N/A')}")
                st.write(f"**Verified (boolean):** {data.get('verified', 'N/A')}")

        # Results Display
        if results["issues_found"] == 0:
            st.markdown("""
                <div class="success-box">
                    <h3>✓ No Issues Found!</h3>
                    <p>All URLs passed validation. All templates, formats, and metadata are correct.</p>
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
            for issue_type, type_issues in issues_by_type.items():
                with st.expander(f"**{issue_type}** ({len(type_issues)} issues)", expanded=True):
                    for i, issue in enumerate(type_issues, 1):
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

                        if i < len(type_issues):
                            st.markdown("---")

            # Summary Table
            st.markdown("### 📋 Issue Summary")
            issue_summary = []
            for issue_type, type_issues in issues_by_type.items():
                issue_summary.append({
                    "Issue Type": issue_type,
                    "Count": len(type_issues)
                })

            st.table(issue_summary)

    # Footer
    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; color: #666; padding: 20px;'>
            <p>URL Audit Tool v2.0 | Built with Streamlit</p>
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
