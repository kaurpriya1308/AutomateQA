import streamlit as st
import json
import re
from datetime import datetime

st.set_page_config(
    page_title="URL Audit Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
    <style>
    .main { padding: 2rem; }
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
    .success-box {
        border-left: 4px solid #00c853;
        padding: 15px;
        margin: 10px 0;
        background-color: #f1f8f4;
        border-radius: 4px;
    }
    </style>
""", unsafe_allow_html=True)


class URLAuditor:

    # These are the ACTUAL template keywords that mean "not direct"
    # Only these should trigger the "Direct but templates found" error
    TEMPLATE_KEYWORDS = [
        r'json:',
        r'baseurl',
        r'window_flag',
        r'wd:',
        r'xpath=',
        r'jsarg=',
        r'split_text',
        r'split_flag',
        r'curl:',
        r'append_text',
        r'clean_links',
        r'mark_text_null',
        r'hdrs',
        r'\{miny',
        r'\{epp',
        r'\{onclick',
        r'\{json=',
        r'\{json_',
        r'\{js_',
    ]

    @staticmethod
    def clean_json_input(text):
        text = text.strip()
        lines = [re.sub(r'^[\s\-]+', '', l) for l in text.split('\n') if l.strip()]
        text = '\n'.join(lines)
        if not text.startswith('{') and not text.startswith('['):
            m = re.search(r'[{\[]', text)
            if m:
                text = text[m.start():]
        if '{' in text:
            s = text.index('{')
            bc = 0
            e = s
            for i in range(s, len(text)):
                if text[i] == '{':
                    bc += 1
                elif text[i] == '}':
                    bc -= 1
                    if bc == 0:
                        e = i + 1
                        break
            text = text[s:e]
        return text

    @staticmethod
    def parse_json(text):
        c = URLAuditor.clean_json_input(text)
        errs = []
        for name, fn in [
            ("Direct", lambda t: json.loads(t)),
            ("Non-strict", lambda t: json.loads(t, strict=False)),
            ("Fixed", lambda t: json.loads(
                re.sub(r',\s*([}\]])', r'\1',
                        re.sub(r'"\s*\n\s*"', '",\n"', t).replace("'", '"'))
            )),
        ]:
            try:
                return fn(c), None
            except json.JSONDecodeError as e:
                errs.append(f"{name}: {e.msg} at line {e.lineno}")
        return None, errs

    @staticmethod
    def urls_contain_templates(urls):
        """Check if any URL contains actual template keywords."""
        for u in urls:
            if not isinstance(u, str):
                continue
            for kw in URLAuditor.TEMPLATE_KEYWORDS:
                if re.search(kw, u, re.IGNORECASE):
                    return True
        return False

    @staticmethod
    def check_miny(urls):
        issues = []
        pat = r"\$\{y\}|\$\{ym1\}|\$\{yp1\}|\$\{y2\}|\$\{ym2\}"
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"\{miny", u):
                if not re.search(r"\$\{miny=\:\d{4}\}", u) or not re.search(pat, u):
                    issues.append({"type": "MINY Template Incorrect", "url_index": i, "url": u})
        return issues

    @staticmethod
    def check_epp(urls):
        issues = []
        pat = r"\$\{p\}|\$\{pm1\}|\$\{pp1\}|\$\{stm1\}|\$\{st\}"
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"\{epp", u):
                if not re.search(r"\$\{epp=\:\d{1,2}\}", u) or not re.search(pat, u):
                    issues.append({"type": "EPP Template Incorrect", "url_index": i, "url": u})
        return issues

    @staticmethod
    def check_xpath(urls):
        issues = []
        pat = (
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
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"\{xpath", u) and not re.search(pat, u):
                issues.append({"type": "XPATH Template Incorrect", "url_index": i, "url": u})
        return issues

    @staticmethod
    def check_onclick(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"\{onclick", u) and not re.search(r'\$\{onclick_var=\:\".*\"\}', u):
                issues.append({"type": "ONCLICK Template Incorrect", "url_index": i, "url": u})
        return issues

    @staticmethod
    def check_jsarg(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"jsarg", u) and not re.search(r'\$\{jsarg=\:\d\}', u):
                issues.append({"type": "JSARG Template Incorrect", "url_index": i, "url": u})
        return issues

    @staticmethod
    def check_json_template(urls):
        issues = []
        jp = (
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
        mp = (
            r'json\:xhr\:|json\:uepost\:xhr\:|json\:jspost\:xhr\:'
            r'|json\:curl\:xhr\:|json\:curl\:|appid'
            r'|json\:\$\{url\}|json\:xhr\:uepost\:'
        )
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"\{json=", u):
                if not re.search(jp, u) or not re.search(mp, u):
                    issues.append({"type": "JSON Template Incorrect", "url_index": i, "url": u})
            elif re.search(r"\{json_", u):
                if not re.search(r"\$\{json_data_load=\:1\}|\$\{json_data_load=\:True\}", u):
                    issues.append({"type": "JSON Data Load Incorrect", "url_index": i, "url": u})
            elif re.search(r"\{js_", u):
                if not re.search(r"\$\{js_json=\:1\}", u):
                    issues.append({"type": "JS JSON Incorrect", "url_index": i, "url": u})
        return issues

    @staticmethod
    def check_baseurl(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"\{baseurl", u):
                if not re.search(r"\$\{baseurl=\:\".*\"\}|\$\{full_baseurl=\:True\}", u):
                    issues.append({"type": "BASEURL Template Incorrect", "url_index": i, "url": u})
        return issues

    @staticmethod
    def check_windowflag(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"\{window", u):
                if not re.search(r"\$\{window_flag_regex=\:\".*\"\}|\$\{window_flag=\:True\}", u):
                    issues.append({"type": "Window Flag Incorrect", "url_index": i, "url": u})
        return issues

    @staticmethod
    def check_regex(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str) or len(u) < 4:
                continue
            if re.search(r"^ev|^df|^cp|^if", u):
                has_up = bool(re.search(r"[A-Z]", u))
                has_esc = bool(re.search(r"\\[A-Z]|A\-Z", u))
                if len(u) >= 11 and has_up and not has_esc:
                    issues.append({"type": "Regex - Uppercase not escaped", "url_index": i, "url": u})
                elif len(u) >= 11 and u[2] != ":":
                    issues.append({"type": "Regex - Missing colon", "url_index": i, "url": u})
        return issues

    @staticmethod
    def check_http(urls):
        issues = []
        skip = '|'.join([r'^df', r'^if', r'^ev', r'^cp'])
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str) or len(u) <= 5:
                continue
            if re.search(skip, u, re.IGNORECASE):
                continue
            has_http = "http" in u.lower()
            has_multi = bool(re.search(r"http.*http", u, re.IGNORECASE))
            if has_multi:
                cleaned = re.sub(r'\$\{baseurl\=\:\"http', '', u, count=1, flags=re.IGNORECASE)
                has_multi = bool(re.search(r"http.*http", cleaned, re.IGNORECASE))
            if not has_http:
                issues.append({"type": "Missing HTTP/HTTPS", "url_index": i, "url": u})
            elif has_multi:
                issues.append({"type": "Multiple HTTP in URL", "url_index": i, "url": u})
            elif re.search(r"\n", u):
                issues.append({"type": "Newline in URL", "url_index": i, "url": u})
        return issues

    @staticmethod
    def check_brackets(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if u.count("{") != u.count("}"):
                issues.append({
                    "type": "Mismatched Brackets", "url_index": i, "url": u,
                    "details": f"Open: {u.count('{')}, Close: {u.count('}')}"
                })
        return issues

    @staticmethod
    def check_duplicates(urls):
        issues = []
        m = {}
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            c = u.strip()
            if len(c) <= 3 or c.lower() in ('nan', 'none', 'null', 'n/a', ''):
                continue
            m.setdefault(c, []).append(i)
        for u, idx in m.items():
            if len(idx) > 1:
                issues.append({
                    "type": "Duplicate URL", "url_indices": idx,
                    "url": u, "occurrences": len(idx)
                })
        return issues

    @staticmethod
    def check_metadata(data):
        issues = []
        agent = str(data.get("status", "") or "").strip().lower()
        ct = str(data.get("case_type", "") or "").strip().lower()
        proj = str(data.get("project", "") or "").strip()
        rs = str(data.get("research_status", "") or "").strip().lower()
        ia = str(data.get("issue_area", "") or "").strip()
        fs = str(data.get("final_status", "") or "").strip()
        irsp = str(data.get("irsp_provider", "") or "").strip()
        aurls = data.get("after_save_pageurls", [])

        is_active = bool(re.search(
            r"verified$|manual|escalated_to_technology_team", agent
        ))
        has_active = bool(re.search(
            r"verified|manual|escalated|website_is_down|internal_review", agent
        ))

        if is_active and not ct:
            issues.append({"type": "Metadata Error", "field": "case_type",
                           "message": "No Case Type with active Agent status"})

        if any("curl:" in str(u) for u in aurls if isinstance(u, str)):
            if ct != "cookie_case":
                issues.append({"type": "Metadata Error", "field": "case_type",
                               "message": "curl: found but case_type not cookie_case"})

        if any("s3.amazonaws.com" in str(u) for u in aurls if isinstance(u, str)):
            if ct != "manual_solution_webpage_generated":
                issues.append({"type": "Metadata Error", "field": "case_type",
                               "message": "S3 URL but case_type not manual_solution_webpage_generated"})

        if rs == "not_fixed" and proj != "QA":
            issues.append({"type": "Metadata Error", "field": "research_status",
                           "message": "not_fixed but project not QA"})

        if (not ia and agent not in ["internal_review", "miscellaneous"]
                and proj not in ["New Ticker", "QA"]):
            issues.append({"type": "Metadata Error", "field": "issue_area",
                           "message": "Issue Area missing"})

        if not aurls and has_active:
            issues.append({"type": "Metadata Error", "field": "after_save_pageurls",
                           "message": "Active status but no URLs"})

        if irsp.lower() == "q4web" and not has_active:
            issues.append({"type": "Metadata Error", "field": "irsp_provider",
                           "message": "Q4Web with non-active status"})

        wd = [u for u in aurls if isinstance(u, str) and re.search(r"wd:", u)]
        if wd and ct == "direct":
            issues.append({"type": "Metadata Error", "field": "case_type",
                           "message": "WD in URLs but case_type=direct"})

        # Direct case_type check — only flag if actual template keywords found
        # NOT ev:/cp:/df:/if: regex patterns — those are fine with direct
        if ct == "direct" and aurls and URLAuditor.urls_contain_templates(aurls):
            # Find which template keywords were found for the error message
            found_keywords = []
            for u in aurls:
                if not isinstance(u, str):
                    continue
                for kw in URLAuditor.TEMPLATE_KEYWORDS:
                    if re.search(kw, u, re.IGNORECASE):
                        # Clean up regex escapes for display
                        display_kw = kw.replace(r'\{', '{').replace(r'\:', ':')
                        if display_kw not in found_keywords:
                            found_keywords.append(display_kw)
            kw_list = ", ".join(found_keywords[:5])
            issues.append({
                "type": "Metadata Error", "field": "case_type",
                "message": f"Direct case_type but templates found in URLs. "
                           f"Templates detected: {kw_list}"
            })

        if is_active:
            if not ia:
                issues.append({"type": "Metadata Error", "field": "issue_area",
                               "message": "Issue Area blank"})
            if not fs:
                issues.append({"type": "Metadata Error", "field": "final_status",
                               "message": "Final Status blank"})

        has_cp = any(
            isinstance(u, str) and u.strip().startswith("cp:") for u in aurls
        )
        if has_cp and irsp:
            issues.append({"type": "Metadata Error", "field": "irsp_provider",
                           "message": f"cp: in URLs but irsp_provider='{irsp}'"})

        f3 = aurls[:3] if len(aurls) >= 3 else aurls
        has_text = any(
            isinstance(u, str) and u.strip().lower().startswith("text:")
            for u in f3
        )
        if has_text and irsp != "Q4Web":
            issues.append({"type": "Metadata Error", "field": "irsp_provider",
                           "message": f"text: in first 3 URLs but irsp_provider='{irsp}'"})

        return issues

    @classmethod
    def audit_urls(cls, data):
        urls = data.get("after_save_pageurls", [])
        issues = []
        if urls:
            for fn in [cls.check_miny, cls.check_epp, cls.check_xpath,
                       cls.check_onclick, cls.check_jsarg, cls.check_json_template,
                       cls.check_baseurl, cls.check_windowflag, cls.check_regex,
                       cls.check_http, cls.check_brackets, cls.check_duplicates]:
                issues.extend(fn(urls))
        issues.extend(cls.check_metadata(data))
        return {"status": "Complete", "total_urls": len(urls),
                "issues_found": len(issues), "issues": issues}


def display_url_wrapped(url):
    return f'<div class="url-text">{url}</div>'


def main():
    st.title("🔍 URL Audit Tool")
    st.markdown("---")

    with st.expander("ℹ️ Instructions", expanded=False):
        st.markdown("""
        ### How to use:
        1. **Paste JSON** in the text area
        2. Click **Run Audit**
        3. Review results

        ### Field Mappings (JSON → CSV):
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

        ### Template Keywords (triggers "not direct"):
        `json:`, `baseurl`, `window_flag`, `wd:`, `xpath=`, `jsarg=`,
        `split_text`, `split_flag`, `curl:`, `append_text`, `clean_links`,
        `mark_text_null`, `hdrs`, `{miny`, `{epp`, `{onclick`, `{json=`, `{json_`, `{js_`

        **Note:** `ev:`, `cp:`, `df:`, `if:` regex patterns are NOT templates
        and are allowed with `direct` case_type.
        """)

    # Session state
    if 'audit_result_data' not in st.session_state:
        st.session_state.audit_result_data = None
    if 'audit_json_data' not in st.session_state:
        st.session_state.audit_json_data = None
    if 'clear_trigger' not in st.session_state:
        st.session_state.clear_trigger = False

    # JSON Input
    st.subheader("📝 JSON Input")

    # Handle clear: use callback to reset
    if st.session_state.clear_trigger:
        default_val = ""
        st.session_state.clear_trigger = False
    else:
        default_val = st.session_state.get('last_input', '')

    json_input = st.text_area(
        "Paste JSON:", height=300,
        placeholder='{\n  "status": "verified",\n  "after_save_pageurls": [...]\n}',
        value=default_val, key="json_ta"
    )

    if json_input:
        st.session_state.last_input = json_input

    # Buttons
    b1, b2, b3 = st.columns([2, 2, 2])
    with b1:
        run_btn = st.button("🚀 Run Audit", type="primary", use_container_width=True)
    with b2:
        clr_btn = st.button("🗑️ Clear All", use_container_width=True)
    with b3:
        if st.session_state.audit_result_data is not None:
            st.download_button(
                "📥 Audit Report",
                data=json.dumps(st.session_state.audit_result_data, indent=2),
                file_name=f"audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json", use_container_width=True
            )

    # Clear — reset everything and rerun
    if clr_btn:
        st.session_state.audit_result_data = None
        st.session_state.audit_json_data = None
        st.session_state.last_input = ""
        st.session_state.clear_trigger = True
        st.rerun()

    # Run Audit
    if run_btn:
        if not json_input or not json_input.strip():
            st.warning("⚠️ Paste JSON first!")
        else:
            with st.spinner("🔄 Auditing..."):
                data, errs = URLAuditor.parse_json(json_input)
                if data is None:
                    st.error("❌ JSON parse failed")
                    with st.expander("Errors"):
                        for e in errs:
                            st.text(e)
                else:
                    st.session_state.audit_result_data = URLAuditor.audit_urls(data)
                    st.session_state.audit_json_data = data
                    st.success("✅ Done!")

    # =====================================================================
    # AUDIT RESULTS
    # =====================================================================
    if (st.session_state.audit_result_data is not None
            and st.session_state.audit_json_data is not None):

        data = st.session_state.audit_json_data
        res = st.session_state.audit_result_data

        if not isinstance(res, dict) or "issues_found" not in res:
            st.error("Audit results corrupted. Please run audit again.")
            st.session_state.audit_result_data = None
            st.session_state.audit_json_data = None
            st.rerun()
            return

        st.markdown("---")
        ticker = data.get("ticker", data.get("as_company_id", "?"))
        cid = data.get("as_company_id", data.get("id", "?"))
        st.header(f"📊 {ticker} ({cid})")
        st.caption(f"Agent Status: **{data.get('status', 'N/A')}**")

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("URLs", res.get("total_urls", 0))
        with m2:
            st.metric("Issues", res.get("issues_found", 0))
        with m3:
            st.metric("Status",
                       "✅ PASS" if res.get("issues_found", 0) == 0 else "❌ FAIL")

        with st.expander("📋 Parsed Fields", expanded=False):
            f1, f2 = st.columns(2)
            with f1:
                for f in ['status', 'case_type', 'project', 'issue_area']:
                    st.write(f"**{f}:** {data.get(f, 'N/A')}")
            with f2:
                for f in ['final_status', 'irsp_provider', 'research_status', 'verified']:
                    st.write(f"**{f}:** {data.get(f, 'N/A')}")

        if res.get("issues_found", 0) == 0:
            st.markdown(
                '<div class="success-box"><h3>✓ No Issues!</h3></div>',
                unsafe_allow_html=True
            )
        else:
            st.subheader(f"⚠️ {res['issues_found']} Issues")
            by_type = {}
            for iss in res.get("issues", []):
                by_type.setdefault(iss["type"], []).append(iss)
            for itype, ilist in by_type.items():
                with st.expander(f"**{itype}** ({len(ilist)})", expanded=True):
                    for i, iss in enumerate(ilist, 1):
                        st.markdown(f"**#{i}:**")
                        if 'url_index' in iss:
                            st.write(f"📍 Index: {iss['url_index']}")
                        if 'url_indices' in iss:
                            st.write(f"📍 {iss['url_indices']} ({iss['occurrences']}x)")
                        if 'field' in iss:
                            st.write(f"🏷️ `{iss['field']}` — {iss['message']}")
                        if 'url' in iss:
                            st.markdown(
                                display_url_wrapped(iss['url']),
                                unsafe_allow_html=True
                            )
                        if 'details' in iss:
                            st.info(iss['details'])
                        if i < len(ilist):
                            st.markdown("---")
            st.table([{"Issue": t, "Count": len(l)} for t, l in by_type.items()])

    st.markdown("---")
    st.markdown(
        '<div style="text-align:center;color:#666;padding:20px;">'
        'URL Audit Tool v3.2</div>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
