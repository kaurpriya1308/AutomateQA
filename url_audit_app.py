import streamlit as st
import json
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd

# Page configuration
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
    .crawl-progress {
        font-family: 'Courier New', monospace;
        font-size: 11px;
        color: #666;
    }
    .dataframe a {
        color: #1a73e8;
        text-decoration: none;
    }
    .dataframe a:hover {
        text-decoration: underline;
    }
    </style>
""", unsafe_allow_html=True)


# =============================================================================
# CONCURRENT DOMAIN CRAWLER
# =============================================================================
class ConcurrentDomainCrawler:

    EXCLUDED_EXTENSIONS = {
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.zip', '.rar', '.tar', '.gz', '.7z',
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.ico', '.webp',
        '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.wav',
        '.css', '.js', '.woff', '.woff2', '.ttf', '.eot', '.otf',
        '.exe', '.dmg', '.msi', '.apk',
    }

    EXCLUDED_PATH_PATTERNS = [
        r'/wp-content/', r'/wp-includes/', r'/wp-admin/',
        r'/assets/', r'/static/', r'/images/', r'/img/',
        r'/fonts/', r'/css/', r'/js/',
        r'javascript:', r'mailto:', r'tel:',
        r'/cdn-cgi/', r'/feed/', r'/rss/',
        r'/login', r'/logout', r'/signup', r'/register',
        r'/cart', r'/checkout', r'/account',
        r'/page/\d+', r'\?replytocom=',
        r'/xmlrpc\.php', r'/wp-json/',
    ]

    def __init__(self, max_depth=10, max_pages=1000, max_workers=50,
                 timeout=10, delay=0.1):
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.max_workers = max_workers
        self.timeout = timeout
        self.delay = delay
        self._lock = threading.Lock()
        self._pages_crawled = 0

    def _make_session(self):
        s = requests.Session()
        s.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        s.max_redirects = 5
        return s

    def _is_valid_url(self, url, allowed_domains):
        try:
            parsed = urlparse(url)
            if not parsed.scheme or parsed.scheme not in ('http', 'https'):
                return False
            if not parsed.netloc:
                return False
            domain = parsed.netloc.lower().replace('www.', '')
            if domain not in allowed_domains:
                return False
            path_lower = parsed.path.lower()
            for ext in self.EXCLUDED_EXTENSIONS:
                if path_lower.endswith(ext):
                    return False
            url_lower = url.lower()
            for pat in self.EXCLUDED_PATH_PATTERNS:
                if re.search(pat, url_lower):
                    return False
            return True
        except Exception:
            return False

    def _normalize_url(self, url):
        try:
            parsed = urlparse(url)
            normalized = parsed._replace(fragment='')
            path = normalized.path.rstrip('/') or '/'
            normalized = normalized._replace(path=path)
            if normalized.query:
                clean = [
                    p for p in normalized.query.split('&')
                    if p.split('=')[0].lower() not in
                    ('utm_source', 'utm_medium', 'utm_campaign',
                     'utm_term', 'utm_content', 'fbclid', 'gclid')
                ]
                normalized = normalized._replace(query='&'.join(clean))
            return normalized.geturl()
        except Exception:
            return url

    def _fetch_links(self, url, session):
        links = []
        try:
            time.sleep(self.delay)
            r = session.get(url, timeout=self.timeout, allow_redirects=True)
            if r.status_code != 200:
                return links
            if 'text/html' not in r.headers.get('Content-Type', ''):
                return links
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href'].strip()
                if href and not href.startswith('#') and not href.startswith('javascript:'):
                    links.append(urljoin(url, href))
        except Exception:
            pass
        return links

    def _crawl_batch(self, urls_with_depth, allowed_domains, visited, session):
        new_urls = []
        for url, depth in urls_with_depth:
            if depth > self.max_depth:
                continue
            with self._lock:
                if url in visited:
                    continue
                visited.add(url)
                self._pages_crawled += 1
                if self._pages_crawled > self.max_pages:
                    return new_urls

            for link in self._fetch_links(url, session):
                norm = self._normalize_url(link)
                if self._is_valid_url(norm, allowed_domains):
                    with self._lock:
                        if norm not in visited:
                            new_urls.append((norm, depth + 1))
        return new_urls

    def crawl(self, after_urls, progress_callback=None):
        """
        Crawl ALL domains found in after_urls.
        Returns dict: {url: {"seed": ..., "depth": ..., "domain": ...}}
        """
        seed_urls = []
        allowed_domains = set()

        for url in after_urls:
            if not isinstance(url, str):
                continue
            url = url.strip()
            if url.startswith('http'):
                parsed = urlparse(url)
                domain = parsed.netloc.lower().replace('www.', '')
                allowed_domains.add(domain)
                norm = self._normalize_url(url)
                seed_urls.append(norm)

        if not seed_urls:
            return {}

        self._pages_crawled = 0
        visited = set()
        all_discovered = {}

        current_level = []
        for url in seed_urls:
            parsed = urlparse(url)
            domain = parsed.netloc.lower().replace('www.', '')
            all_discovered[url] = {"seed": url, "depth": 0, "domain": domain}
            current_level.append((url, 0))

        if progress_callback:
            progress_callback(
                0, len(seed_urls), len(all_discovered), 0,
                f"Starting: {len(seed_urls)} seeds, {len(allowed_domains)} domain(s)"
            )

        for depth_level in range(self.max_depth + 1):
            if not current_level or self._pages_crawled >= self.max_pages:
                break

            batch_size = max(1, len(current_level) // self.max_workers)
            batches = [
                current_level[i:i + batch_size]
                for i in range(0, len(current_level), batch_size)
            ]

            next_level = []

            with ThreadPoolExecutor(
                max_workers=min(self.max_workers, max(len(batches), 1))
            ) as ex:
                futures = [
                    ex.submit(
                        self._crawl_batch, batch, allowed_domains,
                        visited, self._make_session()
                    )
                    for batch in batches
                ]
                for f in as_completed(futures):
                    try:
                        for new_url, new_depth in f.result(timeout=120):
                            if new_url not in all_discovered:
                                parsed = urlparse(new_url)
                                domain = parsed.netloc.lower().replace('www.', '')
                                seed = next(
                                    (s for s in seed_urls
                                     if urlparse(s).netloc.lower().replace('www.', '') == domain),
                                    seed_urls[0]
                                )
                                all_discovered[new_url] = {
                                    "seed": seed,
                                    "depth": new_depth,
                                    "domain": domain
                                }
                                next_level.append((new_url, new_depth))
                    except Exception:
                        continue

            if progress_callback:
                progress_callback(
                    self._pages_crawled, len(next_level),
                    len(all_discovered), depth_level,
                    f"Depth {depth_level} done | Crawled: {self._pages_crawled} | "
                    f"Next: {len(next_level)}"
                )

            current_level = next_level

        return all_discovered


# =============================================================================
# URL MATCHING
# =============================================================================
class URLMatcher:

    @staticmethod
    def extract_http_urls(urls):
        return [
            u.strip() for u in urls
            if isinstance(u, str) and u.strip().startswith('http')
        ]

    @staticmethod
    def extract_all_domains(urls):
        domains = set()
        for u in urls:
            if isinstance(u, str) and u.strip().startswith('http'):
                d = urlparse(u.strip()).netloc.lower().replace('www.', '')
                if d:
                    domains.add(d)
        return domains

    @staticmethod
    def extract_regex_patterns(urls):
        return [
            u.strip() for u in urls
            if isinstance(u, str) and re.match(r'^(ev|cp|df|if):', u.strip(), re.IGNORECASE)
        ]

    @staticmethod
    def is_url_covered(discovered_url, after_urls, regex_patterns):
        """Returns (covered: bool, reason: str)"""
        norm = discovered_url.rstrip('/').replace('://www.', '://')
        for au in after_urls:
            if not isinstance(au, str) or not au.startswith('http'):
                continue
            if norm == au.strip().rstrip('/').replace('://www.', '://'):
                return True, "Exact match in after URLs"

        parsed = urlparse(discovered_url)
        for pat_str in regex_patterns:
            m = re.match(r'^(ev|cp|df|if):(.*)', pat_str, re.IGNORECASE)
            if not m:
                continue
            regex_part = m.group(2)
            if not regex_part:
                continue
            try:
                if re.search(regex_part, parsed.path) or re.search(regex_part, discovered_url):
                    return True, f"Matched regex: {pat_str[:60]}"
            except re.error:
                continue

        return False, ""


# =============================================================================
# URL AUDITOR
# =============================================================================
class URLAuditor:

    TEMPLATE_INDICATORS = [
        r'\$\{', r'\{miny', r'\{epp', r'\{xpath', r'\{onclick',
        r'\{json[=_]', r'\{js_', r'\{jsarg', r'\{baseurl', r'\{window',
        r'^cp:', r'^ev:', r'^df:', r'^if:', r'wd:', r'curl:',
        r'json:xhr:', r'json:curl:', r'appid',
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
        for u in urls:
            if not isinstance(u, str):
                continue
            for p in URLAuditor.TEMPLATE_INDICATORS:
                if re.search(p, u, re.IGNORECASE):
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
                    issues.append({
                        "type": "MINY Template Incorrect",
                        "url_index": i, "url": u
                    })
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
                    issues.append({
                        "type": "EPP Template Incorrect",
                        "url_index": i, "url": u
                    })
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
                issues.append({
                    "type": "XPATH Template Incorrect",
                    "url_index": i, "url": u
                })
        return issues

    @staticmethod
    def check_onclick(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"\{onclick", u) and not re.search(r'\$\{onclick_var=\:\".*\"\}', u):
                issues.append({
                    "type": "ONCLICK Template Incorrect",
                    "url_index": i, "url": u
                })
        return issues

    @staticmethod
    def check_jsarg(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"jsarg", u) and not re.search(r'\$\{jsarg=\:\d\}', u):
                issues.append({
                    "type": "JSARG Template Incorrect",
                    "url_index": i, "url": u
                })
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
                    issues.append({
                        "type": "JSON Template Incorrect",
                        "url_index": i, "url": u
                    })
            elif re.search(r"\{json_", u):
                if not re.search(
                    r"\$\{json_data_load=\:1\}|\$\{json_data_load=\:True\}", u
                ):
                    issues.append({
                        "type": "JSON Data Load Incorrect",
                        "url_index": i, "url": u
                    })
            elif re.search(r"\{js_", u):
                if not re.search(r"\$\{js_json=\:1\}", u):
                    issues.append({
                        "type": "JS JSON Incorrect",
                        "url_index": i, "url": u
                    })
        return issues

    @staticmethod
    def check_baseurl(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"\{baseurl", u):
                if not re.search(
                    r"\$\{baseurl=\:\".*\"\}|\$\{full_baseurl=\:True\}", u
                ):
                    issues.append({
                        "type": "BASEURL Template Incorrect",
                        "url_index": i, "url": u
                    })
        return issues

    @staticmethod
    def check_windowflag(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if re.search(r"\{window", u):
                if not re.search(
                    r"\$\{window_flag_regex=\:\".*\"\}|\$\{window_flag=\:True\}", u
                ):
                    issues.append({
                        "type": "Window Flag Incorrect",
                        "url_index": i, "url": u
                    })
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
                    issues.append({
                        "type": "Regex - Uppercase not escaped",
                        "url_index": i, "url": u
                    })
                elif len(u) >= 11 and u[2] != ":":
                    issues.append({
                        "type": "Regex - Missing colon",
                        "url_index": i, "url": u
                    })
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
                cleaned = re.sub(
                    r'\$\{baseurl\=\:\"http', '', u, count=1, flags=re.IGNORECASE
                )
                has_multi = bool(re.search(r"http.*http", cleaned, re.IGNORECASE))
            if not has_http:
                issues.append({
                    "type": "Missing HTTP/HTTPS",
                    "url_index": i, "url": u
                })
            elif has_multi:
                issues.append({
                    "type": "Multiple HTTP in URL",
                    "url_index": i, "url": u
                })
            elif re.search(r"\n", u):
                issues.append({
                    "type": "Newline in URL",
                    "url_index": i, "url": u
                })
        return issues

    @staticmethod
    def check_brackets(urls):
        issues = []
        for i, u in enumerate(urls, 1):
            if not isinstance(u, str):
                continue
            if u.count("{") != u.count("}"):
                issues.append({
                    "type": "Mismatched Brackets",
                    "url_index": i, "url": u,
                    "details": f"{{ {u.count('{')}, }} {u.count('}')}"
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
                    "type": "Duplicate URL",
                    "url_indices": idx, "url": u,
                    "occurrences": len(idx)
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
            issues.append({
                "type": "Metadata Error", "field": "case_type",
                "message": "No Case Type with active Agent status"
            })

        if any("curl:" in str(u) for u in aurls if isinstance(u, str)):
            if ct != "cookie_case":
                issues.append({
                    "type": "Metadata Error", "field": "case_type",
                    "message": "curl: found but case_type not cookie_case"
                })

        if any("s3.amazonaws.com" in str(u) for u in aurls if isinstance(u, str)):
            if ct != "manual_solution_webpage_generated":
                issues.append({
                    "type": "Metadata Error", "field": "case_type",
                    "message": "S3 URL but case_type not manual_solution_webpage_generated"
                })

        if rs == "not_fixed" and proj != "QA":
            issues.append({
                "type": "Metadata Error", "field": "research_status",
                "message": "not_fixed but project not QA"
            })

        if (not ia
                and agent not in ["internal_review", "miscellaneous"]
                and proj not in ["New Ticker", "QA"]):
            issues.append({
                "type": "Metadata Error", "field": "issue_area",
                "message": "Issue Area missing"
            })

        if not aurls and has_active:
            issues.append({
                "type": "Metadata Error", "field": "after_save_pageurls",
                "message": "Active status but no URLs"
            })

        if irsp.lower() == "q4web" and not has_active:
            issues.append({
                "type": "Metadata Error", "field": "irsp_provider",
                "message": "Q4Web with non-active status"
            })

        wd = [u for u in aurls if isinstance(u, str) and re.search(r"wd:", u)]
        if wd and ct == "direct":
            issues.append({
                "type": "Metadata Error", "field": "case_type",
                "message": "WD in URLs but case_type=direct"
            })

        if ct == "direct" and aurls and URLAuditor.urls_contain_templates(aurls):
            issues.append({
                "type": "Metadata Error", "field": "case_type",
                "message": "Direct but templates found in URLs"
            })

        if is_active:
            if not ia:
                issues.append({
                    "type": "Metadata Error", "field": "issue_area",
                    "message": "Issue Area blank"
                })
            if not fs:
                issues.append({
                    "type": "Metadata Error", "field": "final_status",
                    "message": "Final Status blank"
                })

        has_cp = any(
            isinstance(u, str) and u.strip().startswith("cp:")
            for u in aurls
        )
        if has_cp and irsp:
            issues.append({
                "type": "Metadata Error", "field": "irsp_provider",
                "message": f"cp: in URLs but irsp_provider='{irsp}'"
            })

        f3 = aurls[:3] if len(aurls) >= 3 else aurls
        has_text = any(
            isinstance(u, str) and u.strip().lower().startswith("text:")
            for u in f3
        )
        if has_text and irsp != "Q4Web":
            issues.append({
                "type": "Metadata Error", "field": "irsp_provider",
                "message": f"text: in first 3 URLs but irsp_provider='{irsp}'"
            })

        return issues

    @classmethod
    def audit_urls(cls, data):
        urls = data.get("after_save_pageurls", [])
        issues = []
        if urls:
            for fn in [
                cls.check_miny, cls.check_epp, cls.check_xpath,
                cls.check_onclick, cls.check_jsarg, cls.check_json_template,
                cls.check_baseurl, cls.check_windowflag, cls.check_regex,
                cls.check_http, cls.check_brackets, cls.check_duplicates
            ]:
                issues.extend(fn(urls))
        issues.extend(cls.check_metadata(data))
        return {
            "status": "Complete",
            "total_urls": len(urls),
            "issues_found": len(issues),
            "issues": issues
        }


# =============================================================================
# DISPLAY HELPER
# =============================================================================
def display_url_wrapped(url):
    return f'<div class="url-text">{url}</div>'


# =============================================================================
# MAIN
# =============================================================================
def main():
    st.title("🔍 URL Audit Tool")
    st.markdown("---")

    with st.expander("ℹ️ Instructions", expanded=False):
        st.markdown("""
        1. **Paste JSON** → **Run Audit** for template/metadata checks
        2. **Check for Missing URLs** → Crawls ALL domains from after URLs
           at depth 10 with 50 concurrent threads
        3. Missing URLs shown in a **table with clickable links** + CSV/JSON download
        """)

    # Session state — use distinct keys
    if 'audit_result_data' not in st.session_state:
        st.session_state.audit_result_data = None
    if 'audit_json_data' not in st.session_state:
        st.session_state.audit_json_data = None
    if 'clear_trigger' not in st.session_state:
        st.session_state.clear_trigger = False
    if 'crawl_summary' not in st.session_state:
        st.session_state.crawl_summary = None
    if 'missing_df' not in st.session_state:
        st.session_state.missing_df = None

    # JSON Input
    st.subheader("📝 JSON Input")
    dv = "" if st.session_state.clear_trigger else st.session_state.get('last_input', '')
    json_input = st.text_area(
        "Paste JSON:", height=300,
        placeholder='{\n  "status": "verified",\n  "after_save_pageurls": [...]\n}',
        value=dv, key="json_ta"
    )
    if json_input and not st.session_state.clear_trigger:
        st.session_state.last_input = json_input
    if st.session_state.clear_trigger:
        st.session_state.clear_trigger = False

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

    # Clear
    if clr_btn:
        st.session_state.audit_result_data = None
        st.session_state.audit_json_data = None
        st.session_state.crawl_summary = None
        st.session_state.missing_df = None
        st.session_state.last_input = ""
        st.session_state.clear_trigger = True
        st.rerun()

    # Run Audit
    if run_btn:
        if not json_input.strip():
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
                    st.session_state.crawl_summary = None
                    st.session_state.missing_df = None
                    st.success("✅ Done!")

    # =====================================================================
    # AUDIT RESULTS
    # =====================================================================
    if (st.session_state.audit_result_data is not None
            and st.session_state.audit_json_data is not None):

        data = st.session_state.audit_json_data
        res = st.session_state.audit_result_data

        # Safety check
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
            st.metric(
                "Status",
                "✅ PASS" if res.get("issues_found", 0) == 0 else "❌ FAIL"
            )

        with st.expander("📋 Fields", expanded=False):
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
            st.table([
                {"Issue": t, "Count": len(l)} for t, l in by_type.items()
            ])

        # =============================================================
        # MISSING URL CHECK
        # =============================================================
        st.markdown("---")
        st.header("🌐 Missing URL Check")

        after_urls = data.get("after_save_pageurls", [])
        all_http = URLMatcher.extract_http_urls(after_urls)
        all_domains = URLMatcher.extract_all_domains(after_urls)

        if not all_http:
            st.warning("⚠️ No HTTP URLs in after_save_pageurls.")
        else:
            st.markdown(
                f"**Domains to crawl ({len(all_domains)}):** "
                f"`{'`, `'.join(sorted(all_domains))}`"
            )
            st.markdown(
                f"**Seed URLs:** {len(all_http)} HTTP URLs from after_save_pageurls"
            )

            with st.expander("⚙️ Crawl Settings", expanded=False):
                s1, s2, s3 = st.columns(3)
                with s1:
                    depth = st.slider("Max Depth", 1, 15, 10, key="cd")
                with s2:
                    pages = st.slider("Max Pages", 100, 2000, 1000, 100, key="cp")
                with s3:
                    workers = st.slider("Threads", 10, 100, 50, 5, key="cw")

            crawl_btn = st.button(
                "🔎 Check for Missing URLs", type="secondary",
                use_container_width=True
            )

            if crawl_btn:
                crawler = ConcurrentDomainCrawler(
                    max_depth=depth, max_pages=pages, max_workers=workers
                )

                prog = st.progress(0)
                stat = st.empty()

                def cb(crawled, queued, discovered, d, msg):
                    prog.progress(min(crawled / pages, 1.0))
                    stat.markdown(
                        f"**Crawled:** {crawled} | **Queued:** {queued} | "
                        f"**Found:** {discovered} | **Depth:** {d}"
                    )

                with st.spinner(
                    f"🕷️ Crawling {len(all_domains)} domain(s) with "
                    f"{workers} threads, depth {depth}..."
                ):
                    discovered = crawler.crawl(after_urls, progress_callback=cb)

                prog.progress(1.0)
                stat.markdown(
                    f"✅ **Crawl complete!** Found **{len(discovered)}** URLs "
                    f"across **{len(all_domains)}** domain(s)"
                )

                # Compare against after URLs
                regex_pats = URLMatcher.extract_regex_patterns(after_urls)
                missing_rows = []
                covered_count = 0

                for url, info in sorted(discovered.items()):
                    covered, reason = URLMatcher.is_url_covered(
                        url, after_urls, regex_pats
                    )
                    if covered:
                        covered_count += 1
                    else:
                        missing_rows.append({
                            "Domain": info["domain"],
                            "Source Seed URL": info["seed"],
                            "Missing URL": url,
                            "Depth Found": info["depth"],
                        })

                st.session_state.crawl_summary = {
                    "total_discovered": len(discovered),
                    "covered_count": covered_count,
                    "missing_count": len(missing_rows),
                }

                if missing_rows:
                    st.session_state.missing_df = pd.DataFrame(missing_rows)
                else:
                    st.session_state.missing_df = None

            # ==========================================================
            # DISPLAY CRAWL RESULTS
            # ==========================================================
            if st.session_state.crawl_summary is not None:
                cs = st.session_state.crawl_summary

                if not isinstance(cs, dict) or "total_discovered" not in cs:
                    st.warning("Crawl data corrupted. Please re-run.")
                    st.session_state.crawl_summary = None
                    st.session_state.missing_df = None
                else:
                    st.markdown("---")
                    st.subheader("📊 Crawl Results")

                    x1, x2, x3 = st.columns(3)
                    with x1:
                        st.metric("Total Found", cs["total_discovered"])
                    with x2:
                        st.metric("Already Covered", cs["covered_count"])
                    with x3:
                        st.metric("Missing from After URLs", cs["missing_count"])

                    if (st.session_state.missing_df is not None
                            and not st.session_state.missing_df.empty):

                        df = st.session_state.missing_df

                        st.markdown(
                            f"### 🔴 {len(df)} URLs Found on Domain "
                            f"but Missing from After URLs"
                        )
                        st.markdown("*Click any URL to open in new tab*")

                        def make_link(url):
                            short = url if len(url) <= 80 else url[:77] + "..."
                            return (
                                f'<a href="{url}" target="_blank" '
                                f'title="{url}">{short}</a>'
                            )

                        display_df = df.copy()
                        display_df["Missing URL"] = display_df["Missing URL"].apply(
                            make_link
                        )
                        display_df["Source Seed URL"] = display_df[
                            "Source Seed URL"
                        ].apply(make_link)

                        sort_col = st.selectbox(
                            "Sort by:",
                            ["Depth Found", "Domain", "Missing URL"],
                            index=0, key="sort_col"
                        )

                        sorted_idx = df.sort_values(sort_col).index
                        display_df = display_df.loc[sorted_idx].reset_index(drop=True)
                        display_df.index = display_df.index + 1
                        display_df.index.name = "#"

                        st.markdown(
                            display_df.to_html(escape=False, index=True),
                            unsafe_allow_html=True
                        )

                        # Domain breakdown
                        st.markdown("### 📊 Missing URLs by Domain")
                        domain_counts = (
                            df["Domain"].value_counts().reset_index()
                        )
                        domain_counts.columns = ["Domain", "Missing URLs"]
                        st.table(domain_counts)

                        # Depth breakdown
                        st.markdown("### 📊 Missing URLs by Depth")
                        depth_counts = (
                            df["Depth Found"]
                            .value_counts()
                            .sort_index()
                            .reset_index()
                        )
                        depth_counts.columns = ["Depth", "Count"]
                        st.bar_chart(depth_counts.set_index("Depth"))

                        # Downloads
                        st.markdown("### 📥 Download")
                        d1, d2 = st.columns(2)
                        with d1:
                            st.download_button(
                                "📥 Download CSV",
                                data=df.to_csv(index=False),
                                file_name=(
                                    f"missing_urls_"
                                    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                                ),
                                mime="text/csv",
                                use_container_width=True
                            )
                        with d2:
                            st.download_button(
                                "📥 Download JSON",
                                data=df.to_json(orient="records", indent=2),
                                file_name=(
                                    f"missing_urls_"
                                    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                                ),
                                mime="application/json",
                                use_container_width=True
                            )

                    elif cs["missing_count"] == 0:
                        st.markdown("""
                            <div class="success-box">
                                <h3>✓ No Missing URLs!</h3>
                                <p>All discovered URLs on the domain(s) are covered
                                by after_save_pageurls (exact match or regex).</p>
                            </div>
                        """, unsafe_allow_html=True)

    # Footer
    st.markdown("---")
    st.markdown(
        '<div style="text-align:center;color:#666;padding:20px;">'
        'URL Audit Tool v3.0</div>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
