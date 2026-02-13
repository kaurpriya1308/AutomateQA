import streamlit as st
import json
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import time
from collections import deque

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
    .missing-url-box {
        border-left: 4px solid #ff9800;
        padding: 12px;
        margin: 8px 0;
        background-color: #fff8e1;
        border-radius: 4px;
    }
    .found-url-box {
        border-left: 4px solid #2196f3;
        padding: 12px;
        margin: 8px 0;
        background-color: #e3f2fd;
        border-radius: 4px;
    }
    .summary-card {
        padding: 20px;
        border-radius: 8px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .crawl-progress {
        font-family: 'Courier New', monospace;
        font-size: 11px;
        color: #666;
    }
    </style>
""", unsafe_allow_html=True)


# =============================================================================
# URL CRAWLER CLASS
# =============================================================================
class DomainCrawler:
    """Crawls a domain up to a specified depth and returns discovered URLs."""

    EXCLUDED_EXTENSIONS = {
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.zip', '.rar', '.tar', '.gz', '.7z',
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.ico', '.webp',
        '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.wav',
        '.css', '.js', '.json', '.xml', '.rss', '.atom',
        '.woff', '.woff2', '.ttf', '.eot', '.otf',
        '.exe', '.dmg', '.msi', '.apk',
    }

    EXCLUDED_PATH_PATTERNS = [
        r'/wp-content/', r'/wp-includes/', r'/wp-admin/',
        r'/assets/', r'/static/', r'/images/', r'/img/',
        r'/fonts/', r'/css/', r'/js/',
        r'#', r'javascript:', r'mailto:', r'tel:',
        r'/cdn-cgi/', r'/feed/', r'/rss/',
        r'/login', r'/logout', r'/signup', r'/register',
        r'/cart', r'/checkout', r'/account',
        r'/search', r'/tag/', r'/category/',
        r'/page/\d+', r'\?replytocom=',
        r'/xmlrpc\.php', r'/wp-json/',
        r'/privacy', r'/terms', r'/cookie', r'/legal',
        r'/disclaimer', r'/imprint', r'/impressum',
        r'/sitemap', r'\.xml$',
    ]

    # Patterns that likely indicate IR/financial content pages
    IR_CONTENT_PATTERNS = [
        r'investor', r'annual.?report', r'quarterly', r'financial',
        r'press.?release', r'news', r'event', r'presentation',
        r'earning', r'sec.?filing', r'proxy', r'governance',
        r'board', r'shareholder', r'dividend', r'stock',
        r'sustainability', r'esg', r'csr', r'responsibility',
        r'report', r'document', r'filing', r'archive',
        r'result', r'disclosure', r'regulatory',
        r'agm', r'meeting', r'webcast', r'transcript',
    ]

    def __init__(self, max_depth=10, max_pages=500, timeout=10, delay=0.3):
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.timeout = timeout
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

    def _is_valid_url(self, url, base_domain):
        """Check if URL belongs to the same domain and is not excluded."""
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return False
            # Same domain check
            url_domain = parsed.netloc.lower().replace('www.', '')
            if url_domain != base_domain:
                return False
            # Extension check
            path_lower = parsed.path.lower()
            for ext in self.EXCLUDED_EXTENSIONS:
                if path_lower.endswith(ext):
                    return False
            # Excluded path pattern check
            full_url_lower = url.lower()
            for pattern in self.EXCLUDED_PATH_PATTERNS:
                if re.search(pattern, full_url_lower):
                    return False
            return True
        except Exception:
            return False

    def _normalize_url(self, url):
        """Normalize URL for comparison."""
        try:
            parsed = urlparse(url)
            # Remove fragment
            normalized = parsed._replace(fragment='')
            # Remove trailing slash from path
            path = normalized.path.rstrip('/')
            if not path:
                path = '/'
            normalized = normalized._replace(path=path)
            return normalized.geturl()
        except Exception:
            return url

    def crawl(self, start_urls, progress_callback=None):
        """
        Crawl starting from given URLs up to max_depth.
        Returns set of discovered URLs.
        """
        if not start_urls:
            return set()

        # Determine base domain from first HTTP URL
        base_domain = None
        for url in start_urls:
            if isinstance(url, str) and url.startswith('http'):
                parsed = urlparse(url)
                base_domain = parsed.netloc.lower().replace('www.', '')
                break

        if not base_domain:
            return set()

        visited = set()
        discovered = set()
        # Queue: (url, depth)
        queue = deque()

        # Seed with start URLs
        for url in start_urls:
            if isinstance(url, str) and url.startswith('http'):
                norm = self._normalize_url(url)
                queue.append((norm, 0))
                discovered.add(norm)

        pages_crawled = 0

        while queue and pages_crawled < self.max_pages:
            current_url, depth = queue.popleft()

            if current_url in visited:
                continue

            if depth > self.max_depth:
                continue

            visited.add(current_url)
            pages_crawled += 1

            if progress_callback:
                progress_callback(pages_crawled, len(queue), len(discovered), depth, current_url)

            try:
                time.sleep(self.delay)
                response = self.session.get(
                    current_url,
                    timeout=self.timeout,
                    allow_redirects=True
                )

                if response.status_code != 200:
                    continue

                content_type = response.headers.get('Content-Type', '')
                if 'text/html' not in content_type:
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')

                # Extract all links
                for tag in soup.find_all('a', href=True):
                    href = tag['href'].strip()
                    if not href:
                        continue

                    # Resolve relative URLs
                    absolute_url = urljoin(current_url, href)
                    normalized = self._normalize_url(absolute_url)

                    if self._is_valid_url(normalized, base_domain):
                        discovered.add(normalized)
                        if normalized not in visited and depth + 1 <= self.max_depth:
                            queue.append((normalized, depth + 1))

            except requests.exceptions.RequestException:
                continue
            except Exception:
                continue

        return discovered

    @staticmethod
    def is_ir_relevant(url):
        """Check if a URL likely contains IR/financial content."""
        url_lower = url.lower()
        for pattern in DomainCrawler.IR_CONTENT_PATTERNS:
            if re.search(pattern, url_lower):
                return True
        return False


# =============================================================================
# URL MATCHING UTILITIES
# =============================================================================
class URLMatcher:
    """Utilities to check if a discovered URL is already covered by after_save_pageurls."""

    @staticmethod
    def extract_domains_from_urls(urls):
        """Extract unique base domains from a list of URLs."""
        domains = set()
        for url in urls:
            if isinstance(url, str) and url.startswith('http'):
                parsed = urlparse(url)
                domain = parsed.netloc.lower().replace('www.', '')
                domains.add(domain)
        return domains

    @staticmethod
    def extract_http_urls(urls):
        """Extract only HTTP/HTTPS URLs from the list."""
        http_urls = []
        for url in urls:
            if isinstance(url, str) and url.strip().startswith('http'):
                http_urls.append(url.strip())
        return http_urls

    @staticmethod
    def extract_regex_patterns(urls):
        """Extract ev:/cp:/df:/if: regex patterns from the URL list."""
        patterns = []
        for url in urls:
            if not isinstance(url, str):
                continue
            url = url.strip()
            if re.match(r'^(ev|cp|df|if):', url, re.IGNORECASE):
                patterns.append(url)
        return patterns

    @staticmethod
    def url_matches_exact(discovered_url, after_urls):
        """Check if discovered URL exactly matches any after URL."""
        norm_discovered = discovered_url.rstrip('/')
        for after_url in after_urls:
            if not isinstance(after_url, str) or not after_url.startswith('http'):
                continue
            norm_after = after_url.strip().rstrip('/')
            if norm_discovered == norm_after:
                return True
            # Also check with/without www
            if norm_discovered.replace('://www.', '://') == norm_after.replace('://www.', '://'):
                return True
        return False

    @staticmethod
    def url_matches_regex_pattern(discovered_url, regex_patterns):
        """
        Check if discovered URL path matches any ev:/cp:/df:/if: regex.
        Returns the matching pattern if found, else None.
        """
        parsed = urlparse(discovered_url)
        url_path = parsed.path

        for pattern_str in regex_patterns:
            # Extract the regex part after the prefix (e.g., "cp:/some_regex")
            match = re.match(r'^(ev|cp|df|if):(.*)', pattern_str, re.IGNORECASE)
            if not match:
                continue

            prefix = match.group(1).lower()
            regex_part = match.group(2)

            if not regex_part:
                continue

            try:
                # Try to match the regex against the URL path
                if re.search(regex_part, url_path):
                    return pattern_str
                # Also try against full URL
                if re.search(regex_part, discovered_url):
                    return pattern_str
            except re.error:
                # Invalid regex, skip
                continue

        return None

    @staticmethod
    def classify_discovered_url(discovered_url, after_urls, regex_patterns):
        """
        Classify a discovered URL against after_save_pageurls.
        Returns a tuple: (is_covered, reason)
        """
        # Check exact match
        if URLMatcher.url_matches_exact(discovered_url, after_urls):
            return True, "Already added"

        # Check regex match
        matching_pattern = URLMatcher.url_matches_regex_pattern(discovered_url, regex_patterns)
        if matching_pattern:
            return True, f"Sub page - scraped with ev/cp regex ({matching_pattern})"

        return False, "Potentially missing"


# =============================================================================
# URL AUDITOR CLASS (same as before with all checks)
# =============================================================================
class URLAuditor:
    """URL Audit logic class"""

    TEMPLATE_INDICATORS = [
        r'\$\{',
        r'\{miny',
        r'\{epp',
        r'\{xpath',
        r'\{onclick',
        r'\{json[=_]',
        r'\{js_',
        r'\{jsarg',
        r'\{baseurl',
        r'\{window',
        r'^cp:',
        r'^ev:',
        r'^df:',
        r'^if:',
        r'wd:',
        r'curl:',
        r'json:xhr:',
        r'json:curl:',
        r'appid',
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

        try:
            return json.loads(cleaned_json), None
        except json.JSONDecodeError as e:
            parse_errors.append(f"Direct parse: {e.msg} at line {e.lineno}")

        try:
            return json.loads(cleaned_json, strict=False), None
        except json.JSONDecodeError as e:
            parse_errors.append(f"Non-strict parse: {e.msg} at line {e.lineno}")

        try:
            fixed_json = cleaned_json
            fixed_json = re.sub(r'"\s*\n\s*"', '",\n"', fixed_json)
            fixed_json = fixed_json.replace("'", '"')
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
        issues = []
        miny_pattern = r"\$\{y\}|\$\{ym1\}|\$\{yp1\}|\$\{y2\}|\$\{ym2\}"
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_miny_bracket = bool(re.search(r"\{miny", url))
            has_miny_syntax = bool(re.search(r"\$\{miny=\:\d{4}\}", url))
            has_miny_template = bool(re.search(miny_pattern, url))
            if has_miny_bracket and (not has_miny_syntax or not has_miny_template):
                issues.append({"type": "MINY Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_epp(urls):
        issues = []
        epp_pattern = r"\$\{p\}|\$\{pm1\}|\$\{pp1\}|\$\{stm1\}|\$\{st\}"
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_epp_bracket = bool(re.search(r"\{epp", url))
            has_epp_syntax = bool(re.search(r"\$\{epp=\:\d{1,2}\}", url))
            has_epp_template = bool(re.search(epp_pattern, url))
            if has_epp_bracket and (not has_epp_syntax or not has_epp_template):
                issues.append({"type": "EPP Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_xpath(urls):
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
                issues.append({"type": "XPATH Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_onclick(urls):
        issues = []
        onclick_pattern = r'\$\{onclick_var=\:\".*\"\}'
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_onclick = bool(re.search(r"\{onclick", url))
            has_valid_onclick = bool(re.search(onclick_pattern, url))
            if has_onclick and not has_valid_onclick:
                issues.append({"type": "ONCLICK Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_jsarg(urls):
        issues = []
        jsarg_pattern = r'\$\{jsarg=\:\d\}'
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_jsarg = bool(re.search(r"jsarg", url))
            has_valid_jsarg = bool(re.search(jsarg_pattern, url))
            if has_jsarg and not has_valid_jsarg:
                issues.append({"type": "JSARG Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_json_template(urls):
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
                issues.append({"type": "JSON Template Incorrect", "url_index": idx, "url": url})
            elif has_json_data and not has_valid_json_data:
                issues.append({"type": "JSON Data Load Template Incorrect", "url_index": idx, "url": url})
            elif has_js and not has_valid_js:
                issues.append({"type": "JS JSON Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_baseurl(urls):
        issues = []
        baseurl_pattern = r"\$\{baseurl=\:\".*\"\}|\$\{full_baseurl=\:True\}"
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_baseurl = bool(re.search(r"\{baseurl", url))
            has_valid_baseurl = bool(re.search(baseurl_pattern, url))
            if has_baseurl and not has_valid_baseurl:
                issues.append({"type": "BASEURL Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_windowflag(urls):
        issues = []
        window_pattern = r"\$\{window_flag_regex=\:\".*\"\}|\$\{window_flag=\:True\}"
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            has_window = bool(re.search(r"\{window", url))
            has_valid_window = bool(re.search(window_pattern, url))
            if has_window and not has_valid_window:
                issues.append({"type": "Window Flag Template Incorrect", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_regex(urls):
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
                    issues.append({"type": "Regex Incorrect - Uppercase not escaped", "url_index": idx, "url": url})
                elif len(url) >= 11 and url[2] != ":":
                    issues.append({"type": "Regex Incorrect - Missing colon at position 2", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_http(urls):
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
            if has_multiple_http:
                baseurl_pat = r'\$\{baseurl\=\:\"http'
                url_without_baseurl = re.sub(baseurl_pat, '', url, count=1, flags=re.IGNORECASE)
                has_multiple_http = bool(re.search(r"http.*http", url_without_baseurl, re.IGNORECASE))
            has_newline = bool(re.search(r"\n", url))
            if not has_http:
                issues.append({"type": "Missing HTTP/HTTPS", "url_index": idx, "url": url})
            elif has_multiple_http:
                issues.append({"type": "Multiple HTTP in URL", "url_index": idx, "url": url})
            elif has_newline:
                issues.append({"type": "Newline character in URL", "url_index": idx, "url": url})
        return issues

    @staticmethod
    def check_brackets(urls):
        issues = []
        for idx, url in enumerate(urls, 1):
            if not isinstance(url, str):
                continue
            open_count = url.count("{")
            close_count = url.count("}")
            if open_count != close_count:
                issues.append({
                    "type": "Mismatched Brackets {}",
                    "url_index": idx, "url": url,
                    "details": f"Open: {open_count}, Close: {close_count}"
                })
        return issues

    @staticmethod
    def check_duplicates(urls):
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
                    "url_indices": indices, "url": url,
                    "occurrences": len(indices)
                })
        return issues

    @staticmethod
    def check_metadata(data):
        issues = []

        agent_status = str(data.get("status", "") or "").strip().lower()
        case_type = str(data.get("case_type", "") or "").strip().lower()
        project = str(data.get("project", "") or "").strip()
        research_status = str(data.get("research_status", "") or "").strip().lower()
        issue_area = str(data.get("issue_area", "") or "").strip()
        final_status_val = str(data.get("final_status", "") or "").strip()
        irsp_provider = str(data.get("irsp_provider", "") or "").strip()
        after_urls = data.get("after_save_pageurls", [])

        is_active_status = bool(re.search(
            r"verified$|manual|escalated_to_technology_team", agent_status
        ))

        if is_active_status and not case_type:
            issues.append({
                "type": "Metadata Error", "field": "case_type",
                "message": "No Case Type with Verified/Manual/Escalated Agent status"
            })

        has_curl = any("curl:" in str(url) for url in after_urls if isinstance(url, str))
        if has_curl and case_type != "cookie_case":
            issues.append({
                "type": "Metadata Error", "field": "case_type",
                "message": "URLs contain 'curl:' but case_type is not 'cookie_case'"
            })

        has_s3 = any("s3.amazonaws.com" in str(url) for url in after_urls if isinstance(url, str))
        if has_s3 and case_type != "manual_solution_webpage_generated":
            issues.append({
                "type": "Metadata Error", "field": "case_type",
                "message": "S3 URL found but case_type is not 'manual_solution_webpage_generated'"
            })

        if research_status == "not_fixed" and project != "QA":
            issues.append({
                "type": "Metadata Error", "field": "research_status / project",
                "message": "Not fixed status but project not QA"
            })

        if (not issue_area
                and agent_status not in ["internal_review", "miscellaneous"]
                and project not in ["New Ticker", "QA"]):
            issues.append({
                "type": "Metadata Error", "field": "issue_area",
                "message": "Issue Area missing for non-covered case"
            })

        has_active_agent = bool(re.search(
            r"verified|manual|escalated|website_is_down|internal_review", agent_status
        ))

        if not after_urls and has_active_agent:
            issues.append({
                "type": "Metadata Error", "field": "after_save_pageurls",
                "message": "Case verified/escalated but no URL added"
            })

        if irsp_provider.lower() == "q4web" and not has_active_agent:
            issues.append({
                "type": "Metadata Error", "field": "irsp_provider",
                "message": "Q4Web added with non-active Agent Status"
            })

        wd_urls = [url for url in after_urls if isinstance(url, str) and bool(re.search(r"wd:", url))]
        if wd_urls and case_type == "direct":
            issues.append({
                "type": "Metadata Error", "field": "case_type",
                "message": "WD added in URLs but case type marked as direct | URLs: " + " || ".join(wd_urls[:3])
            })

        # Amendment 1: Direct + templates
        if case_type == "direct" and after_urls:
            if URLAuditor.urls_contain_templates(after_urls):
                issues.append({
                    "type": "Metadata Error", "field": "case_type",
                    "message": (
                        "'Direct' case_type used but templates/regex patterns "
                        "found in after_save_pageurls. 'Direct' should only be "
                        "used when there are no templates."
                    )
                })

        # Amendment 2: issue_area and final_status must not be blank
        if is_active_status:
            if not issue_area:
                issues.append({
                    "type": "Metadata Error", "field": "issue_area",
                    "message": "Issue Area is blank. It must not be empty."
                })
            if not final_status_val:
                issues.append({
                    "type": "Metadata Error", "field": "final_status",
                    "message": "Final Status is blank. It must not be empty."
                })

        # Amendment 3: cp: in URLs => irsp_provider must be blank
        has_cp = any(
            isinstance(url, str) and url.strip().startswith("cp:")
            for url in after_urls
        )
        if has_cp and irsp_provider:
            issues.append({
                "type": "Metadata Error", "field": "irsp_provider",
                "message": (
                    f"'cp:' pattern found in after_save_pageurls but "
                    f"irsp_provider is '{irsp_provider}'. It should be blank "
                    f"when 'cp:' URLs are present."
                )
            })

        # Amendment 4: text: in first 3 URLs => irsp_provider should be Q4Web
        first_three = after_urls[:3] if len(after_urls) >= 3 else after_urls
        has_text_prefix = any(
            isinstance(url, str) and url.strip().lower().startswith("text:")
            for url in first_three
        )
        if has_text_prefix and irsp_provider != "Q4Web":
            issues.append({
                "type": "Metadata Error", "field": "irsp_provider",
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

        issues.extend(cls.check_metadata(data))

        return {
            "status": "Complete",
            "total_urls": len(urls),
            "issues_found": len(issues),
            "issues": issues
        }


# =============================================================================
# DISPLAY HELPERS
# =============================================================================
def display_url_wrapped(url, max_length=80):
    return f'<div class="url-text">{url}</div>'


# =============================================================================
# MAIN APP
# =============================================================================
def main():
    st.title("🔍 URL Audit Tool")
    st.markdown("---")

    with st.expander("ℹ️ Instructions", expanded=False):
        st.markdown("""
        ### How to use this tool:
        1. **Paste your JSON data** in the text area below
        2. Click **Run Audit** to check for template/metadata errors
        3. Click **Check for Missing URLs** to crawl the domain and find uncovered pages
        
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

        ### Missing URL Checklist Options:
        - **Already added** – URL is already in after_save_pageurls
        - **Added in ticker issue sheet** – URL tracked separately
        - **Added with template** – URL covered via template variables
        - **Sub page - scraped with ev/cp regex** – URL path matches an ev:/cp: regex
        """)

    # Session state
    if 'audit_results' not in st.session_state:
        st.session_state.audit_results = None
    if 'audit_data' not in st.session_state:
        st.session_state.audit_data = None
    if 'clear_trigger' not in st.session_state:
        st.session_state.clear_trigger = False
    if 'crawl_results' not in st.session_state:
        st.session_state.crawl_results = None
    if 'checklist_state' not in st.session_state:
        st.session_state.checklist_state = {}

    # JSON Input
    st.subheader("📝 JSON Input")
    default_value = "" if st.session_state.clear_trigger else st.session_state.get('last_input', '')

    json_input = st.text_area(
        "Paste your JSON data here:",
        height=300,
        placeholder='{\n  "id": "12345",\n  "as_company_id": "TICKER",\n  "status": "verified",\n  "after_save_pageurls": [...]\n}',
        value=default_value,
        key="json_text_area"
    )

    if json_input and not st.session_state.clear_trigger:
        st.session_state.last_input = json_input
    if st.session_state.clear_trigger:
        st.session_state.clear_trigger = False

    # Buttons row
    col1, col2, col3 = st.columns([2, 2, 2])

    with col1:
        run_button = st.button("🚀 Run Audit", type="primary", use_container_width=True)
    with col2:
        clear_button = st.button("🗑️ Clear All", use_container_width=True)
    with col3:
        if st.session_state.audit_results:
            st.download_button(
                label="📥 Download Audit Report",
                data=json.dumps(st.session_state.audit_results, indent=2),
                file_name=f"audit_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                use_container_width=True
            )

    if clear_button:
        st.session_state.audit_results = None
        st.session_state.audit_data = None
        st.session_state.last_input = ""
        st.session_state.clear_trigger = True
        st.session_state.crawl_results = None
        st.session_state.checklist_state = {}
        st.rerun()

    # =========================================================================
    # RUN AUDIT
    # =========================================================================
    if run_button:
        if not json_input.strip():
            st.warning("⚠️ Please paste JSON data first!")
        else:
            with st.spinner("🔄 Processing audit..."):
                data, parse_errors = URLAuditor.parse_json(json_input)
                if data is None:
                    st.error("❌ Failed to parse JSON")
                    with st.expander("View Parse Errors"):
                        for error in parse_errors:
                            st.text(error)
                    st.info("💡 Please ensure your JSON is properly formatted.")
                else:
                    audit_results = URLAuditor.audit_urls(data)
                    st.session_state.audit_results = audit_results
                    st.session_state.audit_data = data
                    # Reset crawl when new audit runs
                    st.session_state.crawl_results = None
                    st.session_state.checklist_state = {}
                    st.success("✅ Audit completed successfully!")

    # =========================================================================
    # DISPLAY AUDIT RESULTS
    # =========================================================================
    if st.session_state.audit_results:
        st.markdown("---")
        data = st.session_state.audit_data
        results = st.session_state.audit_results

        ticker = data.get("ticker", data.get("as_company_id", "Unknown"))
        company_id = data.get("as_company_id", data.get("id", "Unknown"))
        agent_status = data.get("status", "N/A")

        st.header(f"📊 Audit Results for {ticker} ({company_id})")
        st.caption(f"Agent Status (from 'status' field): **{agent_status}**")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total URLs", results["total_urls"])
        with col2:
            st.metric("Issues Found", results["issues_found"])
        with col3:
            status = "✅ PASS" if results["issues_found"] == 0 else "❌ FAIL"
            st.metric("Status", status)

        st.markdown("---")

        with st.expander("📋 Parsed Field Values", expanded=False):
            ic1, ic2 = st.columns(2)
            with ic1:
                st.write(f"**Agent Status (status):** {data.get('status', 'N/A')}")
                st.write(f"**Case Type:** {data.get('case_type', 'N/A')}")
                st.write(f"**Project:** {data.get('project', 'N/A')}")
                st.write(f"**Issue Area:** {data.get('issue_area', 'N/A')}")
            with ic2:
                st.write(f"**Final Status:** {data.get('final_status', 'N/A')}")
                st.write(f"**IRSP Provider:** {data.get('irsp_provider', 'N/A')}")
                st.write(f"**Research Status:** {data.get('research_status', 'N/A')}")
                st.write(f"**Verified (boolean):** {data.get('verified', 'N/A')}")

        if results["issues_found"] == 0:
            st.markdown("""
                <div class="success-box">
                    <h3>✓ No Issues Found!</h3>
                    <p>All URLs passed validation. All templates, formats, and metadata are correct.</p>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.subheader(f"⚠️ Issues Found: {results['issues_found']}")

            issues_by_type = {}
            for issue in results["issues"]:
                issue_type = issue["type"]
                if issue_type not in issues_by_type:
                    issues_by_type[issue_type] = []
                issues_by_type[issue_type].append(issue)

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

            st.markdown("### 📋 Issue Summary")
            issue_summary = [
                {"Issue Type": it, "Count": len(ti)}
                for it, ti in issues_by_type.items()
            ]
            st.table(issue_summary)

        # =====================================================================
        # MISSING URL CHECK SECTION
        # =====================================================================
        st.markdown("---")
        st.header("🌐 Missing URL Check")
        st.markdown(
            "Crawl the domain(s) found in the first 5 after URLs to discover pages "
            "that may not be covered by the current URL configuration."
        )

        after_urls = data.get("after_save_pageurls", [])
        http_urls = URLMatcher.extract_http_urls(after_urls[:5])

        if not http_urls:
            st.warning("⚠️ No HTTP URLs found in the first 5 after_save_pageurls. Cannot crawl.")
        else:
            domains = URLMatcher.extract_domains_from_urls(http_urls)
            st.info(f"🔗 **Seed URLs:** {len(http_urls)} | **Domain(s):** {', '.join(domains)}")

            # Crawl settings
            with st.expander("⚙️ Crawl Settings", expanded=False):
                sc1, sc2, sc3 = st.columns(3)
                with sc1:
                    max_depth = st.slider("Max Crawl Depth", 1, 15, 10, key="crawl_depth")
                with sc2:
                    max_pages = st.slider("Max Pages to Crawl", 50, 1000, 300, step=50, key="crawl_pages")
                with sc3:
                    crawl_delay = st.slider("Delay between requests (sec)", 0.1, 2.0, 0.3, step=0.1, key="crawl_delay")
                show_all = st.checkbox("Show all discovered URLs (including already covered)", value=False, key="show_all")
                ir_only = st.checkbox("Show only IR-relevant URLs", value=True, key="ir_only")

            crawl_button = st.button("🔎 Check for Missing URLs", type="secondary", use_container_width=True)

            if crawl_button:
                crawler = DomainCrawler(
                    max_depth=max_depth,
                    max_pages=max_pages,
                    delay=crawl_delay
                )

                progress_bar = st.progress(0)
                status_text = st.empty()
                detail_text = st.empty()

                def progress_callback(crawled, queued, discovered, depth, current_url):
                    pct = min(crawled / max_pages, 1.0)
                    progress_bar.progress(pct)
                    status_text.markdown(
                        f"**Crawled:** {crawled}/{max_pages} | "
                        f"**Queued:** {queued} | "
                        f"**Discovered:** {discovered} | "
                        f"**Depth:** {depth}"
                    )
                    display_url = current_url[:100] + "..." if len(current_url) > 100 else current_url
                    detail_text.markdown(f'<span class="crawl-progress">Current: {display_url}</span>', unsafe_allow_html=True)

                with st.spinner("🕷️ Crawling domain..."):
                    discovered_urls = crawler.crawl(http_urls, progress_callback=progress_callback)

                progress_bar.progress(1.0)
                status_text.markdown(f"**✅ Crawl complete!** Discovered **{len(discovered_urls)}** unique URLs.")
                detail_text.empty()

                # Classify each discovered URL
                regex_patterns = URLMatcher.extract_regex_patterns(after_urls)

                crawl_results = []
                for disc_url in sorted(discovered_urls):
                    is_covered, reason = URLMatcher.classify_discovered_url(
                        disc_url, after_urls, regex_patterns
                    )
                    is_ir = DomainCrawler.is_ir_relevant(disc_url)

                    crawl_results.append({
                        "url": disc_url,
                        "is_covered": is_covered,
                        "auto_reason": reason,
                        "is_ir_relevant": is_ir,
                    })

                st.session_state.crawl_results = crawl_results

            # Display crawl results
            if st.session_state.crawl_results:
                crawl_results = st.session_state.crawl_results

                # Separate covered vs uncovered
                covered = [r for r in crawl_results if r["is_covered"]]
                uncovered = [r for r in crawl_results if not r["is_covered"]]
                ir_uncovered = [r for r in uncovered if r["is_ir_relevant"]]

                st.markdown("---")
                st.subheader("📊 Crawl Results Summary")

                mc1, mc2, mc3, mc4 = st.columns(4)
                with mc1:
                    st.metric("Total Discovered", len(crawl_results))
                with mc2:
                    st.metric("Already Covered", len(covered))
                with mc3:
                    st.metric("Potentially Missing", len(uncovered))
                with mc4:
                    st.metric("IR-Relevant Missing", len(ir_uncovered))

                # ----- POTENTIALLY MISSING URLs -----
                st.markdown("### 🔴 Potentially Missing URLs")

                display_list = ir_uncovered if ir_only else uncovered

                if not display_list:
                    st.markdown("""
                        <div class="success-box">
                            <h4>✓ No missing URLs detected!</h4>
                            <p>All discovered URLs appear to be covered by the current configuration.</p>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"**Showing {len(display_list)} potentially missing URL(s):**")

                    # Checklist options
                    checklist_options = [
                        "Already added",
                        "Added in ticker issue sheet",
                        "Added with template",
                        "Sub page - scraped with ev/cp regex",
                        "Not relevant / Ignore",
                        "Needs investigation",
                    ]

                    for i, result in enumerate(display_list):
                        url = result["url"]
                        is_ir = result["is_ir_relevant"]
                        url_key = f"missing_{i}"

                        ir_badge = "🟢 IR" if is_ir else "⚪ Non-IR"

                        st.markdown(
                            f'<div class="missing-url-box">'
                            f'<strong>#{i+1}</strong> {ir_badge} &nbsp; '
                            f'<a href="{url}" target="_blank" rel="noopener noreferrer">{url}</a>'
                            f'</div>',
                            unsafe_allow_html=True
                        )

                        # Checklist dropdown
                        current_val = st.session_state.checklist_state.get(url_key, "-- Select validation --")
                        selected = st.selectbox(
                            f"Validation for URL #{i+1}",
                            options=["-- Select validation --"] + checklist_options,
                            index=(
                                0 if current_val == "-- Select validation --"
                                else (["-- Select validation --"] + checklist_options).index(current_val)
                            ),
                            key=f"select_{url_key}",
                            label_visibility="collapsed"
                        )
                        st.session_state.checklist_state[url_key] = selected

                    # Validation summary
                    st.markdown("---")
                    st.markdown("### 📋 Validation Summary")
                    validated = sum(
                        1 for k, v in st.session_state.checklist_state.items()
                        if k.startswith("missing_") and v != "-- Select validation --"
                    )
                    total_missing = len(display_list)
                    st.progress(validated / total_missing if total_missing > 0 else 0)
                    st.write(f"**{validated}/{total_missing}** URLs validated")

                    # Group by validation status
                    validation_summary = {}
                    for k, v in st.session_state.checklist_state.items():
                        if k.startswith("missing_") and v != "-- Select validation --":
                            validation_summary[v] = validation_summary.get(v, 0) + 1

                    if validation_summary:
                        st.table([
                            {"Validation Status": k, "Count": v}
                            for k, v in sorted(validation_summary.items())
                        ])

                    # Download missing URLs report
                    missing_report = []
                    for i, result in enumerate(display_list):
                        url_key = f"missing_{i}"
                        missing_report.append({
                            "url": result["url"],
                            "is_ir_relevant": result["is_ir_relevant"],
                            "validation": st.session_state.checklist_state.get(url_key, "Not validated"),
                        })

                    st.download_button(
                        label="📥 Download Missing URLs Report",
                        data=json.dumps(missing_report, indent=2),
                        file_name=f"missing_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        use_container_width=True
                    )

                # ----- ALREADY COVERED URLs -----
                if show_all and covered:
                    st.markdown("### 🟢 Already Covered URLs")
                    with st.expander(f"View {len(covered)} covered URLs", expanded=False):
                        for i, result in enumerate(covered):
                            url = result["url"]
                            reason = result["auto_reason"]
                            st.markdown(
                                f'<div class="found-url-box">'
                                f'<strong>#{i+1}</strong> &nbsp; '
                                f'<a href="{url}" target="_blank" rel="noopener noreferrer">{url}</a>'
                                f'<br><em>Reason: {reason}</em>'
                                f'</div>',
                                unsafe_allow_html=True
                            )

    # Footer
    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; color: #666; padding: 20px;'>
            <p>URL Audit Tool v2.0 | Built with Streamlit</p>
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
