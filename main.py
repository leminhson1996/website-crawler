import streamlit as st
import pandas as pd
import sqlite3
import asyncio
import aiohttp
import random
from datetime import datetime
from bs4 import BeautifulSoup
from typing import List


class Site:
    def __init__(self, domain, sitemaps_link, exclude_urls, max_concurrent_tasks, max_concurrent_sitemaps, user_agent):
        self.domain = domain
        self.sitemaps_link = sitemaps_link
        self.exclude_urls = exclude_urls
        self.max_concurrent_tasks = max_concurrent_tasks
        self.max_concurrent_sitemaps = max_concurrent_sitemaps
        self.user_agent = user_agent

    async def log(self, message):
        st.write(message)

    async def get_all_sitemap_links(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.sitemaps_link, headers={"User-Agent": self.user_agent}) as response:
                if response.status != 200:
                    await self.log(f"Error fetching sitemap: {response.status}")
                    return []

                soup = BeautifulSoup(await response.text(), "xml")
                sitemaps = [loc.text for loc in soup.find_all("loc")]

                all_urls = []
                semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
                tasks = [self.fetch_sitemap_links(
                    session, sitemap, semaphore) for sitemap in sitemaps]

                results = await asyncio.gather(*tasks)
                for urls in results:
                    all_urls.extend(urls)

                return all_urls

    async def fetch_sitemap_links(self, session, sitemap, semaphore):
        if sitemap in self.exclude_urls:
            return []
        async with semaphore:
            async with session.get(sitemap, headers={"User-Agent": self.user_agent}) as response:
                if response.status != 200:
                    await self.log(f"Error fetching sitemap: {response.status}")
                    return []

                soup = BeautifulSoup(await response.text(), "xml")
                urls = [loc.text for loc in soup.find_all("loc")]
                await self.log(f"Found {len(urls)} links in sitemap: {sitemap}")
                return urls
    
    async def get_post_content(self, soup, url):
        article_body = soup.find(
            'div', class_='c-news-detail').find('div', class_='b-maincontent')
        if article_body:
            paragraphs = article_body.find_all('p')
            full_text = "\n".join([p.get_text() for p in paragraphs])
        else:
            full_text = "No content found"

        header = soup.find(
            'h1', class_='sc-longform-header-title block-sc-title')
        title = header.get_text() if header else "No Title"
        tags_div = soup.find('div', class_='c-widget-tags onecms__tags')
        tags = [a.get_text()
                for a in tags_div.find_all('a')] if tags_div else []
        await self.log(f'tags: {tags}')
        publish_time = soup.find(
            'span', class_='sc-longform-header-date block-sc-publish-time')
        article_datetime = publish_time.get_text(
        ) if publish_time else datetime.now().isoformat()

        data = {
            "title": title,
            "content": full_text,
            "tags": ",".join(tags),
            "datetime": article_datetime,
            "url": url
        }
        return data

    async def scrape_article(self, session, url):
        await self.log(f"Fetching article from {url}...")
        try:
            async with session.get(url, headers={"User-Agent": self.user_agent}) as response:
                soup = BeautifulSoup(await response.text(), "html.parser")
                data = await self.get_post_content(soup, url)
                self.update_to_db(url, data)
        except Exception as e:
            await self.log(f"Error fetching article from {url}: {e}")
            await asyncio.sleep(5)
       

    def init_db(self):
        conn = sqlite3.connect("articles.db")
        self.conn = conn
        self.cursor = conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS articles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        datetime TEXT,
                        site TEXT,
                        url TEXT UNIQUE,
                        title TEXT,
                        content TEXT,
                        tags TEXT)''')
        self.conn.commit()

    def save_to_db(self, data):
        self.cursor.execute('''INSERT OR IGNORE INTO articles (datetime, site, url, title, content, tags)
                    VALUES (?, ?, ?, ?, ?, ?)''',
                            (data["datetime"], self.domain, data["url"], data["title"], data["content"], data["tags"]))

    def update_to_db(self, url, data):
        self.cursor.execute('''
            INSERT INTO articles (datetime, site, url, title, content, tags)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                datetime = excluded.datetime,
                site = excluded.site,
                title = excluded.title,
                content = excluded.content,
                tags = excluded.tags
        ''', (data["datetime"], self.domain, url, data["title"], data["content"], data["tags"]))

    def url_content_exists(self, url):
        self.cursor.execute(
            "SELECT content FROM articles WHERE url = ?", (url,))
        result = self.cursor.fetchone()
        return result is not None and bool(result[0])

    def get_all_urls(self):
        self.cursor.execute(
            "SELECT url FROM articles WHERE site = ?", (self.domain,))
        return [row[0] for row in self.cursor.fetchall()]

    def save_all_urls(self, urls: list):
        for url in urls:
            self.save_to_db(dict(datetime="", site=self.domain,
                            url=url, title="", content="", tags=""))
        self.conn.commit()

    def count_valid_posts(self):
        query = "SELECT COUNT(*) FROM articles WHERE site = ? AND title != ''"
        self.cursor.execute(query, (self.domain,))
        result = self.cursor.fetchall()
        return result[0][0] if result else 0
    
    def done(self):
        self.conn.close()


class NhandanSite(Site):
    def __init__(self, user_agent):
        super().__init__("nhandan.vn", "https://nhandan.vn/sitemap.xml", [
            "https://nhandan.vn/sitemap-article-daily.xml",
            "https://nhandan.vn/sitemap-news.xml",
            "https://nhandan.vn/sitemap-category.xml",
            "https://nhandan.vn/sitemap-event.xml"
        ], 5, 5, user_agent)


class DaidoanketSite(Site):
    def __init__(self, user_agent):
        super().__init__("daidoanket.vn", "https://daidoanket.vn/sitemap.xml", [
            "https://daidoanket.vn/sitemap-article-daily.xml",
            "https://daidoanket.vn/sitemap-news.xml",
            "https://daidoanket.vn/sitemap-category.xml",
            "https://daidoanket.vn/sitemap-event.xml"
        ], 10, 5, user_agent)


class VnEconomySite(Site):
    def __init__(self, user_agent):
        super().__init__("vneconomy.vn", "https://vneconomy.vn/sitemap.xml", [
            "https://vneconomy.vn/sitemap/categories.xml",
            "https://vneconomy.vn/sitemap/latest-news.xml",
            "https://vneconomy.vn/sitemap/google-news.xml",
        ], 10, 5, user_agent)


class TuoiTreSite(Site):
    def __init__(self, user_agent, category_url):
        super().__init__("tuoitre.vn", category_url, [], 10, 5, user_agent)
        self.category_url = category_url
        self.kinhdoanh_url = "https://tuoitre.vn/ajax-load-list-by-cate-11"

    async def crawl_category(self, base_url, max_pages=10000):
        print(f"crawl_category from {base_url}...")
        unique_articles = set()
        headers = {
            'accept': 'text/html, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9,vi;q=0.8',
            'cache-control': 'no-cache',
            'cookie': '__RC=5; __R=3; __tb=0; __uidac=0165fed13b8e86555460b0e0227d3be7; __admUTMtime=1736073309; _ck_isLogin=false; _ck_user=false; _ck_isTTSao=false; __stdf=MA==; orig_aid=4k26hg6kb2ag79d0.1736073310.des; fosp_uid=4k26hg6kb2ag79d0.1736073310.des; _cc_id=efe99a70316faae43a88f4af5a66fb30; _uidcms=1267123015247160976; __IP=2884841489; __stat=IkJMT0NLIg==; __M=1_6.0__8_14_131.0.0.0_Google-_0; __NM=1; __stp=eyJ2aXNpdCI6InJldHVybmluZyIsInV1aWQiOiIyYzQ4OTg3My1lNjk5LTQ2MjQtOGEyNC03YjFjMWYxZDdkN2IifQ==; _gid=GA1.2.381225933.1736262052; __stgeo=IjAi; __stbpnenable=MQ==; panoramaId_expiry=1736348452625; panoramaId=7bd01da82e4965d65f5a86aca673a9fb927a2bd1f8fbdc32115b65c162b67b3d; panoramaIdType=panoDevice; onetap_widget_cap=3; fosp_location_zone=1; dable_uid=63338376.1685181033473; jiyakeji_uuid=ead53b10-cd08-11ef-b09f-5174c2723e80; _ga_J8TZJ65FPH=GS1.1.1736262365.1.0.1736262365.60.0.0; cto_bundle=HyslrF9UaXpCNXI1bXVXRGFYaU9wSUNtSTVvcVI5UEpNWjhZRXUwb2ZBWHo3d3ljNnpMJTJGWTRjM3hrbmVoV09rS0VibXRNSzBxbGFqa2hMUWJZeUljQ3pNS0licndJTGxBZmhLQ1VGU3JSWEEzM0JvSGlhdUVJZTQwazBZOFlHMWNENkJqZmx3JTJGaE0xciUyRlJYZmdBeEh1JTJCZFE0USUzRCUzRA; _ga_RKT0H57HEE=GS1.1.1736262365.1.1.1736262776.0.0.0; __adm_upl=eyJ0aW1lIjoxNzM2MjY2MjQxLCJfdXBsIjoiMC0xMjY3MTIzMDE1MjQ3MTYwOTc2In0=; __uid=1267123015247160976; __create=1667123015; _ckdtdz=true; __first_load=1; __sts=eyJzaWQiOjE3MzYyNjIwNTIyMTksInR4IjoxNzM2MjY1NzU2OTE4LCJ1cmwiOiJodHRwcyUzQSUyRiUyRnR1b2l0cmUudm4lMkZraW5oLWRvYW5oLmh0bSIsInBldCI6MTczNjI2NTc1NjkxOCwic2V0IjoxNzM2MjYyMDUyMjE5LCJwVXJsIjoiaHR0cHMlM0ElMkYlMkZ0dW9pdHJlLnZuJTJGa2luaC1kb2FuaCUyRnRhaS1jaGluaCUyRnRyYW5nLTEzLmh0bSIsInBQZXQiOjE3MzYyNjU2OTk2MDIsInBUeCI6MTczNjI2NTY5OTYwMn0=; __gads=ID=d65e6ea339dd1c35:T=1736073310:RT=1736265761:S=ALNI_MaRWQP-e4CHVXfZiDqIEfKgfIuwGg; __gpi=UID=00000fd5508d7279:T=1736073310:RT=1736265761:S=ALNI_MYk_up18gZBm3_Wt104F30URbmZQw; __eoi=ID=2e5b0fe46215503b:T=1736073310:RT=1736265761:S=AA-AfjbWFYy-gkkX1R6AMJjVCKp8; __uif=__uid%3A1267123015247160976%7C__ui%3A1%252C5%7C__create%3A1667123015; _ttsid=421b14daea9ad613c6be58033740f699c3b6348b603ec5e0c50ef04b66ce03ab; _gat_UA-46730129-1=1; _gat_tto_vcc=1; _ga_G4SHSWB0Y5=GS1.1.1736262052.13.1.1736265933.60.0.0; _ga=GA1.1.1955595459.1736073309; _ga_8KQ37P0QJM=GS1.1.1736262052.13.1.1736265933.60.0.0; _ga_G67558G6BB=GS1.2.1736262052.14.1.1736265934.0.0.0; FCNEC=%5B%5B%22AKsRol93uHY1WWEd5MEbw5lMsjul7FXwacA4YFY5Fmnuhp0S_BmJ2SmUFyqG71x3L9z80b4VNzaaZVDFdaNhEqe1H_wuTsDK9xEZL4QEX2FUD0e_HFS5zVeC9yfkIAnWu277L1Q5D7aFRimq4EMg0BHiprmzeBwtCA%3D%3D%22%5D%5D; _ga_TH15HGRD3E=GS1.1.1736264796.12.1.1736265944.0.0.0',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://tuoitre.vn',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            for page in range(1, max_pages + 1):
                url = f"{base_url}/trang-{page}.htm"
                async with session.get(url) as response:
                    print(f"Fetch news from {url}")
                    soup = BeautifulSoup(await response.text(), "html.parser")
                    # Find all links to articles within elements with class 'box-category-item'
                    articles = []
                    for link in soup.select(".box-category-item a[href]"):
                        href = link['href']
                        full_url = "https://tuoitre.vn" + href
                        if full_url not in unique_articles:
                            unique_articles.add(full_url)
                            articles.append(full_url)
                    print(f"Found {len(articles)} articles on page {page}")
                    self.save_all_urls(articles)
    
    async def get_post_content(self, soup, url):
        article_body = soup.find(
               'div', class_='detail-content afcbc-body')
        if article_body:
            paragraphs = article_body.find_all('p')
            full_text = "\n".join([p.get_text() for p in paragraphs])
        else:
            full_text = "No content found"

        header = soup.find(
            'h1', class_='detail-title article-title')
        title = header.get_text() if header else "error"
        tags = ''

        # Extract publish date from the div with data-role="publishdate"
        publish_date_div = soup.find('div', {'data-role': 'publishdate'})
        article_datetime = publish_date_div.get_text(
            strip=True) if publish_date_div else datetime.now().isoformat()

        data = {
            "title": title,
            "content": full_text,
            "tags": ",".join(tags),
            "datetime": article_datetime,
            "url": url
        }
        return data
    

class ThanhNienSite(Site):
    def __init__(self, user_agent, category_url):
        super().__init__("thanhnien.vn", category_url, [], 10, 5, user_agent)
        self.category_url = category_url
        self.kinhdoanh_url = "https://thanhnien.vn/timelinelist/18549"
    
    async def fetch_page(self, session, url, unique_articles):
        async with session.get(url) as response:
            print(f"Fetch news from {url}")
            soup = BeautifulSoup(await response.text(), "html.parser")
            articles = []
            for link in soup.select(".box-category-item a[href]"):
                href = link['href']
                full_url = f"https://{self.domain}" + href
                if full_url not in unique_articles:
                    unique_articles.add(full_url)
                    articles.append(full_url)
            print(f"Found {len(articles)} articles on {url}")
            return articles

    async def crawl_category(self, base_url, max_pages=10000):
        print(f"crawl_category from {base_url}...")
        tasks = []
        unique_articles = set()
        headers = {
            'accept': 'text/html, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9,vi;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://thanhnien.vn',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = []
            for page in range(7228, max_pages + 1):
                url = f"{base_url}/{page}.htm"
                tasks.append(self.fetch_page(session, url, unique_articles))
                if len(tasks) >= self.max_concurrent_sitemaps:
                    results = await asyncio.gather(*tasks)
                    for articles in results:
                        # if len(articles) == 0:
                        #     stop = True
                        #     break
                        self.save_all_urls(articles)
                    tasks = []
            if tasks:
                results = await asyncio.gather(*tasks)
                for articles in results:
                    self.save_all_urls(articles)

    async def get_post_content(self, soup, url):
        article_body = soup.find(
            'div', class_='detail-content afcbc-body')
        if article_body:
            paragraphs = article_body.find_all('p')
            full_text = "\n".join([p.get_text() for p in paragraphs])
        else:
            full_text = "No content found"

        header = soup.find(
            'h1', class_='detail-title').find('span', {'data-role': 'title'})
        title = header.get_text() if header else "error"
        tags_div = soup.find('div', class_='detail-tab')
        tags = [a.get_text()
                for a in tags_div.find_all('a')] if tags_div else []

        # Extract publish date from the div with data-role="publishdate"
        publish_date_div = soup.find('div', {'data-role': 'publishdate'})
        article_datetime = publish_date_div.get_text(
            strip=True) if publish_date_div else datetime.now().isoformat()

        data = {
            "title": title,
            "content": full_text,
            "tags": ",".join(tags),
            "datetime": article_datetime,
            "url": url
        }
        return data

async def fetch_urls(sites: List[Site]):
    for site in sites:
        site.init_db()
        await site.log(f"Fetching root sitemap from {site.domain}...")
        if isinstance(site, TuoiTreSite) or isinstance(site, ThanhNienSite):
            await site.crawl_category(site.sitemaps_link)
        else:
            links = await site.get_all_sitemap_links()
            site.save_all_urls(links)


async def crawl_sites(sites: List[Site]):
    for site in sites:
        site.init_db()
        async with aiohttp.ClientSession() as session:
            tasks = []
            links = site.get_all_urls()
            for link in links:
                if site.url_content_exists(link):
                    # await site.log(f"URL content already exists: {link}")
                    continue

                tasks.append(site.scrape_article(session, link))
                if len(tasks) >= site.max_concurrent_tasks:
                    await asyncio.gather(*tasks)
                    site.conn.commit()
                    st.write(f"total posts: {site.count_valid_posts()}")
                    tasks = []
                    await asyncio.sleep(random.uniform(1, 3))

            if tasks:
                await asyncio.gather(*tasks)
                site.conn.commit()

        await site.log(f"Crawling completed for {site.domain}.")
        site.done()


def parse_info(domains, sitemaps, exclude_urls):
    domain_list = [domain.strip() for domain in domains.split(',')]
    sitemap_list = [sitemap.strip() for sitemap in sitemaps.split(',')]
    exclude_url_list = [exclude_url.strip()
                        for exclude_url in exclude_urls.split(',')]
    return [{"domain": domain, "sitemaps_link": sitemap, "exclude_urls": exclude_url_list} for domain, sitemap in zip(domain_list, sitemap_list)]


def fetch_data_from_db(page, page_size, site_filter, url_filter, title_filter):
    conn = sqlite3.connect("articles.db")
    cursor = conn.cursor()
    offset = page * page_size
    query = "SELECT * FROM articles WHERE 1=1"
    params = []
    if site_filter:
        query += " AND site LIKE ?"
        params.append(f"%{site_filter}%")
    if url_filter:
        query += " AND url LIKE ?"
        params.append(f"%{url_filter}%")
    if title_filter:
        query += " AND title LIKE ?"
        params.append(f"%{title_filter}%")
    query += " LIMIT ? OFFSET ?"
    params.extend([page_size, offset])
    cursor.execute(query, params)
    data = cursor.fetchall()
    conn.close()
    return data


def count_total_rows(site_filter, url_filter, title_filter):
    conn = sqlite3.connect("articles.db")
    cursor = conn.cursor()
    query = "SELECT COUNT(*) FROM articles WHERE 1=1"
    params = []
    if site_filter:
        query += " AND site LIKE ?"
        params.append(f"%{site_filter}%")
    if url_filter:
        query += " AND url LIKE ?"
        params.append(f"%{url_filter}%")
    if title_filter:
        query += " AND title LIKE ?"
        params.append(f"%{title_filter}%")
    cursor.execute(query, params)
    total_rows = cursor.fetchone()[0]
    conn.close()
    return total_rows


def run_asyncio_tasks(tasks):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


def main():
    st.title('Website Crawling Tool')

    st.markdown("### Git Repository")
    st.markdown(
        "[git@github.com:leminhson1996/website-crawler.git](git@github.com:leminhson1996/website-crawler.git)")

    sites = ["nhandan.vn", "daidoanket.vn", "vneconomy.vn", "tuoitre.vn", "thanhnien.vn"]
    selected_site = st.selectbox("Select a site to crawl", sites)

    user_agent = st.text_input(
        'Enter User-Agent', value="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

    category_url = ""
    if selected_site in ["tuoitre.vn", "thanhnien.vn"]:
        category_url = st.text_input('Enter category URL')

    tab = st.sidebar.radio("Navigation", ["Crawl", "View Data"])

    log_placeholder = st.empty()

    if tab == "Crawl":
        with log_placeholder.container() as log_container:
            if st.button('Get all urls of sites'):
                if selected_site == "nhandan.vn":
                    site = NhandanSite(user_agent)
                elif selected_site == "daidoanket.vn":
                    site = DaidoanketSite(user_agent)
                elif selected_site == "vneconomy.vn":
                    site = VnEconomySite(user_agent)
                elif selected_site == "tuoitre.vn":
                    if not category_url:
                        st.error("Please enter a category URL for tuoitre.vn")
                        return
                    site = TuoiTreSite(user_agent, category_url)
                elif selected_site == "thanhnien.vn":
                    if not category_url:
                        st.error("Please enter a category URL for thanhnien.vn")
                        return
                    site = ThanhNienSite(user_agent, category_url)
                else:
                    st.error("Invalid site selected")
                    return

                run_asyncio_tasks([
                    fetch_urls([site])
                ])
                st.write("Done")

            if st.button('Start Crawling content'):
                if selected_site == "nhandan.vn":
                    site = NhandanSite(user_agent)
                elif selected_site == "daidoanket.vn":
                    site = DaidoanketSite(user_agent)
                elif selected_site == "vneconomy.vn":
                    site = VnEconomySite(user_agent)
                elif selected_site == "tuoitre.vn":
                    if not category_url:
                        st.error("Please enter a category URL for tuoitre.vn")
                        return
                    site = TuoiTreSite(user_agent, category_url)
                elif selected_site == "thanhnien.vn":
                    if not category_url:
                        st.error("Please enter a category URL for tuoitre.vn")
                        return
                    site = ThanhNienSite(user_agent, category_url)
                else:
                    st.error("Invalid site selected")
                    return

                st.write("Starting the crawl process... Please wait.")
                run_asyncio_tasks([
                    crawl_sites([site])
                ])
                st.write("Crawling process completed!")

    elif tab == "View Data":
        st.header("View Data")
        site_filter = st.text_input("Filter by Site")
        url_filter = st.text_input("Filter by URL")
        title_filter = st.text_input("Filter by Title")

        page_size = 10
        total_rows = count_total_rows(site_filter, url_filter, title_filter)
        total_pages = (total_rows + page_size - 1) // page_size
        page = st.number_input("Page", min_value=0,
                               max_value=total_pages-1, step=1, value=0)

        data = fetch_data_from_db(
            page, page_size, site_filter, url_filter, title_filter)
        df = pd.DataFrame(
            data, columns=["ID", "Datetime", "Site", "URL", "Title", "Content", "Tags"])
        st.dataframe(df)


if __name__ == "__main__":
    main()
