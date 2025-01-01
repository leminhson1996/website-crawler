import streamlit as st
import pandas as pd
import sqlite3
import asyncio
import aiohttp
import random
from typing import List
from datetime import datetime
from bs4 import BeautifulSoup
import traceback



class Site(object):
    def __init__(self, domain, sitemaps_link, exclude_urls, max_concurrent_tasks, max_concurrent_sitemaps, user_agent):
        self.domain = domain
        self.sitemaps_link = sitemaps_link
        self.exclude_urls = exclude_urls
        self.max_concurrent_tasks = max_concurrent_tasks
        self.max_concurrent_sitemaps = max_concurrent_sitemaps
        self.user_agent = user_agent
        self.total_posts = 0

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
    
    async def get_article_content(self, response):
        soup = BeautifulSoup(await response.text(), "html.parser")
        article = soup.find("div", class_="article__body cms-body")
        content = None
        if article:
            paragraphs = article.find_all("p")
            content = "\n".join([p.get_text() for p in paragraphs])
        title = soup.find("h1").get_text(
        ) if soup.find("h1") else "No Title"
        tags = [tag.get_text()
                for tag in soup.select(".article__tag .box-content a")]
        datetime_tag = soup.find("meta", class_="cms-date")
        article_datetime = datetime_tag["content"] if datetime_tag else datetime.now(
        ).isoformat()

        return content, title, tags, article_datetime

    async def scrape_article(self, session, url):
        await self.log(f"Fetching article from {url}, at {datetime.now()}")
        try:
            async with session.get(url, headers={"User-Agent": self.user_agent}) as response:
                content, title, tags, article_datetime = await self.get_article_content(response)
                if content and title:
                    data = {
                        "title": title,
                        "content": content,
                        "tags": ",".join(tags),
                        "datetime": article_datetime,
                        "url": url
                    }
                    # await self.log(f"Title: {title}, date: {article_datetime}")
                    self.update_to_db(url, data)
                    self.total_posts += 1
                    with st.empty():
                        await self.log(f'Total posts: {self.total_posts}')
                else:
                    data = {
                        "title": "removed",
                        "content": "",
                        "tags": "",
                        "datetime": "",
                        "url": url
                    }
                    # await self.log("Invalid format -> removed")
                    self.update_to_db(url, data)
                return None
        except Exception as e:
            await self.log(f"Error from {url}: {traceback.print_exc()}")
            data = {
                "title": "error",
                "content": "",
                "tags": "",
                "datetime": "",
                "url": url
            }
            # await self.log("Invalid format -> removed")
            self.update_to_db(url, data)
            # await self.log(e.stack)
            # await self.log("Sleeping for 10 seconds...")
            # await asyncio.sleep(10)
            
       
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
        self.total_posts = count_total_rows(self.domain, None, None)

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
            "SELECT url, title FROM articles WHERE site = ?", (self.domain,))
        return [(row[0], row[1]) for row in self.cursor.fetchall()]

    def save_all_urls(self, urls: list):
        for url in urls:
            self.save_to_db(dict(datetime="", site=self.domain,
                            url=url, title="", content="", tags=""))
        self.conn.commit()

    def done(self):
        self.conn.close()

class NhandanSite(Site):
    def __init__(self):
        super().__init__("nhandan.vn", "https://nhandan.vn/sitemap.xml", [
            "https://nhandan.vn/sitemap-article-daily.xml",
            "https://nhandan.vn/sitemap-news.xml",
            "https://nhandan.vn/sitemap-category.xml",
            "https://nhandan.vn/sitemap-event.xml"
        ], 5, 5, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/")

class DaidoanketSite(Site):
    def __init__(self):
        super().__init__("daidoanket.vn", "https://daidoanket.vn/sitemap.xml", [
            "https://daidoanket.vn/sitemap-article-daily.xml",
            "https://daidoanket.vn/sitemap-news.xml",
            "https://daidoanket.vn/sitemap-category.xml",
            "https://daidoanket.vn/sitemap-event.xml"
        ], 10, 5, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/")

    async def get_article_content(self, response):
        soup = BeautifulSoup(await response.text(), "html.parser")
        content = None
        article_body = soup.find('div', class_='c-news-detail').find('div', class_='b-maincontent')
        if article_body:
            paragraphs = article_body.find_all('p')
            content = "\n".join([p.get_text() for p in paragraphs])
        title = soup.find("h1", class_="sc-longform-header-title block-sc-title").get_text()
        tags_div = soup.find('div', class_='c-widget-tags onecms__tags')
        tags = [a.get_text() for a in tags_div.find_all('a')]
        datetime_tag = soup.find(
            "span", class_="sc-longform-header-date block-sc-publish-time").get_text()
        article_datetime = datetime_tag if datetime_tag else datetime.now(
        ).isoformat()

        return content, title, tags, article_datetime
        

async def fetch_urls(sites: List[Site]):
    for site in sites:
        site.init_db()
        await site.log(f"Fetching root sitemap from {site.domain}...")
        links = await site.get_all_sitemap_links()
        site.save_all_urls(links)


async def crawl_sites(sites: List[Site]):
    for site in sites:
        # site = Site(**item)
        site.init_db()
        async with aiohttp.ClientSession() as session:
            tasks = []
            urls = site.get_all_urls()
            for item in urls:
                link, title = item
                if site.url_content_exists(link) or title == 'error':
                    # await site.log(f"URL content already exists: {link}")
                    continue

                tasks.append(site.scrape_article(session, link))
                if len(tasks) >= site.max_concurrent_tasks:
                    await asyncio.gather(*tasks)
                    site.conn.commit()
                    tasks = []
                    await asyncio.sleep(random.uniform(1, 3))

            if tasks:
                await asyncio.gather(*tasks)
                site.conn.commit()

        await site.log(f"Crawling completed for {site.domain}.")
        site.done()


def parse_info(domains: str)-> List[Site]:
    domain_list = [domain.strip() for domain in domains.split(',')]
    sites = []
    for domain in domain_list:
        if domain == 'nhandan.vn':
            site = NhandanSite()
        elif domain == 'daidoanket.vn':
            site = DaidoanketSite()
        sites.append(site)
    return sites


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
    query = "SELECT COUNT(*) FROM articles WHERE title IS NOT '' AND title IS NOT 'removed'"
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

    domains = st.text_area('Enter a list of domains (comma separated), must be in this list: nhandan.vn, daidoanket.vn',
                           value="daidoanket.vn",
                           help="Enter the domain names for crawling, e.g., 'nhandan.vn,daidoanket.vn'")

    # sitemaps = st.text_area('Enter corresponding sitemap links (comma separated)',
    #                         value="https://daidoanket.vn/sitemap.xml",
    #                         help="Enter the sitemap URLs corresponding to the domains.")

    # exclude_urls = st.text_area('Enter exclude URLs (comma separated)',
    #                             value="https://daidoanket.vn/sitemap-article-daily.xml,https://daidoanket.vn/sitemap-news.xml,https://daidoanket.vn/sitemap-category.xml,https://daidoanket.vn/sitemap-event.xml",
    #                             help="Enter the exclude URLs for each domain.")

    # user_agent = st.text_input(
    #     'Enter User-Agent', value="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

    # max_concurrent_tasks = st.number_input(
    #     'Enter max concurrent tasks', min_value=1, value=1)
    # max_concurrent_sitemaps = st.number_input(
    #     'Enter max concurrent sitemaps', min_value=1, value=5)

    tab = st.sidebar.radio("Navigation", ["Crawl", "View Data"])

    log_placeholder = st.empty()

    if tab == "Crawl":
        with log_placeholder.container() as log_container:
            if st.button('Get all urls of sites'):
                sites_to_crawl = parse_info(domains)
                run_asyncio_tasks([
                    fetch_urls(sites_to_crawl)
                ])
                st.write("Done")

            if st.button('Start Crawling content'):
                sites_to_crawl = parse_info(domains)
                st.write("Starting the crawl process... Please wait.")
                run_asyncio_tasks([
                    crawl_sites(sites_to_crawl)
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
