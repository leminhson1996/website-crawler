import streamlit as st
import pandas as pd
import sqlite3
import asyncio
import aiohttp
import random
from datetime import datetime
from bs4 import BeautifulSoup

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}


class Site(object):
    def __init__(self, domain, sitemaps_link, exclude_urls, log_queue, max_concurrent_tasks, max_concurrent_sitemaps):
        self.domain = domain
        self.sitemaps_link = sitemaps_link
        self.exclude_urls = exclude_urls
        self.log_queue = log_queue
        self.max_concurrent_tasks = max_concurrent_tasks
        self.max_concurrent_sitemaps = max_concurrent_sitemaps

    async def log(self, message):
        await self.log_queue.put(message)

    async def get_all_sitemap_links(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.sitemaps_link, headers=USER_AGENT) as response:
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
            async with session.get(sitemap, headers=USER_AGENT) as response:
                if response.status != 200:
                    await self.log(f"Error fetching sitemap: {response.status}")
                    return []

                soup = BeautifulSoup(await response.text(), "xml")
                urls = [loc.text for loc in soup.find_all("loc")]
                await self.log(f"Found {len(urls)} links in sitemap: {sitemap}")
                return urls

    async def scrape_article(self, session, url):
        await self.log(f"Fetching article from {url}...")
        async with session.get(url, headers=USER_AGENT) as response:
            soup = BeautifulSoup(await response.text(), "html.parser")

            article = soup.find("div", class_="article__body cms-body")
            title = soup.find("h1").get_text(
            ) if soup.find("h1") else "No Title"
            tags = [tag.get_text()
                    for tag in soup.select(".article__tag .box-content a")]
            await self.log(f'tags: {tags}')
            datetime_tag = soup.find("cms-date")
            article_datetime = datetime_tag["content"] if datetime_tag else datetime.now(
            ).isoformat()

            if article:
                paragraphs = article.find_all("p")
                full_text = "\n".join([p.get_text() for p in paragraphs])
                data = {
                    "title": title,
                    "content": full_text or article.get_text(),
                    "tags": ",".join(tags),
                    "datetime": article_datetime,
                    "url": url
                }
                self.update_to_db(url, data)
            return None

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

    def done(self):
        self.conn.close()


async def fetch_urls(sites, log_queue):
    for item in sites:
        site = Site(**item)
        site.init_db()
        await log_queue.put(f"Fetching root sitemap from {site.domain}...")
        links = await site.get_all_sitemap_links()
        site.save_all_urls(links)


async def crawl_sites(sites, log_queue):
    for item in sites:
        site = Site(**item)
        site.init_db()
        async with aiohttp.ClientSession() as session:
            tasks = []
            links = site.get_all_urls()
            for link in links:
                if site.url_content_exists(link):
                    await log_queue.put(f"URL content already exists: {link}")
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

        await log_queue.put(f"Crawling completed for {site.domain}.")
        site.done()


async def log_writer(log_queue, log_container):
    while True:
        message = await log_queue.get()
        if message is None:
            break
        log_container.write(message)


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
    domains = st.text_area('Enter a list of domains (comma separated)',
                           value="nhandan.vn",
                           help="Enter the domain names for crawling, e.g., 'nhandan.vn,example.com'")

    sitemaps = st.text_area('Enter corresponding sitemap links (comma separated)',
                            value="https://nhandan.vn/sitemaps.xml",
                            help="Enter the sitemap URLs corresponding to the domains.")

    exclude_urls = st.text_area('Enter exclude URLs (comma separated)',
                                value="https://nhandan.vn/sitemaps/categories.xml,https://nhandan.vn/sitemaps/topics.xml",
                                help="Enter the exclude URLs for each domain.")

    max_concurrent_tasks = st.number_input(
        'Enter max concurrent tasks when fetching content', min_value=1, value=1)
    max_concurrent_sitemaps = st.number_input(
        'Enter max concurrent tasks when fetching sitemaps urls', min_value=1, value=5)

    tab = st.sidebar.radio("Navigation", ["Crawl", "View Data"])

    log_placeholder = st.empty()

    if tab == "Crawl":
        log_queue = asyncio.Queue()
        with log_placeholder.container() as log_container:
            if st.button('Get all urls of sites'):
                sites_to_crawl = parse_info(domains, sitemaps, exclude_urls)
                for site in sites_to_crawl:
                    site['max_concurrent_tasks'] = max_concurrent_tasks
                    site['max_concurrent_sitemaps'] = max_concurrent_sitemaps
                run_asyncio_tasks([
                    fetch_urls(sites_to_crawl, log_queue),
                    log_writer(log_queue, log_container)
                ])
                log_queue.put_nowait(None)  # Signal the log writer to stop

            if st.button('Start Crawling content'):
                sites_to_crawl = parse_info(domains, sitemaps, exclude_urls)
                for site in sites_to_crawl:
                    site['max_concurrent_tasks'] = max_concurrent_tasks
                    site['max_concurrent_sitemaps'] = max_concurrent_sitemaps
                log_queue.put_nowait(
                    "Starting the crawl process... Please wait.")
                run_asyncio_tasks([
                    crawl_sites(sites_to_crawl, log_queue),
                    log_writer(log_queue, log_container)
                ])
                log_queue.put_nowait(None)  # Signal the log writer to stop

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
