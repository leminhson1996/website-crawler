# Website Crawler

This project is a website crawling tool built with Streamlit. It allows you to fetch URLs from sitemaps, crawl content from those URLs, and view the crawled data with filtering options.

## Features

- Fetch URLs from sitemaps
- Crawl content from URLs
- View crawled data with filtering options
- Real-time logging of crawling process

## Git Repository

You can find the project repository at [git@github.com:leminhson1996/website-crawler.git](git@github.com:leminhson1996/website-crawler.git).

## Installation

1. Clone the repository:
    ```sh
    git clone git@github.com:leminhson1996/website-crawler.git
    cd website-crawler
    ```

2. Create a virtual environment and activate it:
    ```sh
    python3 -m venv env
    source env/bin/activate  # On Windows use `env\Scripts\activate`
    ```

3. Install the required packages:
    ```sh
    pip install -r requirements.txt
    ```

## Usage

1. Run the Streamlit app:
    ```sh
    streamlit run main.py
    ```

2. Open your web browser and go to `http://localhost:8501`.

3. Use the app as follows:

### Crawl Tab

1. **Enter a list of domains**: Enter the domain names for crawling, e.g., `nhandan.vn,example.com`.
2. **Enter corresponding sitemap links**: Enter the sitemap URLs corresponding to the domains.
3. **Enter exclude URLs**: Enter the exclude URLs for each domain.
4. **Get all URLs of sites**: Click this button to fetch all URLs from the sitemaps.
5. **Start Crawling content**: Click this button to start crawling content from the fetched URLs. The logs will be displayed in real-time.

### View Data Tab

1. **Filter by Site**: Enter a site name to filter the data.
2. **Filter by URL**: Enter a URL to filter the data.
3. **Filter by Title**: Enter a title to filter the data.
4. **Page**: Use the number input to navigate through the pages of data.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the MIT License.