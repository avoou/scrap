# Web Scraping Project

This project has three versions of an app that scrapes data (name, price) from a website about boots. The project uses Python and runs on Ubuntu OS.

## Versions of Web Scraping:
1. **Using Python3 with only BeautifulSoup** (`krossy.py`)
   - To run:
     ```bash
     python krossy.py
     ```

2. **Using Prefect with BeautifulSoup** (`krossy_prefect.py`)
   - To run:
     ```bash
     python krossy_prefect.py
     ```

3. **Using Scrapy Framework** (`/krossy_project`)
   - To run:
     1. Navigate to the project directory:
        ```bash
        cd krossy_project/
        ```
     2. Run the Scrapy project:
        ```bash
        scrapy crawl boots
        ```

## Requirements:
- Python3
- BeautifulSoup
- Prefect
- Scrapy
