# Web Scraping Project

## Setting Up a Virtual Environment

To ensure a clean environment and avoid conflicts with dependencies create a virtual environment:

1. **Navigate to project directory**:
   ```bash
   cd /path/to/your/project
   ```
   
2. **Create a virtual environment**:
   ```bash
   python3 -m venv venv
   ```
   
3. **Activate the virtual environment**:
   - On Ubuntu/Linux or macOS:
   ```bash
   source venv/bin/activate
   ```

4. **Install the required packages**:
   ```bash
   pip install -r requirements.txt
   ```

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
     1. Navigate to the Scrapy project directory:
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
